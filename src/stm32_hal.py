import logging
import sys
from .hal import HardwareAPI

logger = logging.getLogger(__name__)

try:
    import gpiod
    from gpiod.line import Direction, Value
    HAS_V2 = hasattr(gpiod, 'request_lines')
except ImportError:
    HAS_V2 = False
    logger.error("gpiod module is strictly required for STM32HardwareAPI.")

class STM32HardwareAPI(HardwareAPI):
    def __init__(self):
        logger.info("Initializing STM32HardwareAPI (libgpiod v2 API Mode)")
        
        self.req_relay = None
        self.req_prox = None
        self._cs5490 = None

        if not HAS_V2:
            logger.error("gpiod v2 API not found. Please ensure proper python3-gpiod is installed.")
            return

        try:
            # Relay 1 Setup (PK1) -> gpiochip10, line 1
            self.req_relay = gpiod.request_lines(
                "/dev/gpiochip10",
                consumer="CP700P_Charger",
                config={
                    1: gpiod.LineSettings(direction=Direction.OUTPUT, output_value=Value.INACTIVE)
                }
            )
            
            # Proximity Setup (PI3) -> gpiochip8, line 3
            self.req_prox = gpiod.request_lines(
                "/dev/gpiochip8",
                consumer="CP700P_Charger",
                config={
                    3: gpiod.LineSettings(direction=Direction.INPUT)
                }
            )
        except Exception as e:
            logger.error(f"Failed to initialize gpiod v2 lines: {e}")
        
        self.rfid_serial_port = "/dev/ttySTM6"
        
        self._init_cp_pwm()
        self._init_cs5490()

    def _init_cp_pwm(self):
        """1kHz PWM 100% (State A/B Standby) 초기화"""
        self.pwm_chip = "pwmchip0"
        self.pwm_chan = "2" # TIM2_CH3 PB10
        self.pwm_dir = f"/sys/class/pwm/{self.pwm_chip}/pwm{self.pwm_chan}"
        self.period_ns = 1000000 # 1kHz
        
        import os
        if not os.path.exists(self.pwm_dir):
            try:
                with open(f"/sys/class/pwm/{self.pwm_chip}/export", "w") as f:
                    f.write(self.pwm_chan)
                import time; time.sleep(0.1)
            except Exception as e:
                logger.warning(f"Failed to export CP PWM: {e}")
                
        # Default 100% (DC)
        self.set_cp_pwm(1, 100)

    def _init_cs5490(self):
        """Open persistent serial port, sync, reset, configure gains, and start continuous conversions."""
        import os, serial, time
        if not os.path.exists('/dev/ttySTM5'):
            return
        logger.info("Initializing CS5490 Energy Meter via /dev/ttySTM5...")
        try:
            s = serial.Serial('/dev/ttySTM5', 600, timeout=1.0)
            self._cs5490 = s

            # Sync + Software Reset + Start Continuous Conversions
            s.write(b'\xFF\xFF\xFF\xFE'); time.sleep(0.1)
            s.write(b'\x5E');            time.sleep(0.7)   # Software Reset
            s.write(b'\xD5');            time.sleep(1.5)   # Start Continuous Conversions
            s.reset_input_buffer()

            # Write gain registers explicitly — software reset may leave them at 0
            def cs_write_reg(page, reg, value_24bit):
                s.write(bytes([0x80 | page]))              # Page select
                time.sleep(0.06)
                s.write(bytes([reg & 0x1F]))               # Write (bit5=0)
                time.sleep(0.06)
                s.write(value_24bit.to_bytes(3, 'big'))    # 3-byte payload
                time.sleep(0.06)

            def cs_read_reg(page, reg):
                s.reset_input_buffer()
                s.write(bytes([0x80 | page])); time.sleep(0.06)
                s.write(bytes([0x20 | reg]));  time.sleep(0.06)
                resp = s.read(3)
                return int.from_bytes(resp, 'big') if len(resp) == 3 else None

            cs_write_reg(0x10, 0x07, 0x400000)   # I_GAIN = unity
            cs_write_reg(0x10, 0x09, 0x400000)   # V_GAIN = unity

            igain = cs_read_reg(0x10, 0x07)
            vgain = cs_read_reg(0x10, 0x09)
            logger.info(f"CS5490 gain verify: I_GAIN=0x{igain or 0:06X}  V_GAIN=0x{vgain or 0:06X}")
            logger.info("CS5490 initialized, continuous conversions running.")
        except Exception as e:
            logger.error(f"Failed to initialize CS5490: {e}")
            self._cs5490 = None

    def set_cp_pwm(self, evse_id: int, duty_percent: int):
        """Control Pilot PWM 실시간 변경"""
        import os
        if evse_id != 1 or not os.path.exists(self.pwm_dir): 
            return
            
        duty_ns = int(self.period_ns * (max(0, min(100, duty_percent)) / 100.0))
        try:
            with open(f"{self.pwm_dir}/duty_cycle", "r") as f:
                curr_duty = int(f.read().strip())
            
            if curr_duty > self.period_ns:
                with open(f"{self.pwm_dir}/duty_cycle", "w") as f: f.write("0")
                
            with open(f"{self.pwm_dir}/period", "w") as f: f.write(str(self.period_ns))
            with open(f"{self.pwm_dir}/duty_cycle", "w") as f: f.write(str(duty_ns))
            with open(f"{self.pwm_dir}/enable", "w") as f: f.write("1")
        except Exception as e:
            logger.error(f"Failed to set CP PWM: {e}")

    def read_energy_meter_data(self, evse_id: int) -> dict:
        """Reads V, I, P from CS5490 via UART on /dev/ttySTM5 at 600 baud.

        CS5490 Page 16 register map (Table 18):
          0x0F = IRMS  (RMS current,  unsigned Q23)
          0x10 = VRMS  (RMS voltage,  unsigned Q23)
          0x05 = P_AVG (active power, signed Q23)

        Raw values are normalized fractions of full-scale.
        Scaling factors must be tuned to match hardware voltage divider / CT ratio.
        """
        result = {"voltage": 0.0, "current": 0.0, "power": 0.0, "energy": 0.0}
        if evse_id != 1 or self._cs5490 is None:
            return result

        try:
            import time
            s = self._cs5490

            # Re-sync UART framing before each read batch (no reset, no stop conversions)
            s.write(b'\xFF\xFF\xFF\xFE')
            time.sleep(0.08)
            s.reset_input_buffer()

            def cs_read_reg(page, reg):
                s.reset_input_buffer()
                s.write(bytes([0x80 | page]))  # Page select
                time.sleep(0.06)               # 1 byte @ 600 baud = ~17 ms
                s.write(bytes([0x20 | reg]))   # Read instruction (bit5=1)
                time.sleep(0.06)
                resp = s.read(3)               # 3-byte response @ 600 baud = ~50 ms
                if len(resp) == 3:
                    return int.from_bytes(resp, byteorder='big', signed=False)
                return None

            # Page 16 measurement registers
            v_raw  = cs_read_reg(0x10, 0x10)  # VRMS
            i_raw  = cs_read_reg(0x10, 0x0F)  # IRMS
            p_raw  = cs_read_reg(0x10, 0x05)  # P_AVG (signed)
            i_inst = cs_read_reg(0x10, 0x00)  # I instantaneous (current channel diagnostic)
            igain  = cs_read_reg(0x10, 0x07)  # I_GAIN register

            logger.info(f"CS5490 RAW: VRMS=0x{v_raw or 0:06X}({v_raw})"
                        f"  IRMS=0x{i_raw or 0:06X}({i_raw})"
                        f"  P_AVG=0x{p_raw or 0:06X}({p_raw})"
                        f"  I_inst=0x{i_inst or 0:06X}  I_GAIN=0x{igain or 0:06X}")

            V_FULLSCALE = 250.0
            I_FULLSCALE = 32.0

            if v_raw is not None:
                result["voltage"] = (v_raw / 0xFFFFFF) * V_FULLSCALE

            if i_raw is not None:
                result["current"] = (i_raw / 0xFFFFFF) * I_FULLSCALE

            if p_raw is not None:
                p_signed = p_raw if p_raw < 0x800000 else p_raw - 0x1000000
                result["power"] = (p_signed / 0x7FFFFF) * (V_FULLSCALE * I_FULLSCALE)

            logger.info(f"CS5490 SCALED: V={result['voltage']:.1f}V"
                        f"  I={result['current']:.2f}A"
                        f"  P={result['power']:.1f}W")

        except Exception as e:
            logger.error(f"CS5490 readout failed: {e}")
            self._cs5490 = None  # Mark as dead; reinit on next startup

        return result

    def read_cp_adc(self, evse_id: int) -> int:
        """Reads Peak CP Voltage from STM32 IIO ADC using Burst Sampling"""
        if evse_id != 1: return 0
        adc_path = "/sys/bus/iio/devices/iio:device2/in_voltage0_raw"
        import os
        if not os.path.exists(adc_path): return 0
        
        # Burst sample to catch the Peak of 1kHz PWM (e.g. 16.6% duty)
        max_val = 0
        try:
            with open(adc_path, "r") as f:
                for _ in range(40):
                    f.seek(0)
                    val_str = f.read().strip()
                    if val_str:
                        val = int(val_str)
                        if val > max_val:
                            max_val = val
            return max_val
        except Exception as e:
            return 0

    def check_proximity(self, connector_id: int) -> bool:
        """Reads PP/CP to determine if EV is physically connected"""
        # 1. ADC-based CP detection: Voltage Drop below 50k (~9V or ~6V) means State B or C
        adc_val = self.read_cp_adc(1)
        if 0 < adc_val < 50000:
            return True
            
        # 2. GPIO-based PP detection: Physical pin PI3 fallback
        if self.req_prox:
            try:
                if self.req_prox.get_value(3) == Value.ACTIVE:
                    return True
            except Exception as e:
                logger.error(f"Error reading PI3 proximity: {e}")
                
        return False

    def relay_on(self, evse_id: int):
        """Closes the physical relay switch to provide AC power"""
        logger.info(f"Closing Relay on EVSE {evse_id} (PK1: HIGH)")
        if evse_id == 1 and self.req_relay:
            try:
                self.req_relay.set_value(1, Value.ACTIVE)
            except Exception as e:
                logger.error(f"Failed to write to relay: {e}")

    def relay_off(self, evse_id: int):
        """Opens the physical relay switch to cut AC power"""
        logger.info(f"Opening Relay on EVSE {evse_id} (PK1: LOW)")
        if evse_id == 1 and self.req_relay:
            try:
                self.req_relay.set_value(1, Value.INACTIVE)
            except Exception as e:
                logger.error(f"Failed to write to relay: {e}")

    def is_relay_closed(self, evse_id: int) -> bool:
        if evse_id == 1 and self.req_relay:
            try:
                return self.req_relay.get_value(1) == Value.ACTIVE
            except Exception:
                return False
        return False
