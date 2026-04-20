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

    def _cs5490_read_reg(self, page, addr):
        """Read one CS5490 register.
        CS5490 command format (datasheet Table 2):
          Read:        bits[7:6]=00 → addr & 0x3F
          Write:       bits[7:6]=01 → 0x40 | addr
          Page Select: bits[7:6]=10 → 0x80 | page
          Instruction: bits[7:6]=11 → 0xC0 | code
        """
        import time
        s = self._cs5490
        s.reset_input_buffer()
        s.write(bytes([0x80 | page]))    # Page select
        time.sleep(0.06)
        s.write(bytes([addr & 0x3F]))    # Register Read (bits[7:6]=00)
        time.sleep(0.06)
        resp = s.read(3)
        if len(resp) == 3:
            return int.from_bytes(resp, 'big', signed=False)
        return None

    def _init_cs5490(self):
        """Open persistent serial port, software reset, and start continuous conversions.

        CS5490 Page 16 register addresses (datasheet Table 6.3):
          0x02=I_inst, 0x03=V_inst, 0x05=PAVG, 0x06=IRMS, 0x07=VRMS
          0x24=IGAIN (default 0x400000), 0x26=VGAIN (default 0x400000)

        Instructions (Table 3):
          0xC1 = Software Reset (0xC0|0x01)
          0xD5 = Start Continuous Conversions (0xC0|0x15)

        NOTE: 0xFF bytes are NOT a sync — they are Gain Calibration instructions
        (0xC0|0x3F). Sending them corrupts I_GAIN and V_GAIN.
        Use the 128ms serial timeout instead: any incomplete transaction is cleared
        automatically after 128ms of inactivity.
        """
        import os, serial, time
        if not os.path.exists('/dev/ttySTM5'):
            return
        logger.info("Initializing CS5490 Energy Meter via /dev/ttySTM5...")
        try:
            s = serial.Serial('/dev/ttySTM5', 600, timeout=1.0)
            self._cs5490 = s

            time.sleep(0.2)          # Let 128ms serial timeout clear any stale state
            s.reset_input_buffer()

            s.write(b'\xC1')         # Software Reset instruction (0xC0|0x01)
            time.sleep(1.0)          # Wait for internal initialization sequence (extended for safety)
            s.reset_input_buffer()

            import time as _time

            def _write_reg(page, addr, b0, b1, b2):
                """Write a 24-bit value to a CS5490 register."""
                s.write(bytes([0x80 | page]))   # Page select
                _time.sleep(0.06)
                s.write(bytes([0x40 | addr]))   # Write command
                _time.sleep(0.06)
                s.write(bytes([b0, b1, b2]))    # 3 data bytes MSB first
                _time.sleep(0.06)

            # Config0 (Page 0, addr 0x00): set IPGA=10 (50x gain).
            # Default=0xC02000; IPGA at bits[7:6] of LSB → 10=50x → LSB=0x80.
            _write_reg(0x00, 0x00, 0xC0, 0x20, 0x80)

            # IGAIN (Page 16, addr 0x24): explicitly restore default 0x400000.
            # After reset this should be 0x400000, but write explicitly to ensure.
            _write_reg(0x10, 0x24, 0x40, 0x00, 0x00)

            # VGAIN (Page 16, addr 0x26): same as IGAIN.
            _write_reg(0x10, 0x26, 0x40, 0x00, 0x00)

            s.reset_input_buffer()

            s.write(b'\xD5')         # Start Continuous Conversions (0xC0|0x15)
            time.sleep(1.5)          # Default SampleCount=4000, OWR=4000Hz → 1s per cycle
            s.reset_input_buffer()

            igain = self._cs5490_read_reg(0x10, 0x24)
            vgain = self._cs5490_read_reg(0x10, 0x26)
            logger.info(f"CS5490 ready: I_GAIN=0x{igain or 0:06X} V_GAIN=0x{vgain or 0:06X} (expect 0x400000)")
            logger.info("CS5490 continuous conversions started (50x PGA, I_FULLSCALE=117.8A).")
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

        CS5490 Page 16 register addresses (datasheet Table 6.3):
          0x06 = IRMS  (RMS current,   unsigned Q23, full-scale fraction)
          0x07 = VRMS  (RMS voltage,   unsigned Q23, full-scale fraction)
          0x05 = PAVG  (active power,  signed   Q23, full-scale fraction)

        Voltage full-scale: 250mVpeak / (1K/1689K divider) → 220V gives 130mV RMS
          V_FULLSCALE = 220 × (176.78mV / 130mV) ≈ 299V
        Current full-scale: 50x PGA (Config0 IPGA=10), full-scale = 50mVpeak = 35.35mVrms.
          I_FULLSCALE = 35.35mV_rms / R_shunt_ohms  — set R_SHUNT below.
        """
        result = {"voltage": 0.0, "current": 0.0, "power": 0.0, "energy": 0.0}
        if evse_id != 1 or self._cs5490 is None:
            return result

        try:
            # No sync bytes needed. CS5490 128ms serial timeout clears stale state.
            # Reads are spaced 60s apart so no timeout collision is possible.
            v_raw = self._cs5490_read_reg(0x10, 0x07)  # VRMS
            i_raw = self._cs5490_read_reg(0x10, 0x06)  # IRMS
            p_raw = self._cs5490_read_reg(0x10, 0x05)  # PAVG (signed Q23)
            igain = self._cs5490_read_reg(0x10, 0x24)  # IGAIN (diagnostic, expect 0x400000)

            logger.info(f"CS5490 RAW: VRMS=0x{v_raw or 0:06X}"
                        f"  IRMS=0x{i_raw or 0:06X}"
                        f"  PAVG=0x{p_raw or 0:06X}"
                        f"  IGAIN=0x{igain or 0:06X}")

            # Voltage: schematic 4×422K+1K divider → 130mV RMS @ 220V.
            # CS5490 voltage channel full-scale = 250mVpeak = 176.78mVrms.
            # V_FULLSCALE = 220 × (176.78 / 130) ≈ 299V
            V_FULLSCALE = 299.0

            # Current: 50x PGA (IPGA=10 in Config0), full-scale = 50mVpeak = 35.35mVrms.
            # R_shunt = 300μΩ → I_FULLSCALE = 35.35mV / 0.0003Ω = 117.8A
            R_SHUNT = 0.0003  # 300 micro-ohm shunt resistor
            I_FULLSCALE = 0.03535 / R_SHUNT  # 50x PGA: 35.35mVrms / 0.0003Ω = 117.8A

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
            self._cs5490 = None

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
