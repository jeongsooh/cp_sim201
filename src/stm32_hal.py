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
        
        # Serial RFID Interface placeholder
        self.rfid_serial_port = "/dev/ttySTM6"
        
        self._init_cp_pwm()

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
        """Reads V, I, P, E from CS5490 via UART on /dev/ttySTM5 at 600 baud."""
        result = {"voltage": 0.0, "current": 0.0, "power": 0.0, "energy": 0.0}
        if evse_id != 1: return result
        
        try:
            import serial
            import os
            import time
            if not os.path.exists('/dev/ttySTM5'):
                return result
                
            with serial.Serial('/dev/ttySTM5', 600, timeout=0.1) as s:
                def cs_read_reg(page, reg):
                    s.write(bytes([0x80 | page])) # Select Page
                    # For CS5490, a Read Command is 0b001xxxxx, so we must bitwise OR the register address with 0x20!
                    s.write(bytes([0x20 | reg]))  
                    time.sleep(0.01)
                    resp = s.read(3)
                    if len(resp) == 3:
                        return int.from_bytes(resp, byteorder='big', signed=True)
                    return 0
                
                # Read from Page 16 (0x10)
                v_raw = cs_read_reg(0x10, 0x06)
                i_raw = cs_read_reg(0x10, 0x05)
                p_raw = cs_read_reg(0x10, 0x0E)
                
                # Apply nominal scale. User must tune these scaling factors!
                result["voltage"] = float(v_raw * 1.0)
                result["current"] = float(i_raw * 1.0)
                result["power"] = float(p_raw * 1.0)
                logger.debug(f"CS5490 RAW: V={v_raw}, I={i_raw}, P={p_raw}")

        except Exception as e:
            logger.error(f"CS5490 readout failed: {e}")
            
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
