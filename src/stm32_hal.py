import logging
import sys
from .hal import HardwareAPI

logger = logging.getLogger(__name__)

try:
    import gpiod
except ImportError:
    logger.error("gpiod module is strictly required for STM32HardwareAPI. Please install python3-gpiod.")

class STM32HardwareAPI(HardwareAPI):
    def __init__(self):
        logger.info("Initializing STM32HardwareAPI (libgpiod python3-gpiod Mode)")
        
        # STM32 exposes each port as a separate gpiochip in typical OpenSTLinux configs.
        # Port A=0, B=1, ... J=9, K=10.
        # PK1 -> gpiochip10, line 1
        # PI3 -> gpiochip8, line 3
        
        self.line_relay = None
        self.line_prox = None

        try:
            # Relay 1 Setup (PK1)
            self.chip_relay = gpiod.Chip("gpiochip10")
            self.line_relay = self.chip_relay.get_line(1)
            self.line_relay.request(consumer="CP700P_Charger", type=gpiod.LINE_REQ_DIR_OUT)
            self.line_relay.set_value(0) # Open initially (Power OFF)
            
            # Proximity Setup (PI3)
            self.chip_prox = gpiod.Chip("gpiochip8")
            self.line_prox = self.chip_prox.get_line(3)
            self.line_prox.request(consumer="CP700P_Charger", type=gpiod.LINE_REQ_DIR_IN)
        except AttributeError as ae:
            logger.error(f"gpiod version mismatch (possibly v2 API). Error: {ae}")
        except Exception as e:
            logger.error(f"Failed to initialize gpiod lines: {e}. Check if gpiochip numbers are correct on this board.")
        
        # Serial RFID Interface placeholder
        self.rfid_serial_port = "/dev/ttySTM1"

    def read_physical_connection(self) -> str:
        """Reads physical sensor to determine if cable is Available or Occupied"""
        if not self.line_prox:
            return "Available"
        try:
            val = self.line_prox.get_value()
            return "Occupied" if val == 1 else "Available"
        except Exception as e:
            logger.error(f"Error reading PI3 proximity: {e}")
            return "Available"

    def relay_on(self, evse_id: int):
        """Closes the physical relay switch to provide AC power"""
        logger.info(f"Closing Relay on EVSE {evse_id} (PK1: HIGH)")
        if evse_id == 1 and self.line_relay:
            try:
                self.line_relay.set_value(1)
            except Exception as e:
                logger.error(f"Failed to write to relay: {e}")

    def relay_off(self, evse_id: int):
        """Opens the physical relay switch to cut AC power"""
        logger.info(f"Opening Relay on EVSE {evse_id} (PK1: LOW)")
        if evse_id == 1 and self.line_relay:
            try:
                self.line_relay.set_value(0)
            except Exception as e:
                logger.error(f"Failed to write to relay: {e}")

    def is_relay_closed(self, evse_id: int) -> bool:
        if evse_id == 1 and self.line_relay:
            try:
                return self.line_relay.get_value() == 1
            except Exception:
                return False
        return False
