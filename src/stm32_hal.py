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
        self.rfid_serial_port = "/dev/ttySTM1"

    def read_physical_connection(self) -> str:
        """Reads physical sensor to determine if cable is Available or Occupied"""
        if not self.req_prox:
            return "Available"
        try:
            val = self.req_prox.get_value(3)
            return "Occupied" if val == Value.ACTIVE else "Available"
        except Exception as e:
            logger.error(f"Error reading PI3 proximity: {e}")
            return "Available"

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
