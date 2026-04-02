import os
import logging
from .hal import HardwareAPI

logger = logging.getLogger(__name__)

class SysfsGPIO:
    """
    A simple wrapper to control STM32 GPIOs via Linux Sysfs since libgpiod is not available.
    Converts STM32 pin names like 'PK1' to Linux GPIO numbers.
    Formula: (Port Index * 16) + Pin Number
    A=0, B=1, ... K=10. Thus PK1 = (10 * 16) + 1 = 161.
    """
    def __init__(self, port_str: str):
        self.port_str = port_str.upper()
        port = self.port_str[1]
        pin = int(self.port_str[2:])
        port_num = ord(port) - ord('A')
        self.gpio_num = (port_num * 16) + pin
        self.base_path = f"/sys/class/gpio/gpio{self.gpio_num}"

        self.export()

    def export(self):
        if not os.path.exists(self.base_path):
            try:
                with open("/sys/class/gpio/export", "w") as f:
                    f.write(str(self.gpio_num))
            except Exception as e:
                logger.error(f"Failed to export GPIO {self.port_str}({self.gpio_num}): {e}")

    def set_direction(self, direction: str):
        """direction should be 'in' or 'out'"""
        try:
            with open(os.path.join(self.base_path, "direction"), "w") as f:
                f.write(direction)
        except Exception as e:
            logger.error(f"Failed to set direction for {self.port_str}({self.gpio_num}): {e}")

    def write(self, value: int):
        """value should be 0 or 1"""
        try:
            with open(os.path.join(self.base_path, "value"), "w") as f:
                f.write(str(value))
        except Exception as e:
            logger.error(f"Failed to write GPIO {self.port_str}({self.gpio_num}): {e}")

    def read(self) -> int:
        try:
            with open(os.path.join(self.base_path, "value"), "r") as f:
                return int(f.read().strip())
        except Exception as e:
            logger.error(f"Failed to read GPIO {self.port_str}({self.gpio_num}): {e}")
            return 0


class STM32HardwareAPI(HardwareAPI):
    def __init__(self):
        logger.info("Initializing STM32HardwareAPI (Sysfs Mode)")
        
        # Power Contactor: Mapped to RELAY1 (PK1) per pin table
        self.relay1 = SysfsGPIO("PK1")
        self.relay1.set_direction("out")
        self.relay1.write(0) # Open initially (Power OFF)

        # Connector Proximity/State: Mapped to PI3 (AC_TEST1) as a placeholder for now
        self.proximity_pin = SysfsGPIO("PI3")
        self.proximity_pin.set_direction("in")

        # Serial RFID Interface placeholder mapping
        self.rfid_serial_port = "/dev/ttySTM1"

    def read_physical_connection(self) -> str:
        """Reads physical sensor to determine if cable is Available or Occupied"""
        val = self.proximity_pin.read()
        # Assume Active-High (1) means Occupied. Change appropriately based on circuit.
        return "Occupied" if val == 1 else "Available"

    def relay_on(self, evse_id: int):
        """Closes the physical relay switch to provide AC power"""
        logger.info(f"Closing Relay on EVSE {evse_id} (PK1: HIGH)")
        if evse_id == 1:
            self.relay1.write(1)

    def relay_off(self, evse_id: int):
        """Opens the physical relay switch to cut AC power"""
        logger.info(f"Opening Relay on EVSE {evse_id} (PK1: LOW)")
        if evse_id == 1:
            self.relay1.write(0)

    def is_relay_closed(self, evse_id: int) -> bool:
        if evse_id == 1:
            return self.relay1.read() == 1
        return False
