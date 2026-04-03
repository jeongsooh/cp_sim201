import asyncio
import logging
import sys
import os

# Automatically add the project root to python path to avoid ModuleNotFoundError when running under Sudo.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ocpp_client import OCPPClient
from src.controller import ChargingStationController

# Replace default HardwareAPI with STM32 custom logic seamlessly
import src.hal
from src.stm32_hal import STM32HardwareAPI
src.hal.HardwareAPI = STM32HardwareAPI()

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("MAIN")

async def rfid_monitor(controller: ChargingStationController):
    """
    Background daemon to read RFID scans via /dev/ttySTM1 UART asynchronously.
    """
    logger.info("Starting RFID UART monitor on /dev/ttySTM1 (Placeholder loop)")
    # TODO: Implement real async pyserial read flow here.
    while True:
        await asyncio.sleep(5)
        # Uncomment and modify string below to simulate or trigger a real scan
        # await controller.handle_rfid_scan("A1B2C3D4")

async def proximity_monitor(controller: ChargingStationController):
    """
    Background daemon to scan physical connection state (PI3 pin) and trigger OCCP events.
    """
    logger.info("Starting Proximity GPIO monitor daemon (Polling PI3)")
    last_status = "Available"
    while True:
        # read_physical_connection calls sysfs directly
        current_status = controller.connector_hal.read_physical_connection()
        if current_status != last_status:
            logger.info(f"Physical Connection changed from {last_status} to {current_status}")
            last_status = current_status
            if current_status == "Occupied":
                await controller.simulate_cable_plugged()
            else:
                # Triggers StatusNotification (Available)
                await controller.connector_hal.on_status_change()
        
        # Debounce/Poll delay
        await asyncio.sleep(1)

async def main():
    logger.info("========================================")
    logger.info("   STM32MP1 CP700P EV Charger daemon    ")
    logger.info("========================================")

    # 1. Configuration
    station_id = "STM32_CS_01"
    server_ws_url = "ws://192.168.0.82:8000/ocpp/2.0.1" # 타겟 Real CSMS 주소
    
    client = OCPPClient(station_id, server_ws_url)
    controller = ChargingStationController(client)

    # 2. Connect to CSMS in the background
    client_task = asyncio.create_task(client.connect())

    # Wait briefly for connection (in production, should wait for state flag)
    logger.info("Connecting to CSMS...")
    await asyncio.sleep(3) 

    # 3. Trigger initial Boot Notification
    await controller.boot_routine()

    # 4. Spin up hardware reading mechanisms
    rfid_task = asyncio.create_task(rfid_monitor(controller))
    prox_task = asyncio.create_task(proximity_monitor(controller))

    logger.info("System is live and listening for hardware events.")

    # Block indefinitely
    await asyncio.gather(client_task, rfid_task, prox_task)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down charger daemon.")
