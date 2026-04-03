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

import time

def blocking_read_rfid(ser) -> str:
    """Reads bytes from serial port robustly and cleans them."""
    try:
        if ser and ser.in_waiting > 0:
            time.sleep(0.1)  # Allow buffer to fill
            data = ser.read(ser.in_waiting)
            raw_hex = data.hex().upper()
            logger.info(f"Raw UART Bytes (HEX): {raw_hex}")
            
            # Extract the actual 8-byte Card Number from the fixed binary frame
            # Frame: STX(02) + Len(0009) + Cmd(43) + UUID(8 bytes) + Tail(3D) -> Example: 02000943 1040009970148953 3D
            card_id = raw_hex
            if raw_hex.startswith("02") and len(raw_hex) >= 24:
                card_id = raw_hex[8:24]
                logger.info(f"Extracted Card ID for Auth: {card_id}")
                
            return card_id
    except Exception as e:
        logger.error(f"Serial read error: {e}")
    return ""

async def rfid_monitor(controller: ChargingStationController):
    """
    Background daemon to read RFID scans via /dev/ttySTM6 UART asynchronously.
    """
    port = "/dev/ttySTM6"
    baudrate = 9600
    logger.info(f"Starting RFID UART monitor on {port} (Baud: {baudrate})")
    
    try:
        import serial
        ser = serial.Serial(port, baudrate, timeout=1)
    except ImportError:
        logger.error("pyserial is not installed! Run: pip install pyserial")
        return
    except Exception as e:
        logger.error(f"Failed to open RFID Serial Port {port}: {e}")
        logger.warning("RFID monitor cannot start hardware loop.")
        ser = None

    while True:
        await asyncio.sleep(0.5) # Poll interval
        if ser:
            uid = await asyncio.to_thread(blocking_read_rfid, ser)
            if uid:
                logger.info(f"=====================================")
                logger.info(f"   RFID TAG SCANNED: [{uid}]")
                logger.info(f"=====================================")
                # Handles AuthorizeRequest internally and triggers 
                # a transaction if accepted by CSMS and connector is plugged.
                await controller.handle_rfid_scan(uid)

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
