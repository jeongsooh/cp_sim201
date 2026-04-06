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
    Background daemon to scan physical connection state (PI3/ADC) and trigger OCCP events with debounce.
    """
    logger.info("Starting Proximity monitor daemon (Polled with Debounce)")
    stable_status = "Available"
    pending_status = "Available"
    consecutive_counts = 0
    REQUIRED_COUNTS = 5  # Needs 5 consecutive matches (5 * 0.2s = 1.0s)
    
    while True:
        # read_physical_connection calls sysfs directly
        current_status = controller.connector_hal.read_physical_connection()
        
        if current_status == pending_status:
            consecutive_counts += 1
        else:
            pending_status = current_status
            consecutive_counts = 1
            
        if consecutive_counts >= REQUIRED_COUNTS and pending_status != stable_status:
            logger.info(f"Physical Connection STABLE: changed from {stable_status} to {pending_status}")
            stable_status = pending_status
            if stable_status == "Occupied":
                await controller.simulate_cable_plugged()
            else:
                await controller.simulate_cable_unplugged()
        
        # Poll rapidly for debounce
        await asyncio.sleep(0.2)

async def cp_adc_monitor(controller: ChargingStationController):
    """
    Background daemon to scan ADC Channel 0 for State C transitions (+6V).
    State A: ~53000 | State B: ~45000 | State C: ~36500
    """
    logger.info("Starting CP ADC monitor daemon (Polling in_voltage0_raw)")
    state_c_threshold = 40000
    
    while True:
        adc_val = controller.power_contactor_hal.read_cp_voltage()
        # If ADC is valid and drops below 40k, EV is pulling power (State C)
        if 0 < adc_val < state_c_threshold:
            if controller.transaction_id and not getattr(controller, "_state_c_active", False):
                await controller.handle_state_c()
                
        await asyncio.sleep(0.5)

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
    adc_task  = asyncio.create_task(cp_adc_monitor(controller))

    logger.info("System is live and listening for hardware events.")

    # Block indefinitely
    await asyncio.gather(client_task, rfid_task, prox_task, adc_task)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down charger daemon.")
