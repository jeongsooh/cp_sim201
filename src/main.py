import asyncio
import logging
import sys
import os

# Automatically add the project root to python path to avoid ModuleNotFoundError when running under Sudo.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ocpp_client import OCPPClient
from src.controller import ChargingStationController
from src.station_config import StationConfig, StationConfigError

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("MAIN")

# Replace default HardwareAPI with STM32 custom logic seamlessly
import src.hal
from src.stm32_hal import STM32HardwareAPI
src.hal.HardwareAPI = STM32HardwareAPI()

import time

def blocking_read_rfid(ser) -> str:
    """Reads bytes from serial port robustly and extracts the card UID.

    The reader can emit two UID-bearing frame types depending on the
    card:

      Cmd 0x43 (Len=0009): 8-byte UID + tail. Used by cards with a full
          ISO14443 / Mifare DESFire 7+1 byte UID. Header "02000943",
          16 hex chars of UID after.
      Cmd 0x4D (Len=0005): 4-byte UID + tail. Used by 4-byte-UID cards
          (e.g. Mifare Classic). Header "0200054D", 8 hex chars of UID
          after.

    Some cards emit both frames in one buffered read — a 4-byte 0x4D
    "short" representation followed by the full 0x43 UID. In that
    case the user-registered keycode is the 8-byte value, so prefer
    0x43 when present and fall back to 0x4D otherwise.

    Old code blindly took raw_hex[8:24] which straddled adjacent
    frames when 0x4D preceded 0x43 — observed on TC_G_11_CS where a
    card with true UID 1040009970148953 was misreported as
    018A2A18F1020009.
    """
    try:
        if ser and ser.in_waiting > 0:
            time.sleep(0.1)  # Allow buffer to fill
            data = ser.read(ser.in_waiting)
            raw_hex = data.hex().upper()
            logger.info(f"Raw UART Bytes (HEX): {raw_hex}")

            # Prefer 8-byte UID frame (0x43) if present — it's the
            # canonical full UID for cards that have one.
            UID8_HEADER = "02000943"
            UID8_HEX_LEN = 16  # 8 bytes
            idx = raw_hex.find(UID8_HEADER)
            if idx >= 0 and idx + len(UID8_HEADER) + UID8_HEX_LEN <= len(raw_hex):
                card_id = raw_hex[idx + len(UID8_HEADER):
                                  idx + len(UID8_HEADER) + UID8_HEX_LEN]
                logger.info(f"Extracted Card ID for Auth (8B): {card_id}")
                return card_id

            # Fallback: 4-byte UID frame (0x4D), used by cards that
            # only present a 4-byte UID.
            UID4_HEADER = "0200054D"
            UID4_HEX_LEN = 8  # 4 bytes
            idx = raw_hex.find(UID4_HEADER)
            if idx >= 0 and idx + len(UID4_HEADER) + UID4_HEX_LEN <= len(raw_hex):
                card_id = raw_hex[idx + len(UID4_HEADER):
                                  idx + len(UID4_HEADER) + UID4_HEX_LEN]
                logger.info(f"Extracted Card ID for Auth (4B): {card_id}")
                return card_id

            # Neither frame found — stray data or noise. Log and skip
            # so the controller doesn't authorize a garbage idToken.
            logger.warning(
                f"No UID frame (0x43 or 0x4D) found in UART data — "
                f"ignoring scan. Raw={raw_hex}"
            )
    except Exception as e:
        logger.error(f"Serial read error: {e}")
    return ""

async def rfid_monitor(controller: ChargingStationController) -> None:
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

    # TC_C_39_CS: presence-based dedup. The reader keeps emitting the same UID
    # frame while a tag stays in the field, so a single "tap" that lasts >2s
    # used to bypass the time-based dedup in handle_rfid_scan and fire a
    # second AuthorizeRequest after stop_transaction had cleared transaction
    # state — OCTT saw it during the post-stop unplug window and rejected it.
    # Suppress consecutive same-UID emissions until an empty read confirms
    # the field has cleared (i.e. the user actually lifted the tag).
    last_uid_emitted = None
    while True:
        await asyncio.sleep(0.5) # Poll interval
        try:
            if ser:
                uid = await asyncio.to_thread(blocking_read_rfid, ser)
                if uid:
                    if uid == last_uid_emitted:
                        continue
                    last_uid_emitted = uid
                    logger.info(f"=====================================")
                    logger.info(f"   RFID TAG SCANNED: [{uid}]")
                    logger.info(f"=====================================")
                    # Handles AuthorizeRequest internally and triggers
                    # a transaction if accepted by CSMS and connector is plugged.
                    await controller.handle_rfid_scan(uid)
                else:
                    last_uid_emitted = None
        except Exception as e:
            logger.error(f"RFID monitor iteration failed: {e}", exc_info=True)

async def proximity_monitor(controller: ChargingStationController) -> None:
    """
    Background daemon to scan physical connection state (PI3/ADC) and trigger OCCP events with debounce.
    """
    logger.info("Starting Proximity monitor daemon (Polled with Debounce)")
    stable_status = "Available"
    pending_status = "Available"
    consecutive_counts = 0
    REQUIRED_COUNTS = 5  # Needs 5 consecutive matches (5 * 0.2s = 1.0s)
    
    while True:
        try:
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
        except Exception as e:
            logger.error(f"Proximity monitor iteration failed: {e}", exc_info=True)

        # Poll rapidly for debounce
        await asyncio.sleep(0.2)

async def cp_adc_monitor(controller: ChargingStationController) -> None:
    """
    Background daemon to scan ADC Channel 0 for State C transitions (+6V).
    State A: ~53000 | State B: ~45000 | State C: ~36500
    """
    logger.info("Starting CP ADC monitor daemon (Polling in_voltage0_raw)")
    state_c_threshold = 40000
    
    while True:
        try:
            adc_val = controller.power_contactor_hal.read_cp_voltage()
            # If ADC is valid and drops below 40k, EV is pulling power (State C)
            if 0 < adc_val < state_c_threshold:
                if controller.transaction_id and not getattr(controller, "_state_c_active", False):
                    await controller.handle_state_c()
        except Exception as e:
            logger.error(f"CP ADC monitor iteration failed: {e}", exc_info=True)

        await asyncio.sleep(0.5)

async def main() -> None:
    logger.info("========================================")
    logger.info("   STM32MP1 CP700P EV Charger daemon    ")
    logger.info("========================================")

    # 1. Configuration
    try:
        cfg = StationConfig()
    except (StationConfigError, FileNotFoundError) as e:
        logger.error(f"Failed to load station_config.json: {e}")
        return

    logger.info(f"Station config loaded: {cfg}")

    client = OCPPClient(cfg.station_id, cfg.csms_url, ws_kwargs=cfg.build_ws_kwargs())
    controller = ChargingStationController(
        client,
        cert_dir=cfg.cert_dir,
        security_profile=cfg.security_profile,
        basic_auth_user=cfg.basic_auth_user,
        ca_cert=cfg.ca_cert,
        serial_number=cfg.serial_number,
        firmware_version=cfg.firmware_version,
        signature_algorithm=cfg.signature_algorithm,
    )

    # 2. Connect to CSMS in the background (on_connect callback sends BootNotification)
    client_task = asyncio.create_task(client.connect())

    # Wait briefly for connection and BootNotification to complete
    logger.info("Connecting to CSMS...")
    await asyncio.sleep(5)

    # 3. Spin up hardware reading mechanisms
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
