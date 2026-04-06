import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

from .ocpp_client import OCPPClient
from .hal import ConnectorHAL, TokenReaderHAL, PowerContactorHAL

logger = logging.getLogger(__name__)

class ChargingStationController:
    def __init__(self, ocpp_client: OCPPClient):
        self.ocpp_client = ocpp_client
        self.evse_id = 1
        self.connector_id = 1
        
        self.connector_hal = ConnectorHAL(self.evse_id, self.connector_id, self.ocpp_client)
        self.token_reader_hal = TokenReaderHAL(self.ocpp_client)
        self.power_contactor_hal = PowerContactorHAL(self.evse_id, self.ocpp_client)
        
        self.is_authorized: bool = False
        self.transaction_id: str | None = None
        self.meter_value: float = 0.0
        self._state_c_active: bool = False
        
        self._heartbeat_task = None
        self._meter_task = None
        
        # Register server commands (TC_B_08_CS)
        self.ocpp_client.register_action_handler("Reset", self.handle_reset_request)
        
        # Register Security commands (TC_A_*)
        self.ocpp_client.register_action_handler("InstallCertificate", self.handle_install_certificate)
        self.ocpp_client.register_action_handler("GetInstalledCertificateIds", self.handle_get_installed_certificate_ids)
        self.ocpp_client.register_action_handler("DeleteCertificate", self.handle_delete_certificate)
        self.ocpp_client.register_action_handler("GetCertificateStatus", self.handle_get_certificate_status)
        self.ocpp_client.register_action_handler("CertificateSigned", self.handle_certificate_signed)

    async def boot_routine(self):
        logger.info("Executing Boot Routine")
        payload = {
            "reason": "PowerUp",
            "chargingStation": {
                "model": "AC_SIMULATOR_201",
                "vendorName": "TEST_CORP"
            }
        }
        res = await self.ocpp_client.call("BootNotification", payload)
        if res and res.get("status") == "Accepted":
            logger.info("BootNotification Accepted.")
            await self.connector_hal.on_status_change()
            
            # Start Heartbeat loop (TC_B_03_CS)
            interval = res.get("interval", 300)
            if not self._heartbeat_task:
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(interval))
        else:
            logger.warning("BootNotification Not Accepted.")

    async def _heartbeat_loop(self, interval: int):
        while True:
            await asyncio.sleep(interval)
            try:
                await self.ocpp_client.call("Heartbeat", {})
                logger.info("Heartbeat sent.")
            except Exception as e:
                logger.error(f"Failed to send heartbeat: {e}")

    async def handle_reset_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_08_CS: Handles incoming ResetRequest from CSMS"""
        reset_type = payload.get("type", "Immediate")
        logger.info(f"Received ResetRequest: {reset_type}")
        return {"status": "Accepted"}

    async def handle_rfid_scan(self, raw_uid: str):
        logger.info(f"RFID scanned: {raw_uid}")
        id_token = {"idToken": raw_uid, "type": "ISO14443"}
        try:
            res = await self.ocpp_client.call("Authorize", {"idToken": id_token})
            if res and res.get("idTokenInfo", {}).get("status") == "Accepted":
                if not self.transaction_id:
                    self.is_authorized = True
                    await self._try_start_transaction()
                else:
                    await self.stop_transaction("Local")
        except Exception as e:
            logger.error(f"Authorisation call failed: {e}")

    async def simulate_cable_plugged(self):
        logger.info("Cable plugged in. Connector Occupied.")
        self.connector_hal.status = "Occupied"
        payload = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "connectorStatus": "Occupied",
            "evseId": self.evse_id,
            "connectorId": self.connector_id
        }
        try:
            await self.ocpp_client.call("StatusNotification", payload)
            await self._try_start_transaction()
        except Exception as e:
            logger.error(f"Failed to process cable plug in: {e}")

    async def simulate_cable_unplugged(self):
        logger.info("Cable unplugged. Connector Available.")
        self.connector_hal.status = "Available"
        if self.transaction_id:
            logger.info("Transaction active during unplug. Stopping transaction (EVDisconnected).")
            await self.stop_transaction("EVDisconnected")
            
        payload = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "connectorStatus": "Available",
            "evseId": self.evse_id,
            "connectorId": self.connector_id
        }
        try:
            await self.ocpp_client.call("StatusNotification", payload)
        except Exception as e:
            logger.error(f"Failed to process cable unplug: {e}")

    async def _meter_values_loop(self, transaction_id: str):
        """TC_J_02_CS: Periodically reports TransactionEvent(Updated) with MeterValues"""
        while self.transaction_id == transaction_id:
            await asyncio.sleep(60)
            self.meter_value += 100.0
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            payload = {
                "eventType": "Updated",
                "timestamp": now_iso,
                "triggerReason": "MeterValuePeriodic",
                "seqNo": 1,
                "transactionInfo": {
                    "transactionId": self.transaction_id
                },
                "meterValue": [
                    {
                        "timestamp": now_iso,
                        "sampledValue": [{"value": self.meter_value}]
                    }
                ]
            }
            try:
                await self.ocpp_client.call("TransactionEvent", payload)
            except Exception as e:
                logger.error(f"Failed to send meter value: {e}")

    async def _try_start_transaction(self):
        if self.is_authorized and self.connector_hal.status == "Occupied":
            if not self.transaction_id:
                self.transaction_id = str(uuid.uuid4())
                self.meter_value = 0.0
                self._state_c_active = False
                now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                payload = {
                    "eventType": "Started",
                    "timestamp": now_iso,
                    "triggerReason": "Authorized",
                    "seqNo": 0,
                    "transactionInfo": {
                        "transactionId": self.transaction_id
                    },
                    "meterValue": [
                        {
                            "timestamp": now_iso,
                            "sampledValue": [{"value": self.meter_value}]
                        }
                    ]
                }
                await self.ocpp_client.call("TransactionEvent", payload)
                self.power_contactor_hal.control_relay("Close")
                # Drop to 53% PWM (32 Amps continuous limit) to allow vehicle onboard charger to pull power
                self.power_contactor_hal.set_pwm_duty(53)
                
                if self._meter_task:
                    self._meter_task.cancel()
                self._meter_task = asyncio.create_task(self._meter_values_loop(self.transaction_id))

    async def handle_state_c(self):
        """Called by main.py ADC monitor when CP voltage drops to +6V (< 40000 ADC)"""
        if self.transaction_id and not self._state_c_active:
            self._state_c_active = True
            logger.info("Control Pilot dropped to State C (+6V). EV is Charging!")
            payload = {
                "eventType": "Updated",
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "triggerReason": "ChargingStateChanged",
                "seqNo": 1,
                "transactionInfo": {
                    "transactionId": self.transaction_id,
                    "chargingState": "Charging"
                }
            }
            await self.ocpp_client.call("TransactionEvent", payload)

    async def stop_transaction(self, stopped_reason: str = "Local"):
        if self.transaction_id:
            self.power_contactor_hal.control_relay("Open")
            # Restore 100% PWM (+12V Standing State)
            self.power_contactor_hal.set_pwm_duty(100)
            
            if self._meter_task:
                self._meter_task.cancel()
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            payload = {
                "eventType": "Ended",
                "timestamp": now_iso,
                "triggerReason": "StopAuthorized" if stopped_reason == "Local" else "EVDeparted",
                "seqNo": 2,
                "transactionInfo": {
                    "transactionId": self.transaction_id,
                    "stoppedReason": stopped_reason
                },
                "meterValue": [
                    {
                        "timestamp": now_iso,
                        "sampledValue": [{"value": self.meter_value + 1500.0}]
                    }
                ]
            }
            await self.ocpp_client.call("TransactionEvent", payload)
            self.transaction_id = None
            self.is_authorized = False
            self._state_c_active = False

    # ==========================================
    # Phase 6: Security and Certificates (TC_A_*)
    # ==========================================
    async def trigger_security_event(self, type: str, info: str):
        """TC_A_01_CS: SecurityEventNotification"""
        payload = {
            "type": type,
            "timestamp": "2026-04-02T12:00:00Z",
            "techInfo": info
        }
        await self.ocpp_client.call("SecurityEventNotification", payload)

    async def trigger_sign_certificate(self, csr: str, cert_type: str = "ChargingStationCertificate"):
        """TC_A_04_CS, TC_A_05_CS: SignCertificate"""
        payload = {
            "csr": csr,
            "certificateType": cert_type
        }
        await self.ocpp_client.call("SignCertificate", payload)

    async def trigger_get_15118_ev_certificate(self, iso15118_schema_version: str):
        """TC_A_15_CS: Get15118EVCertificate"""
        payload = {
            "iso15118SchemaVersion": iso15118_schema_version,
            "action": "Install",
            "exiRequest": "dummy_exi_data"
        }
        await self.ocpp_client.call("Get15118EVCertificate", payload)

    async def handle_install_certificate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_06_CS, TC_A_07_CS"""
        logger.info("Handling InstallCertificate")
        return {"status": "Accepted"}

    async def handle_get_installed_certificate_ids(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_09_CS, TC_A_10_CS"""
        logger.info("Handling GetInstalledCertificateIds")
        return {
            "status": "Accepted",
            "certificateHashDataChain": []
        }

    async def handle_delete_certificate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_11_CS, TC_A_12_CS"""
        logger.info("Handling DeleteCertificate")
        return {"status": "Accepted"}

    async def handle_get_certificate_status(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_13_CS, TC_A_14_CS"""
        logger.info("Handling GetCertificateStatus")
        return {"status": "Accepted"}

    async def handle_certificate_signed(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_19_CS, TC_A_20_CS"""
        logger.info("Handling CertificateSigned")
        return {"status": "Accepted"}

