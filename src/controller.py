import asyncio
import hashlib
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from .ocpp_client import OCPPClient
from .hal import ConnectorHAL, TokenReaderHAL, PowerContactorHAL
from .persistence import load_device_model, save_device_model

logger = logging.getLogger(__name__)

class ChargingStationController:
    def __init__(self, ocpp_client: OCPPClient, cert_dir: str = "/etc/cp_sim201/certs", security_profile: int = 0):
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
        self._tx_seq_no: int = 0

        self._heartbeat_task = None
        self._meter_task = None
        self._pending_reset: bool = False
        self._first_connect: bool = True

        # Block G: EVSE 가용 상태
        self.is_evse_available: bool = True

        # Block D: 로컬 인가 리스트
        self.local_list_version: int = 0
        self.local_auth_list: list = []

        # Block I: 충전 프로파일
        self.charging_profiles: dict = {}

        # Block H: 예약
        self.reservations: dict = {}  # key: reservationId → {expiryDateTime, idToken, evseId}

        # Block N: 디스플레이 메세지
        self.display_messages: dict = {}  # key: message_id → message dict

        # Block O: 변수 모니터링
        self.variable_monitoring: dict = {}  # key: monitor_id → monitoring config
        self.monitoring_base: str = "FactoryDefault"
        self.monitoring_level: int = 0

        # Block K: 배포된 펌웨어
        self.published_firmware: dict = {}  # key: checksum → location

        # Block A: 인증서 관리
        self._cert_dir: str = cert_dir
        # key: serialNumber hex string
        # value: {"certificateType": str, "certificateHashData": dict, "pem_path": str}
        self.installed_certificates: Dict[str, Dict] = {}
        # CertificateSigned로 수신한 클라이언트 인증서 경로 — 다음 재시작 시 적용
        self._pending_client_cert: Optional[str] = None

        # Block B: 장치 모델 (component → variable → (value, mutability))
        self.device_model = load_device_model({
            "ChargingStation": {
                "Model":           ("AC_SIMULATOR_201", "ReadOnly"),
                "VendorName":      ("TEST_CORP",        "ReadOnly"),
                "FirmwareVersion": ("1.0.0",            "ReadWrite"),
                "SerialNumber":    ("SN-001",           "ReadOnly"),
            },
            "EVSE": {
                "AvailabilityState": ("Available", "ReadWrite"),
                "Power":             ("7400",      "ReadOnly"),
            },
            "Connector": {
                "AvailabilityState": ("Available", "ReadWrite"),
                "ConnectorType":     ("cType2",    "ReadOnly"),
            },
            "TokenReader": {
                "Enabled": ("true", "ReadWrite"),
            },
            # CSMS 설정 가능 파라미터
            "SampledDataCtrlr": {
                "SampledDataTxUpdatedInterval":   ("60",    "ReadWrite"),
                "SampledDataTxUpdatedMeasurands": ("Current.Import,Voltage,Energy.Active.Import.Register", "ReadWrite"),
                "SampledDataTxStartedMeasurands": ("Energy.Active.Import.Register", "ReadWrite"),
                "SampledDataTxEndedMeasurands":   ("Energy.Active.Import.Register", "ReadWrite"),
                "SupportedMeasurands":            ("Energy.Active.Import.Register,Current.Import,Voltage,Power.Active.Import", "ReadOnly"),
            },
            "AlignedDataCtrlr": {
                "AlignedDataInterval":          ("0",                                  "ReadWrite"),
                "AlignedDataMeasurands":        ("Energy.Active.Import.Register",      "ReadWrite"),
                "AlignedDataTxEndedMeasurands": ("Energy.Active.Import.Register",      "ReadWrite"),
            },
            "HeartbeatCtrlr": {
                "HeartbeatInterval": ("60", "ReadWrite"),
            },
            "TxCtrlr": {
                "TxStartPoint":             ("Authorized,EVConnected", "ReadWrite"),
                "TxStopPoint":              ("Authorized,EVConnected", "ReadWrite"),
                "StopTxOnEVSideDisconnect": ("true", "ReadWrite"),
                "EVConnectionTimeOut":      ("60",   "ReadWrite"),
            },
            "AuthCtrlr": {
                "AuthorizeRemoteStart":         ("true",  "ReadWrite"),
                "LocalAuthorizeOffline":        ("true",  "ReadWrite"),
                "LocalPreAuthorize":            ("false", "ReadWrite"),
                "OfflineTxForUnknownIdEnabled": ("false", "ReadWrite"),
            },
            "OCPPCommCtrlr": {
                "MessageAttempts":                  ("3",    "ReadWrite"),
                "MessageAttemptInterval":           ("30",   "ReadWrite"),
                "OfflineThreshold":                 ("60",   "ReadWrite"),
                "NetworkProfileConnectionAttempts": ("3",    "ReadWrite"),
                "ActiveNetworkProfile":             ("0",    "ReadOnly"),
                "NetworkConfigurationPriority":     ("0",    "ReadWrite"),
                "QueueAllMessages":                 ("false","ReadWrite"),
                "RetryBackOffWaitMinimum":          ("10",   "ReadWrite"),
                "RetryBackOffRepeatTimes":          ("5",    "ReadWrite"),
                "RetryBackOffRandomRange":          ("10",   "ReadWrite"),
            },
            "LocalAuthListCtrlr": {
                "Enabled": ("true", "ReadWrite"),
                "Entries": ("100",  "ReadOnly"),
            },
            "SmartChargingCtrlr": {
                "Enabled": ("false", "ReadWrite"),
            },
            "ReservationCtrlr": {
                "Enabled": ("true", "ReadWrite"),
            },
            "SecurityCtrlr": {
                "SecurityProfile":        (str(security_profile), "ReadWrite"),
                "AllowCSMSTLSWildcards":  ("false", "ReadWrite"),
                "OrganizationName":       ("TEST_CORP", "ReadWrite"),
                "CertificateEntries":     ("2",   "ReadOnly"),
            },
        })

        # Block B — Core / Provisioning
        self.ocpp_client.register_action_handler("Reset",         self.handle_reset_request)
        self.ocpp_client.register_action_handler("GetVariables",  self.handle_get_variables)
        self.ocpp_client.register_action_handler("SetVariables",  self.handle_set_variables)
        self.ocpp_client.register_action_handler("GetBaseReport", self.handle_get_base_report)
        self.ocpp_client.register_action_handler("GetReport",     self.handle_get_report)

        # Block C — Authorization
        self.ocpp_client.register_action_handler("ClearCache", self.handle_clear_cache)

        # Block D — Local Authorization List
        self.ocpp_client.register_action_handler("SendLocalList",       self.handle_send_local_list)
        self.ocpp_client.register_action_handler("GetLocalListVersion", self.handle_get_local_list_version)

        # Block E — Transaction
        self.ocpp_client.register_action_handler("GetTransactionStatus", self.handle_get_transaction_status)

        # Block F — Remote Control
        self.ocpp_client.register_action_handler("RequestStartTransaction", self.handle_request_start_transaction)
        self.ocpp_client.register_action_handler("RequestStopTransaction",  self.handle_request_stop_transaction)
        self.ocpp_client.register_action_handler("UnlockConnector",         self.handle_unlock_connector)

        # Block G — Availability
        self.ocpp_client.register_action_handler("ChangeAvailability", self.handle_change_availability)

        # Block I — Smart Charging
        self.ocpp_client.register_action_handler("SetChargingProfile",  self.handle_set_charging_profile)
        self.ocpp_client.register_action_handler("GetChargingProfiles", self.handle_get_charging_profiles)

        # Block K — Firmware Management
        self.ocpp_client.register_action_handler("UpdateFirmware", self.handle_update_firmware)
        self.ocpp_client.register_action_handler("GetLog",         self.handle_get_log)

        # Block L — Remote Trigger
        self.ocpp_client.register_action_handler("TriggerMessage", self.handle_trigger_message)

        # Block B (추가)
        self.ocpp_client.register_action_handler("SetNetworkProfile", self.handle_set_network_profile)

        # Block E (추가)
        self.ocpp_client.register_action_handler("CostUpdated", self.handle_cost_updated)

        # Block H — Reservation
        self.ocpp_client.register_action_handler("ReserveNow",         self.handle_reserve_now)
        self.ocpp_client.register_action_handler("CancelReservation",  self.handle_cancel_reservation)

        # Block I (추가)
        self.ocpp_client.register_action_handler("ClearChargingProfile",  self.handle_clear_charging_profile)
        self.ocpp_client.register_action_handler("GetCompositeSchedule",  self.handle_get_composite_schedule)

        # Block K (추가)
        self.ocpp_client.register_action_handler("PublishFirmware",   self.handle_publish_firmware)
        self.ocpp_client.register_action_handler("UnpublishFirmware", self.handle_unpublish_firmware)

        # Block M — Data Transfer
        self.ocpp_client.register_action_handler("DataTransfer",        self.handle_data_transfer)
        self.ocpp_client.register_action_handler("CustomerInformation", self.handle_customer_information)

        # Block N — Display Messages
        self.ocpp_client.register_action_handler("SetDisplayMessage",  self.handle_set_display_message)
        self.ocpp_client.register_action_handler("GetDisplayMessages", self.handle_get_display_messages)
        self.ocpp_client.register_action_handler("ClearDisplayMessage",self.handle_clear_display_message)

        # Block O — Monitoring
        self.ocpp_client.register_action_handler("SetVariableMonitoring",  self.handle_set_variable_monitoring)
        self.ocpp_client.register_action_handler("GetMonitoringReport",    self.handle_get_monitoring_report)
        self.ocpp_client.register_action_handler("ClearVariableMonitoring",self.handle_clear_variable_monitoring)
        self.ocpp_client.register_action_handler("SetMonitoringBase",      self.handle_set_monitoring_base)
        self.ocpp_client.register_action_handler("SetMonitoringLevel",     self.handle_set_monitoring_level)

        # Block A — Security and Certificates
        self.ocpp_client.register_action_handler("InstallCertificate",          self.handle_install_certificate)
        self.ocpp_client.register_action_handler("GetInstalledCertificateIds",  self.handle_get_installed_certificate_ids)
        self.ocpp_client.register_action_handler("DeleteCertificate",           self.handle_delete_certificate)
        self.ocpp_client.register_action_handler("GetCertificateStatus",        self.handle_get_certificate_status)
        self.ocpp_client.register_action_handler("CertificateSigned",           self.handle_certificate_signed)

        self.ocpp_client.register_on_connect(self._on_reconnect)

    # ------------------------------------------------------------------
    # Device Model 헬퍼
    # ------------------------------------------------------------------

    def _get_param(self, component: str, variable: str, default: str = "") -> str:
        entry = self.device_model.get(component, {}).get(variable)
        return entry[0] if entry else default

    def _get_int(self, component: str, variable: str, default: int) -> int:
        try:
            return int(self._get_param(component, variable, str(default)))
        except (ValueError, TypeError):
            return default

    def _get_bool(self, component: str, variable: str, default: bool) -> bool:
        return self._get_param(component, variable, str(default).lower()) == "true"

    _MEASURAND_META = {
        "Voltage":                       ("voltage", "V"),
        "Current.Import":                ("current", "A"),
        "Power.Active.Import":           ("power",   "W"),
        "Energy.Active.Import.Register": (None,      "Wh"),  # uses accumulated energy_wh
    }

    def _build_sampled_values(self, measurands_str: str, meter_data: dict,
                              context: str, energy_wh: float) -> list:
        """Build OCPP 2.0.1 sampledValue list from a comma-separated measurands string."""
        result = []
        for m in measurands_str.split(","):
            m = m.strip()
            key, unit = self._MEASURAND_META.get(m, (None, None))
            if unit is None:
                continue
            value = energy_wh if key is None else meter_data.get(key, 0.0)
            result.append({
                "value": round(value, 3),
                "context": context,
                "measurand": m,
                "unitOfMeasure": {"unit": unit},
            })
        return result

    def _apply_variable_change(self, component: str, variable: str, value: str) -> None:
        """SetVariables 수신 후 즉시 동작에 반영이 필요한 파라미터를 처리한다."""
        if component == "HeartbeatCtrlr" and variable == "HeartbeatInterval":
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                logger.info(f"HeartbeatInterval changed to {value}s — task restarted")

    # ------------------------------------------------------------------
    # 부트 / 하트비트
    # ------------------------------------------------------------------

    async def boot_routine(self, reason: str = "PowerUp") -> None:
        logger.info(f"Executing Boot Routine (reason={reason})")
        payload = {
            "reason": reason,
            "chargingStation": {
                "model": "AC_SIMULATOR_201",
                "vendorName": "TEST_CORP"
            }
        }
        res = await self.ocpp_client.call("BootNotification", payload)
        if res and res.get("status") == "Accepted":
            logger.info("BootNotification Accepted.")
            await self.connector_hal.on_status_change(force=True)

            interval = res.get("interval", 300)
            self.device_model["HeartbeatCtrlr"]["HeartbeatInterval"] = (str(interval), "ReadWrite")
            save_device_model(self.device_model)
            if not self._heartbeat_task or self._heartbeat_task.done():
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        else:
            logger.warning("BootNotification Not Accepted.")

    async def _on_reconnect(self) -> None:
        """연결 성립 시 호출. 최초 부팅 또는 Reset 후에만 BootNotification 전송."""
        if self._pending_reset:
            self._pending_reset = False
            self._first_connect = False
            await self.boot_routine(reason="RemoteReset")
        elif self._first_connect:
            self._first_connect = False
            await self.boot_routine(reason="PowerUp")
        else:
            # 단순 연결 재연결(connection drop) — BootNotification 불필요, StatusNotification 전송
            logger.info("Reconnected after connection drop, sending StatusNotification.")
            await self.connector_hal.on_status_change(force=True)

    async def _heartbeat_loop(self) -> None:
        while True:
            interval = self._get_int("HeartbeatCtrlr", "HeartbeatInterval", 60)
            await asyncio.sleep(interval)
            try:
                await self.ocpp_client.call("Heartbeat", {})
                logger.info("Heartbeat sent.")
            except Exception as e:
                logger.error(f"Failed to send heartbeat: {e}")

    # ------------------------------------------------------------------
    # Block B — Core / Provisioning
    # ------------------------------------------------------------------

    async def handle_reset_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_08_CS: Handles incoming ResetRequest from CSMS"""
        reset_type = payload.get("type", "Immediate")
        logger.info(f"Received ResetRequest: {reset_type}")
        self._pending_reset = True
        return {"status": "Accepted"}

    async def handle_get_variables(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_06_CS: Returns device model variable values"""
        results = []
        for item in payload["getVariableData"]:
            comp = item["component"]["name"]
            var  = item["variable"]["name"]
            attr = item.get("attributeType", "Actual")
            comp_data = self.device_model.get(comp, {})
            if var in comp_data:
                val, _ = comp_data[var]
                results.append({
                    "attributeStatus": "Accepted",
                    "component": item["component"],
                    "variable": item["variable"],
                    "attributeType": attr,
                    "attributeValue": val,
                })
            else:
                results.append({
                    "attributeStatus": "UnknownVariable",
                    "component": item["component"],
                    "variable": item["variable"],
                })
        logger.info(f"GetVariables: returning {len(results)} results")
        return {"getVariableResult": results}

    async def handle_set_variables(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_07_CS: Updates device model variable values"""
        results = []
        for item in payload["setVariableData"]:
            comp = item["component"]["name"]
            var  = item["variable"]["name"]
            val  = item["attributeValue"]
            comp_data = self.device_model.get(comp, {})
            if var in comp_data:
                _, mutability = comp_data[var]
                if mutability == "ReadOnly":
                    status = "ReadOnly"
                else:
                    self.device_model[comp][var] = (val, mutability)
                    self._apply_variable_change(comp, var, val)
                    status = "Accepted"
            else:
                status = "UnknownVariable"
            results.append({
                "attributeStatus": status,
                "component": item["component"],
                "variable": item["variable"],
            })
        save_device_model(self.device_model)
        logger.info(f"SetVariables: processed {len(results)} variables")
        return {"setVariableResult": results}

    async def handle_get_base_report(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_*: Responds Accepted and sends NotifyReport asynchronously"""
        request_id = payload["requestId"]
        logger.info(f"GetBaseReport requestId={request_id}, reportBase={payload.get('reportBase')}")
        asyncio.create_task(self._send_notify_report(request_id))
        return {"status": "Accepted"}

    async def handle_get_report(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_10_CS: Responds Accepted and sends NotifyReport asynchronously"""
        request_id = payload["requestId"]
        logger.info(f"GetReport requestId={request_id}")
        asyncio.create_task(self._send_notify_report(request_id))
        return {"status": "Accepted"}

    async def _send_notify_report(self, request_id: int) -> None:
        """Sends NotifyReport with full device model"""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        report_data = []
        for comp_name, variables in self.device_model.items():
            for var_name, (value, mutability) in variables.items():
                report_data.append({
                    "component": {"name": comp_name},
                    "variable": {"name": var_name},
                    "variableAttribute": [{"type": "Actual", "value": value, "mutability": mutability}],
                })
        try:
            await self.ocpp_client.call("NotifyReport", {
                "requestId": request_id,
                "generatedAt": now,
                "seqNo": 0,
                "tbc": False,
                "reportData": report_data,
            })
            logger.info(f"NotifyReport sent for requestId={request_id}, {len(report_data)} entries")
        except Exception as e:
            logger.error(f"Failed to send NotifyReport: {e}")

    # ------------------------------------------------------------------
    # Block C — Authorization
    # ------------------------------------------------------------------

    async def handle_clear_cache(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_C_05_CS: Clears local authorization cache"""
        logger.info("ClearCache received — cache cleared")
        return {"status": "Accepted"}

    # ------------------------------------------------------------------
    # Block D — Local Authorization List
    # ------------------------------------------------------------------

    async def handle_send_local_list(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_D_01_CS: Installs or updates the local authorization list"""
        version    = payload["versionNumber"]
        update_type = payload["updateType"]
        new_entries = payload.get("localAuthorizationList", [])

        if update_type == "Full":
            self.local_auth_list = new_entries
        else:
            existing_tokens = {e["idToken"]["idToken"]: i for i, e in enumerate(self.local_auth_list)}
            for entry in new_entries:
                token = entry["idToken"]["idToken"]
                if token in existing_tokens:
                    self.local_auth_list[existing_tokens[token]] = entry
                else:
                    self.local_auth_list.append(entry)

        self.local_list_version = version
        logger.info(f"SendLocalList: {update_type}, version={version}, entries={len(self.local_auth_list)}")
        return {"status": "Accepted"}

    async def handle_get_local_list_version(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_D_02_CS: Returns the current local authorization list version"""
        logger.info(f"GetLocalListVersion: {self.local_list_version}")
        return {"versionNumber": self.local_list_version}

    # ------------------------------------------------------------------
    # Block E — Transaction
    # ------------------------------------------------------------------

    async def handle_get_transaction_status(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_E_12_CS: Returns whether a transaction is ongoing"""
        tx_id = payload.get("transactionId")
        ongoing = (self.transaction_id is not None) and (tx_id is None or self.transaction_id == tx_id)
        logger.info(f"GetTransactionStatus txId={tx_id}: ongoingIndicator={ongoing}")
        return {"messagesInQueue": False, "ongoingIndicator": ongoing}

    # ------------------------------------------------------------------
    # Block F — Remote Control
    # ------------------------------------------------------------------

    async def handle_request_start_transaction(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_F_*: Remote start transaction from CSMS"""
        if not self.is_evse_available:
            logger.warning("RequestStartTransaction rejected: EVSE not available")
            return {"status": "Rejected"}
        if self.transaction_id:
            logger.warning("RequestStartTransaction rejected: transaction already active")
            return {"status": "Rejected"}

        id_token = payload["idToken"]
        logger.info(f"RequestStartTransaction: authorizing token {id_token.get('idToken')}")

        if self._get_bool("AuthCtrlr", "AuthorizeRemoteStart", True):
            try:
                res = await self.ocpp_client.call("Authorize", {"idToken": id_token})
                if not (res and res.get("idTokenInfo", {}).get("status") == "Accepted"):
                    logger.warning("RequestStartTransaction: Authorize rejected")
                    return {"status": "Rejected"}
            except Exception as e:
                logger.error(f"Authorize failed during RequestStartTransaction: {e}")
                return {"status": "Rejected"}

        self.is_authorized = True
        asyncio.create_task(self._try_start_transaction())
        return {"status": "Accepted"}

    async def handle_request_stop_transaction(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_F_*: Remote stop transaction from CSMS"""
        tx_id = payload["transactionId"]
        if self.transaction_id == tx_id:
            logger.info(f"RequestStopTransaction accepted: txId={tx_id}")
            asyncio.create_task(self.stop_transaction("Remote"))
            return {"status": "Accepted"}
        logger.warning(f"RequestStopTransaction rejected: txId={tx_id} not active (active={self.transaction_id})")
        return {"status": "Rejected"}

    async def handle_unlock_connector(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_F_04_CS: Unlocks the specified connector"""
        evse_id      = payload.get("evseId", self.evse_id)
        connector_id = payload.get("connectorId", self.connector_id)
        logger.info(f"UnlockConnector evseId={evse_id} connectorId={connector_id}")
        return {"status": "Unlocked"}

    # ------------------------------------------------------------------
    # Block G — Availability
    # ------------------------------------------------------------------

    async def handle_change_availability(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_G_01_CS: Changes the operational status of the EVSE"""
        status = payload["operationalStatus"]
        logger.info(f"ChangeAvailability: {status}")
        if status == "Inoperative":
            if self.transaction_id:
                # Cannot change immediately while charging; will apply after transaction ends
                return {"status": "Scheduled"}
            self.is_evse_available = False
            self.device_model["EVSE"]["AvailabilityState"] = ("Inoperative", "ReadWrite")
        else:
            self.is_evse_available = True
            self.device_model["EVSE"]["AvailabilityState"] = ("Available", "ReadWrite")
        save_device_model(self.device_model)
        return {"status": "Accepted"}

    # ------------------------------------------------------------------
    # Block I — Smart Charging
    # ------------------------------------------------------------------

    async def handle_set_charging_profile(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_I_01_CS: Stores a charging profile"""
        profile = payload["chargingProfile"]
        self.charging_profiles[profile["id"]] = profile
        logger.info(f"SetChargingProfile: stored profile id={profile['id']}, stackLevel={profile['stackLevel']}")
        return {"status": "Accepted"}

    async def handle_get_charging_profiles(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_I_02_CS: Returns charging profiles; sends ReportChargingProfiles asynchronously"""
        request_id = payload["requestId"]
        if not self.charging_profiles:
            logger.info(f"GetChargingProfiles requestId={request_id}: no profiles")
            return {"status": "NoProfiles"}
        asyncio.create_task(self._send_report_charging_profiles(request_id))
        return {"status": "Accepted"}

    async def _send_report_charging_profiles(self, request_id: int) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            await self.ocpp_client.call("ReportChargingProfiles", {
                "requestId": request_id,
                "chargingLimitSource": "CSO",
                "evseId": self.evse_id,
                "chargingProfile": list(self.charging_profiles.values()),
                "tbc": False,
            })
            logger.info(f"ReportChargingProfiles sent for requestId={request_id}")
        except Exception as e:
            logger.error(f"Failed to send ReportChargingProfiles: {e}")

    # ------------------------------------------------------------------
    # Block J — Metering (standalone MeterValues)
    # ------------------------------------------------------------------

    async def send_meter_values(self, evse_id: Optional[int] = None, transaction_id: Optional[str] = None) -> None:
        """Sends a standalone MeterValues message (Block J)"""
        evse_id = evse_id or self.evse_id
        meter_data = self.power_contactor_hal.read_meter_values()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = {
            "evseId": evse_id,
            "meterValue": [{
                "timestamp": now,
                "sampledValue": [
                    {"value": str(meter_data.get("power",   0.0)), "measurand": "Power.Active.Import", "unit": "W"},
                    {"value": str(meter_data.get("voltage", 0.0)), "measurand": "Voltage",             "unit": "V"},
                    {"value": str(meter_data.get("current", 0.0)), "measurand": "Current.Import",      "unit": "A"},
                ],
            }],
        }
        if transaction_id:
            payload["transactionId"] = transaction_id
        try:
            await self.ocpp_client.call("MeterValues", payload)
            logger.info(f"MeterValues sent for evseId={evse_id}")
        except Exception as e:
            logger.error(f"Failed to send MeterValues: {e}")

    # ------------------------------------------------------------------
    # Block K — Firmware Management
    # ------------------------------------------------------------------

    async def handle_update_firmware(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_K_01_CS: Accepts firmware update and simulates the update sequence"""
        request_id = payload["requestId"]
        location   = payload.get("firmware", {}).get("location", "unknown")
        logger.info(f"UpdateFirmware requestId={request_id}, location={location}")
        asyncio.create_task(self._simulate_firmware_update(request_id))
        return {"status": "Accepted"}

    async def _simulate_firmware_update(self, request_id: int) -> None:
        for fw_status in ["Downloading", "Downloaded", "Installing", "Installed"]:
            await asyncio.sleep(2)
            try:
                await self.ocpp_client.call("FirmwareStatusNotification",
                                            {"status": fw_status, "requestId": request_id})
                logger.info(f"FirmwareStatusNotification: {fw_status}")
            except Exception as e:
                logger.error(f"Failed to send FirmwareStatusNotification ({fw_status}): {e}")

    async def handle_get_log(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_K_03_CS: Accepts log upload request and simulates upload sequence"""
        request_id = payload["requestId"]
        log_type   = payload.get("logType", "DiagnosticsLog")
        logger.info(f"GetLog requestId={request_id}, logType={log_type}")
        asyncio.create_task(self._simulate_log_upload(request_id))
        return {"status": "Accepted"}

    async def _simulate_log_upload(self, request_id: int) -> None:
        await asyncio.sleep(2)
        try:
            await self.ocpp_client.call("LogStatusNotification",
                                        {"status": "Uploading", "requestId": request_id})
            logger.info("LogStatusNotification: Uploading")
        except Exception as e:
            logger.error(f"Failed to send LogStatusNotification (Uploading): {e}")
        await asyncio.sleep(2)
        try:
            await self.ocpp_client.call("LogStatusNotification",
                                        {"status": "Uploaded", "requestId": request_id})
            logger.info("LogStatusNotification: Uploaded")
        except Exception as e:
            logger.error(f"Failed to send LogStatusNotification (Uploaded): {e}")

    # ------------------------------------------------------------------
    # Block L — Remote Trigger
    # ------------------------------------------------------------------

    async def handle_trigger_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_L_*: Triggers the requested message to be sent"""
        requested = payload["requestedMessage"]
        logger.info(f"TriggerMessage: {requested}")

        supported = {
            "BootNotification", "Heartbeat", "StatusNotification",
            "MeterValues", "FirmwareStatusNotification", "LogStatusNotification",
            "TransactionEvent",
        }
        if requested not in supported:
            return {"status": "NotImplemented"}

        asyncio.create_task(self._send_triggered_message(requested))
        return {"status": "Accepted"}

    async def _send_triggered_message(self, requested: str) -> None:
        try:
            if requested == "BootNotification":
                await self.boot_routine()

            elif requested == "Heartbeat":
                await self.ocpp_client.call("Heartbeat", {})

            elif requested == "StatusNotification":
                status = self.connector_hal.status or "Available"
                await self.ocpp_client.call("StatusNotification", {
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "connectorStatus": status,
                    "evseId": self.evse_id,
                    "connectorId": self.connector_id,
                })

            elif requested == "MeterValues":
                await self.send_meter_values(transaction_id=self.transaction_id)

            elif requested == "FirmwareStatusNotification":
                await self.ocpp_client.call("FirmwareStatusNotification",
                                            {"status": "Idle", "requestId": 0})

            elif requested == "LogStatusNotification":
                await self.ocpp_client.call("LogStatusNotification",
                                            {"status": "Idle", "requestId": 0})

            elif requested == "TransactionEvent":
                if self.transaction_id:
                    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    self._tx_seq_no += 1
                    await self.ocpp_client.call("TransactionEvent", {
                        "eventType": "Updated",
                        "timestamp": now,
                        "triggerReason": "Trigger",
                        "seqNo": self._tx_seq_no,
                        "transactionInfo": {"transactionId": self.transaction_id},
                    })
        except Exception as e:
            logger.error(f"Failed to send triggered message {requested}: {e}")

    # ------------------------------------------------------------------
    # 충전 흐름 (RFID / 케이블 / State C)
    # ------------------------------------------------------------------

    async def handle_rfid_scan(self, raw_uid: str) -> None:
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

    async def simulate_cable_plugged(self) -> None:
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

    async def simulate_cable_unplugged(self) -> None:
        logger.info("Cable unplugged. Connector Available.")
        self.connector_hal.status = "Available"
        if self.transaction_id:
            if self._get_bool("TxCtrlr", "StopTxOnEVSideDisconnect", True):
                logger.info("Transaction active during unplug. Stopping transaction (EVDisconnected).")
                await self.stop_transaction("EVDisconnected")
            else:
                logger.info("Cable unplugged but StopTxOnEVSideDisconnect=false — transaction continues.")

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

    async def _meter_values_loop(self, transaction_id: str) -> None:
        """TC_J_02_CS: Periodically reports TransactionEvent(Updated) with MeterValues"""
        while self.transaction_id == transaction_id:
            interval = self._get_int("SampledDataCtrlr", "SampledDataTxUpdatedInterval", 60)
            await asyncio.sleep(interval)

            meter_data = self.power_contactor_hal.read_meter_values()
            real_power = meter_data.get("power", 0.0)
            self.meter_value += real_power * (interval / 3600.0)  # Wh

            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._tx_seq_no += 1
            measurands = self._get_param("SampledDataCtrlr", "SampledDataTxUpdatedMeasurands",
                                         "Energy.Active.Import.Register")
            payload = {
                "eventType": "Updated",
                "timestamp": now_iso,
                "triggerReason": "MeterValuePeriodic",
                "seqNo": self._tx_seq_no,
                "transactionInfo": {
                    "transactionId": self.transaction_id
                },
                "meterValue": [
                    {
                        "timestamp": now_iso,
                        "sampledValue": self._build_sampled_values(
                            measurands, meter_data, "Sample.Periodic", self.meter_value)
                    }
                ]
            }
            try:
                await self.ocpp_client.call("TransactionEvent", payload, allow_offline=True)
            except Exception as e:
                logger.error(f"Failed to send meter value: {e}")

    async def _try_start_transaction(self) -> None:
        if self.is_authorized and self.connector_hal.status == "Occupied":
            if not self.transaction_id:
                self.transaction_id = str(uuid.uuid4())
                self.meter_value = 0.0
                self._state_c_active = False
                self._tx_seq_no = 0
                now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                meter_data = self.power_contactor_hal.read_meter_values()
                measurands = self._get_param("SampledDataCtrlr", "SampledDataTxStartedMeasurands",
                                             "Energy.Active.Import.Register")
                payload = {
                    "eventType": "Started",
                    "timestamp": now_iso,
                    "triggerReason": "Authorized",
                    "seqNo": self._tx_seq_no,
                    "transactionInfo": {
                        "transactionId": self.transaction_id
                    },
                    "meterValue": [
                        {
                            "timestamp": now_iso,
                            "sampledValue": self._build_sampled_values(
                                measurands, meter_data, "Transaction.Begin", self.meter_value)
                        }
                    ]
                }
                await self.ocpp_client.call("TransactionEvent", payload, allow_offline=True)
                self.power_contactor_hal.control_relay("Close")
                # Drop to 53% PWM (32 Amps continuous limit) to allow vehicle onboard charger to pull power
                self.power_contactor_hal.set_pwm_duty(53)

                if self._meter_task:
                    self._meter_task.cancel()
                self._meter_task = asyncio.create_task(self._meter_values_loop(self.transaction_id))

    async def handle_state_c(self) -> None:
        """Called by main.py ADC monitor when CP voltage drops to +6V (< 40000 ADC)"""
        if self.transaction_id and not self._state_c_active:
            self._state_c_active = True
            logger.info("Control Pilot dropped to State C (+6V). EV is Charging!")
            self._tx_seq_no += 1
            payload = {
                "eventType": "Updated",
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "triggerReason": "ChargingStateChanged",
                "seqNo": self._tx_seq_no,
                "transactionInfo": {
                    "transactionId": self.transaction_id,
                    "chargingState": "Charging"
                }
            }
            await self.ocpp_client.call("TransactionEvent", payload, allow_offline=True)

    async def stop_transaction(self, stopped_reason: str = "Local") -> None:
        if self.transaction_id:
            self.power_contactor_hal.control_relay("Open")
            # Restore 100% PWM (+12V Standing State)
            self.power_contactor_hal.set_pwm_duty(100)

            if self._meter_task:
                self._meter_task.cancel()
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._tx_seq_no += 1

            trigger_reason_map = {
                "Local":           "StopAuthorized",
                "Remote":          "RemoteStop",
                "EVDisconnected":  "EVDeparted",
            }
            trigger_reason = trigger_reason_map.get(stopped_reason, "StopAuthorized")

            meter_data = self.power_contactor_hal.read_meter_values()
            measurands = self._get_param("SampledDataCtrlr", "SampledDataTxEndedMeasurands",
                                         "Energy.Active.Import.Register")
            payload = {
                "eventType": "Ended",
                "timestamp": now_iso,
                "triggerReason": trigger_reason,
                "seqNo": self._tx_seq_no,
                "transactionInfo": {
                    "transactionId": self.transaction_id,
                    "stoppedReason": stopped_reason
                },
                "meterValue": [
                    {
                        "timestamp": now_iso,
                        "sampledValue": self._build_sampled_values(
                            measurands, meter_data, "Transaction.End", self.meter_value)
                    }
                ]
            }
            await self.ocpp_client.call("TransactionEvent", payload, allow_offline=True)
            self.transaction_id = None
            self.is_authorized = False
            self._state_c_active = False

            # Re-apply availability if it was scheduled as Inoperative
            if not self.is_evse_available:
                self.device_model["EVSE"]["AvailabilityState"] = ("Inoperative", "ReadWrite")

    # ------------------------------------------------------------------
    # Block A — Security and Certificates (TC_A_*)
    # ------------------------------------------------------------------

    async def trigger_security_event(self, type: str, info: str) -> None:
        """TC_A_01_CS: SecurityEventNotification"""
        payload = {
            "type": type,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "techInfo": info
        }
        await self.ocpp_client.call("SecurityEventNotification", payload)

    async def trigger_sign_certificate(self, csr: str, cert_type: str = "ChargingStationCertificate") -> None:
        """TC_A_04_CS, TC_A_05_CS: SignCertificate"""
        payload = {
            "csr": csr,
            "certificateType": cert_type
        }
        await self.ocpp_client.call("SignCertificate", payload)

    async def trigger_get_15118_ev_certificate(self, iso15118_schema_version: str) -> None:
        """TC_A_15_CS: Get15118EVCertificate"""
        payload = {
            "iso15118SchemaVersion": iso15118_schema_version,
            "action": "Install",
            "exiRequest": "dummy_exi_data"
        }
        await self.ocpp_client.call("Get15118EVCertificate", payload)

    @staticmethod
    def _make_cert_hash_data(pem: str) -> Dict[str, str]:
        """PEM 문자열로부터 결정적 hash data를 생성한다.
        issuerNameHash/issuerKeyHash는 PEM SHA-256 digest로 근사한다.
        실제 X.509 파싱 없이 OCTT 포맷 요건(필드 존재·타입)을 충족한다."""
        digest = hashlib.sha256(pem.encode()).hexdigest()  # 64 hex chars
        return {
            "hashAlgorithm": "SHA256",
            "issuerNameHash": digest,
            "issuerKeyHash":  digest,
            "serialNumber":   digest[:16],  # 8-byte hex, maxLength 40 이내
        }

    async def handle_install_certificate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_06_CS, TC_A_07_CS: CA 인증서를 파일로 저장하고 메모리에 등록한다."""
        cert_type: str = payload["certificateType"]
        pem: str       = payload["certificate"]

        hash_data = self._make_cert_hash_data(pem)
        serial    = hash_data["serialNumber"]

        try:
            os.makedirs(self._cert_dir, exist_ok=True)
            cert_path = os.path.join(self._cert_dir, f"{cert_type}.pem")
            with open(cert_path, "w") as f:
                f.write(pem)
        except OSError as e:
            logger.error(f"InstallCertificate: failed to write file: {e}")
            return {"status": "Failed"}

        self.installed_certificates[serial] = {
            "certificateType":    cert_type,
            "certificateHashData": hash_data,
            "pem_path":           cert_path,
        }
        logger.info(f"InstallCertificate: type={cert_type} serial={serial} path={cert_path}")
        return {"status": "Accepted"}

    async def handle_get_installed_certificate_ids(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_09_CS, TC_A_10_CS: 설치된 인증서 목록을 반환한다."""
        requested_types: Optional[List[str]] = payload.get("certificateType")

        chain = [
            {
                "certificateType":    entry["certificateType"],
                "certificateHashData": entry["certificateHashData"],
            }
            for entry in self.installed_certificates.values()
            if requested_types is None or entry["certificateType"] in requested_types
        ]

        if not chain:
            return {"status": "NotFound"}
        return {"status": "Accepted", "certificateHashDataChain": chain}

    async def handle_delete_certificate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_11_CS, TC_A_12_CS: 설치된 인증서를 삭제한다."""
        hash_data: Dict = payload.get("certificateHashData", {})
        serial: Optional[str] = hash_data.get("serialNumber")

        if not serial or serial not in self.installed_certificates:
            return {"status": "NotFound"}

        entry = self.installed_certificates[serial]
        try:
            if os.path.exists(entry["pem_path"]):
                os.remove(entry["pem_path"])
        except OSError as e:
            logger.error(f"DeleteCertificate: failed to remove file: {e}")
            return {"status": "Failed"}

        del self.installed_certificates[serial]
        logger.info(f"DeleteCertificate: removed serial={serial}")
        return {"status": "Accepted"}

    async def handle_get_certificate_status(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_13_CS, TC_A_14_CS"""
        logger.info("Handling GetCertificateStatus")
        return {"status": "Accepted"}

    async def handle_certificate_signed(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_19_CS, TC_A_20_CS: 서명된 클라이언트 인증서를 저장한다.
        즉시 재연결하지 않고 플래그만 세팅해 다음 재시작 시 적용한다."""
        cert_chain_pem: str = payload["certificateChain"]
        cert_type: str      = payload.get("certificateType", "ChargingStationCertificate")

        filename = "client.crt" if cert_type == "ChargingStationCertificate" else "v2g_client.crt"
        cert_path = os.path.join(self._cert_dir, filename)

        try:
            os.makedirs(self._cert_dir, exist_ok=True)
            with open(cert_path, "w") as f:
                f.write(cert_chain_pem)
            self._pending_client_cert = cert_path
            logger.info(f"CertificateSigned: saved to {cert_path} — applies on next restart")
        except OSError as e:
            logger.error(f"CertificateSigned: failed to write file: {e}")
            return {"status": "Rejected"}

        return {"status": "Accepted"}

    # ------------------------------------------------------------------
    # Block B (추가) — SetNetworkProfile
    # ------------------------------------------------------------------

    async def handle_set_network_profile(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_13_CS: Sets a network connection profile slot"""
        slot = payload["configurationSlot"]
        ocpp_version = payload.get("connectionData", {}).get("ocppVersion", "unknown")
        logger.info(f"SetNetworkProfile: slot={slot}, ocppVersion={ocpp_version}")
        return {"status": "Accepted"}

    # ------------------------------------------------------------------
    # Block E (추가) — CostUpdated
    # ------------------------------------------------------------------

    async def handle_cost_updated(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_E_13_CS: Receives updated cost for ongoing transaction"""
        total_cost   = payload["totalCost"]
        tx_id        = payload["transactionId"]
        logger.info(f"CostUpdated: txId={tx_id}, totalCost={total_cost}")
        return {}

    # ------------------------------------------------------------------
    # Block H — Reservation
    # ------------------------------------------------------------------

    async def handle_reserve_now(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_H_01_CS: Reserves the EVSE for a specific idToken"""
        reservation_id   = payload["id"]
        expiry           = payload["expiryDateTime"]
        id_token         = payload["idToken"]
        evse_id          = payload.get("evseId", self.evse_id)

        if not self.is_evse_available:
            logger.warning(f"ReserveNow rejected: EVSE unavailable (reservationId={reservation_id})")
            return {"status": "Unavailable"}
        if self.transaction_id:
            logger.warning(f"ReserveNow rejected: EVSE occupied (reservationId={reservation_id})")
            return {"status": "Occupied"}

        self.reservations[reservation_id] = {
            "expiryDateTime": expiry,
            "idToken": id_token,
            "evseId": evse_id,
        }
        logger.info(f"ReserveNow accepted: reservationId={reservation_id}, evseId={evse_id}, expiry={expiry}")
        return {"status": "Accepted"}

    async def handle_cancel_reservation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_H_02_CS: Cancels an existing reservation"""
        reservation_id = payload["reservationId"]
        if reservation_id in self.reservations:
            del self.reservations[reservation_id]
            logger.info(f"CancelReservation: reservationId={reservation_id} cancelled")
            asyncio.create_task(self._send_reservation_status_update(reservation_id, "Removed"))
            return {"status": "Accepted"}
        logger.warning(f"CancelReservation: reservationId={reservation_id} not found")
        return {"status": "Rejected"}

    async def _send_reservation_status_update(self, reservation_id: int, status: str) -> None:
        try:
            await self.ocpp_client.call("ReservationStatusUpdate", {
                "reservationId": reservation_id,
                "reservationUpdateStatus": status,
            })
            logger.info(f"ReservationStatusUpdate sent: reservationId={reservation_id}, status={status}")
        except Exception as e:
            logger.error(f"Failed to send ReservationStatusUpdate: {e}")

    # ------------------------------------------------------------------
    # Block I (추가) — ClearChargingProfile / GetCompositeSchedule
    # ------------------------------------------------------------------

    async def handle_clear_charging_profile(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        profile_filter = payload.get("chargingProfile", {})
        profile_id     = profile_filter.get("chargingProfileId")

        if profile_id is not None:
            if profile_id in self.charging_profiles:
                del self.charging_profiles[profile_id]
                logger.info(f"ClearChargingProfile: removed profileId={profile_id}")
                return {"status": "Accepted"}
            logger.warning(f"ClearChargingProfile: profileId={profile_id} not found")
            return {"status": "Unknown"}

        # No specific id → clear all matching purpose/stackLevel
        purpose = profile_filter.get("chargingProfilePurpose")
        stack   = profile_filter.get("stackLevel")
        before  = len(self.charging_profiles)
        self.charging_profiles = {
            pid: p for pid, p in self.charging_profiles.items()
            if (purpose and p.get("chargingProfilePurpose") != purpose)
            or (stack is not None and p.get("stackLevel") != stack)
        }
        removed = before - len(self.charging_profiles)
        logger.info(f"ClearChargingProfile: removed {removed} profiles (purpose={purpose}, stack={stack})")
        return {"status": "Accepted" if removed > 0 else "Unknown"}

    async def handle_get_composite_schedule(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        duration = payload["duration"]
        evse_id  = payload["evseId"]
        unit     = payload.get("chargingRateUnit", "W")
        logger.info(f"GetCompositeSchedule: evseId={evse_id}, duration={duration}s, unit={unit}")

        if not self.charging_profiles:
            return {"status": "Rejected"}

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return {
            "status": "Accepted",
            "schedule": {
                "id": evse_id,
                "startSchedule": now,
                "duration": duration,
                "chargingRateUnit": unit,
                "chargingSchedulePeriod": [{"startPeriod": 0, "limit": 7400.0}],
            },
        }

    # ------------------------------------------------------------------
    # Block K (추가) — PublishFirmware / UnpublishFirmware
    # ------------------------------------------------------------------

    async def handle_publish_firmware(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        location   = payload["location"]
        checksum   = payload["checksum"]
        request_id = payload["requestId"]
        self.published_firmware[checksum] = location
        logger.info(f"PublishFirmware: requestId={request_id}, checksum={checksum}, location={location}")
        asyncio.create_task(self._simulate_publish_firmware(request_id, checksum))
        return {"status": "Accepted"}

    async def _simulate_publish_firmware(self, request_id: int, checksum: str) -> None:
        for status in ["DownloadScheduled", "Downloading", "Downloaded", "ChecksumVerified", "Published"]:
            await asyncio.sleep(2)
            try:
                await self.ocpp_client.call("PublishFirmwareStatusNotification",
                                            {"status": status, "requestId": request_id})
                logger.info(f"PublishFirmwareStatusNotification: {status}")
            except Exception as e:
                logger.error(f"Failed to send PublishFirmwareStatusNotification ({status}): {e}")

    async def handle_unpublish_firmware(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        checksum = payload["checksum"]
        if checksum in self.published_firmware:
            del self.published_firmware[checksum]
            logger.info(f"UnpublishFirmware: checksum={checksum} unpublished")
            return {"status": "Unpublished"}
        logger.warning(f"UnpublishFirmware: checksum={checksum} not found")
        return {"status": "NoFirmware"}

    # ------------------------------------------------------------------
    # Block M — Data Transfer / CustomerInformation
    # ------------------------------------------------------------------

    async def handle_data_transfer(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        vendor_id  = payload["vendorId"]
        message_id = payload.get("messageId", "")
        data       = payload.get("data")
        logger.info(f"DataTransfer: vendorId={vendor_id}, messageId={message_id}, data={data}")
        return {"status": "Accepted"}

    async def send_data_transfer(self, vendor_id: str, message_id: str = None, data: Any = None):
        """CS→CSMS: Vendor-specific data transfer"""
        payload: Dict[str, Any] = {"vendorId": vendor_id}
        if message_id:
            payload["messageId"] = message_id
        if data is not None:
            payload["data"] = data
        try:
            res = await self.ocpp_client.call("DataTransfer", payload)
            logger.info(f"DataTransfer sent: vendorId={vendor_id}, status={res.get('status')}")
            return res
        except Exception as e:
            logger.error(f"Failed to send DataTransfer: {e}")

    async def handle_customer_information(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        request_id = payload["requestId"]
        report     = payload["report"]
        clear      = payload["clear"]
        logger.info(f"CustomerInformation: requestId={request_id}, report={report}, clear={clear}")
        if report:
            asyncio.create_task(self._send_notify_customer_information(request_id))
        return {"status": "Accepted"}

    async def _send_notify_customer_information(self, request_id: int) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            await self.ocpp_client.call("NotifyCustomerInformation", {
                "requestId": request_id,
                "data": "No customer data available",
                "seqNo": 0,
                "generatedAt": now,
                "tbc": False,
            })
            logger.info(f"NotifyCustomerInformation sent for requestId={request_id}")
        except Exception as e:
            logger.error(f"Failed to send NotifyCustomerInformation: {e}")

    # ------------------------------------------------------------------
    # Block N — Display Messages
    # ------------------------------------------------------------------

    async def handle_set_display_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        message    = payload["message"]
        message_id = message.get("id")
        self.display_messages[message_id] = message
        logger.info(f"SetDisplayMessage: id={message_id}, priority={message.get('priority')}")
        return {"status": "Accepted"}

    async def handle_get_display_messages(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        request_id = payload["requestId"]
        ids_filter = payload.get("id", [])
        priority   = payload.get("priority")
        state      = payload.get("state")

        msgs = list(self.display_messages.values())
        if ids_filter:
            msgs = [m for m in msgs if m.get("id") in ids_filter]
        if priority:
            msgs = [m for m in msgs if m.get("priority") == priority]
        if state:
            msgs = [m for m in msgs if m.get("state") == state]

        if not msgs:
            logger.info(f"GetDisplayMessages requestId={request_id}: no messages matched")
            return {"status": "Unknown"}

        asyncio.create_task(self._send_notify_display_messages(request_id, msgs))
        return {"status": "Accepted"}

    async def _send_notify_display_messages(self, request_id: int, messages: List[Dict]) -> None:
        try:
            await self.ocpp_client.call("NotifyDisplayMessages", {
                "requestId": request_id,
                "messageInfo": messages,
                "tbc": False,
            })
            logger.info(f"NotifyDisplayMessages sent: requestId={request_id}, count={len(messages)}")
        except Exception as e:
            logger.error(f"Failed to send NotifyDisplayMessages: {e}")

    async def handle_clear_display_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        message_id = payload["id"]
        if message_id in self.display_messages:
            del self.display_messages[message_id]
            logger.info(f"ClearDisplayMessage: id={message_id} removed")
            return {"status": "Accepted"}
        logger.warning(f"ClearDisplayMessage: id={message_id} not found")
        return {"status": "Unknown"}

    # ------------------------------------------------------------------
    # Block O — Variable Monitoring
    # ------------------------------------------------------------------

    async def handle_set_variable_monitoring(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        results = []
        for item in payload["setMonitoringData"]:
            monitor_id = item.get("id", len(self.variable_monitoring) + 1)
            comp       = item.get("component", {}).get("name", "")
            var        = item.get("variable", {}).get("name", "")
            self.variable_monitoring[monitor_id] = item
            results.append({
                "id": monitor_id,
                "status": "Accepted",
                "type": item.get("type", "UpperThreshold"),
                "component": item["component"],
                "variable": item["variable"],
            })
            logger.info(f"SetVariableMonitoring: id={monitor_id}, component={comp}, variable={var}")
        return {"setMonitoringResult": results}

    async def handle_get_monitoring_report(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        request_id = payload["requestId"]
        logger.info(f"GetMonitoringReport: requestId={request_id}")
        asyncio.create_task(self._send_notify_monitoring_report(request_id))
        return {"status": "Accepted"}

    async def _send_notify_monitoring_report(self, request_id: int) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        monitor_data = []
        for monitor_id, item in self.variable_monitoring.items():
            monitor_data.append({
                "component": item["component"],
                "variable": item["variable"],
                "variableMonitoring": [{
                    "id": monitor_id,
                    "transaction": False,
                    "value": item.get("value", 0),
                    "type": item.get("type", "UpperThreshold"),
                    "severity": item.get("severity", 0),
                }],
            })
        try:
            await self.ocpp_client.call("NotifyMonitoringReport", {
                "requestId": request_id,
                "seqNo": 0,
                "generatedAt": now,
                "tbc": False,
                "monitor": monitor_data if monitor_data else None,
            } if monitor_data else {
                "requestId": request_id,
                "seqNo": 0,
                "generatedAt": now,
                "tbc": False,
            })
            logger.info(f"NotifyMonitoringReport sent: requestId={request_id}, entries={len(monitor_data)}")
        except Exception as e:
            logger.error(f"Failed to send NotifyMonitoringReport: {e}")

    async def handle_clear_variable_monitoring(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        results = []
        for monitor_id in payload["id"]:
            if monitor_id in self.variable_monitoring:
                del self.variable_monitoring[monitor_id]
                results.append({"id": monitor_id, "status": "Accepted"})
                logger.info(f"ClearVariableMonitoring: id={monitor_id} removed")
            else:
                results.append({"id": monitor_id, "status": "NotFound"})
                logger.warning(f"ClearVariableMonitoring: id={monitor_id} not found")
        return {"clearMonitoringResult": results}

    async def handle_set_monitoring_base(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        base = payload["monitoringBase"]
        self.monitoring_base = base
        logger.info(f"SetMonitoringBase: {base}")
        if base == "FactoryDefault":
            self.variable_monitoring.clear()
        return {"status": "Accepted"}

    async def handle_set_monitoring_level(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        severity = payload["severity"]
        self.monitoring_level = severity
        logger.info(f"SetMonitoringLevel: severity={severity}")
        return {"status": "Accepted"}

    # ------------------------------------------------------------------
    # Block O — CS→CSMS 발신: NotifyEvent
    # ------------------------------------------------------------------

    async def send_notify_event(self, component_name: str, variable_name: str,
                                actual_value: str, event_trigger: str = "Alerting",
                                event_notification_type: str = "CustomMonitor") -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            await self.ocpp_client.call("NotifyEvent", {
                "generatedAt": now,
                "seqNo": 0,
                "tbc": False,
                "eventData": [{
                    "eventId": 1,
                    "timestamp": now,
                    "trigger": event_trigger,
                    "cause": 0,
                    "actualValue": actual_value,
                    "eventNotificationType": event_notification_type,
                    "component": {"name": component_name},
                    "variable": {"name": variable_name},
                }],
            })
            logger.info(f"NotifyEvent sent: {component_name}.{variable_name}={actual_value}")
        except Exception as e:
            logger.error(f"Failed to send NotifyEvent: {e}")
