import asyncio
import hashlib
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from .ocpp_client import OCPPClient
from .hal import ConnectorHAL, TokenReaderHAL, PowerContactorHAL
from .persistence import (
    load_device_model,
    save_device_model,
    load_network_profiles,
    save_network_profile,
    load_cert_metadata,
    save_cert_metadata,
    load_admin_state,
    save_admin_state,
    load_auth_cache,
    save_auth_cache,
    load_installed_certificates,
    save_installed_certificates,
)
from .station_config import StationConfig

logger = logging.getLogger(__name__)


# OCPP 2.0.1 DataEnumType per variable — used by SetVariables to reject
# attributeValue whose format doesn't match the declared type (TC_B_11_CS).
# Variables not listed default to "string" (accept any).
_VAR_DATA_TYPES: Dict[tuple, str] = {
    ("EVSE", "Power"): "decimal",
    ("EVSE", "Available"): "boolean",
    ("EVSE", "AvailabilityState"): "OptionList",
    ("EVSE", "SupplyPhases"): "integer",
    ("Connector", "Available"): "boolean",
    ("Connector", "AvailabilityState"): "OptionList",
    ("Connector", "SupplyPhases"): "integer",
    ("ChargingStation", "AvailabilityState"): "OptionList",
    ("ChargingStation", "Available"): "boolean",
    ("ChargingStation", "SupplyPhases"): "integer",
    ("TokenReader", "Enabled"): "boolean",
    ("SampledDataCtrlr", "TxUpdatedInterval"): "integer",
    ("SampledDataCtrlr", "TxEndedInterval"): "integer",
    ("SampledDataCtrlr", "TxUpdatedMeasurands"): "MemberList",
    ("SampledDataCtrlr", "TxStartedMeasurands"): "MemberList",
    ("SampledDataCtrlr", "TxEndedMeasurands"): "MemberList",
    ("AlignedDataCtrlr", "Interval"): "integer",
    ("AlignedDataCtrlr", "TxEndedInterval"): "integer",
    ("AlignedDataCtrlr", "Measurands"): "MemberList",
    ("AlignedDataCtrlr", "TxEndedMeasurands"): "MemberList",
    ("HeartbeatCtrlr", "HeartbeatInterval"): "integer",
    ("TxCtrlr", "StopTxOnEVSideDisconnect"): "boolean",
    ("TxCtrlr", "StopTxOnInvalidId"): "boolean",
    ("TxCtrlr", "EVConnectionTimeOut"): "integer",
    ("TxCtrlr", "TxStartPoint"): "MemberList",
    ("TxCtrlr", "TxStopPoint"): "MemberList",
    ("AuthCtrlr", "AuthorizeRemoteStart"): "boolean",
    ("AuthCtrlr", "LocalAuthorizeOffline"): "boolean",
    ("AuthCtrlr", "LocalPreAuthorize"): "boolean",
    ("AuthCtrlr", "OfflineTxForUnknownIdEnabled"): "boolean",
    ("OCPPCommCtrlr", "MessageAttempts"): "integer",
    ("OCPPCommCtrlr", "MessageAttemptInterval"): "integer",
    ("OCPPCommCtrlr", "OfflineThreshold"): "integer",
    ("OCPPCommCtrlr", "NetworkProfileConnectionAttempts"): "integer",
    ("OCPPCommCtrlr", "ActiveNetworkProfile"): "integer",
    ("OCPPCommCtrlr", "NetworkConfigurationPriority"): "SequenceList",
    ("OCPPCommCtrlr", "QueueAllMessages"): "boolean",
    ("OCPPCommCtrlr", "RetryBackOffWaitMinimum"): "integer",
    ("OCPPCommCtrlr", "RetryBackOffRepeatTimes"): "integer",
    ("OCPPCommCtrlr", "RetryBackOffRandomRange"): "integer",
    ("OCPPCommCtrlr", "MessageTimeout"): "integer",
    ("OCPPCommCtrlr", "ResetRetries"): "integer",
    ("OCPPCommCtrlr", "UnlockOnEVSideDisconnect"): "boolean",
    ("OCPPCommCtrlr", "WebSocketPingInterval"): "integer",
    ("OCPPCommCtrlr", "FileTransferProtocols"): "MemberList",
    ("ClockCtrlr", "DateTime"): "dateTime",
    ("ClockCtrlr", "TimeSource"): "SequenceList",
    ("DeviceDataCtrlr", "BytesPerMessage"): "integer",
    ("DeviceDataCtrlr", "ItemsPerMessage"): "integer",
    ("LocalAuthListCtrlr", "Enabled"): "boolean",
    ("LocalAuthListCtrlr", "Entries"): "integer",
    ("LocalAuthListCtrlr", "BytesPerMessage"): "integer",
    ("LocalAuthListCtrlr", "ItemsPerMessage"): "integer",
    ("AuthCacheCtrlr", "Enabled"): "boolean",
    ("AuthCacheCtrlr", "LifeTime"): "integer",
    ("AuthCacheCtrlr", "DisablePostAuthorize"): "boolean",
    ("SmartChargingCtrlr", "Enabled"): "boolean",
    ("SmartChargingCtrlr", "Entries"): "integer",
    ("SmartChargingCtrlr", "LimitChangeSignificance"): "decimal",
    ("SmartChargingCtrlr", "PeriodsPerSchedule"): "integer",
    ("SmartChargingCtrlr", "ProfileStackLevel"): "integer",
    ("SmartChargingCtrlr", "RateUnit"): "MemberList",
    ("ReservationCtrlr", "Enabled"): "boolean",
    ("SecurityCtrlr", "SecurityProfile"): "integer",
    ("SecurityCtrlr", "AllowCSMSTLSWildcards"): "boolean",
    ("SecurityCtrlr", "CertificateEntries"): "integer",
    ("SecurityCtrlr", "CertSigningWaitMinimum"): "integer",
    ("SecurityCtrlr", "CertSigningRepeatTimes"): "integer",
    # TC_L_13_CS — OCPP 2.0.1 places this variable under ChargingStation.
    ("ChargingStation", "AllowNewSessionsPendingFirmwareUpdate"): "boolean",
}

# OCPP 2.0.1 VariableCharacteristics.valuesList — required for OptionList /
# MemberList / SequenceList variables. Values taken from the OCPP 2.0.1
# Appendix enum tables.
_VAR_VALUES_LIST: Dict[tuple, str] = {
    ("ChargingStation", "AvailabilityState"): "Available,Occupied,Reserved,Unavailable,Faulted",
    ("EVSE", "AvailabilityState"): "Available,Occupied,Reserved,Unavailable,Faulted",
    ("Connector", "AvailabilityState"): "Available,Occupied,Reserved,Unavailable,Faulted",
    ("ClockCtrlr", "TimeSource"): "Heartbeat,NTP,RealTimeClock,MobileNetwork,RadioTimeTransmitter,GPS",
    ("TxCtrlr", "TxStartPoint"): "ParkingBayOccupancy,EVConnected,Authorized,DataSigned,PowerPathClosed,EnergyTransfer",
    ("TxCtrlr", "TxStopPoint"): "ParkingBayOccupancy,EVConnected,Authorized,DataSigned,PowerPathClosed,EnergyTransfer",
    ("SampledDataCtrlr", "TxUpdatedMeasurands"): "Current.Import,Voltage,Energy.Active.Import.Register,Power.Active.Import",
    ("SampledDataCtrlr", "TxStartedMeasurands"): "Current.Import,Voltage,Energy.Active.Import.Register,Power.Active.Import",
    ("SampledDataCtrlr", "TxEndedMeasurands"): "Current.Import,Voltage,Energy.Active.Import.Register,Power.Active.Import",
    ("AlignedDataCtrlr", "Measurands"): "Current.Import,Voltage,Energy.Active.Import.Register,Power.Active.Import",
    ("AlignedDataCtrlr", "TxEndedMeasurands"): "Current.Import,Voltage,Energy.Active.Import.Register,Power.Active.Import",
    ("SmartChargingCtrlr", "RateUnit"): "A,W",
    ("OCPPCommCtrlr", "FileTransferProtocols"): "FTP,FTPS,HTTP,HTTPS",
    ("OCPPCommCtrlr", "NetworkConfigurationPriority"): "0,1,2,3",
}

# Optional maxLimit — required by OCTT for EVSE.Power (spec makes it optional,
# but the template treats maxLimit as required-present).
_VAR_MAX_LIMIT: Dict[tuple, float] = {
    ("EVSE", "Power"): 22000.0,
}

# OCPP 2.0.1 VariableCharacteristics: optional unit string for select variables
# so NotifyReport carries the spec-mandated unit (e.g. seconds, watts).
_VAR_UNITS: Dict[tuple, str] = {
    ("EVSE", "Power"): "W",
    ("OCPPCommCtrlr", "OfflineThreshold"): "s",
    ("OCPPCommCtrlr", "MessageTimeout"): "s",
    ("OCPPCommCtrlr", "MessageAttemptInterval"): "s",
    ("OCPPCommCtrlr", "RetryBackOffWaitMinimum"): "s",
    ("OCPPCommCtrlr", "WebSocketPingInterval"): "s",
    ("TxCtrlr", "EVConnectionTimeOut"): "s",
    ("HeartbeatCtrlr", "HeartbeatInterval"): "s",
    ("AlignedDataCtrlr", "Interval"): "s",
    ("AlignedDataCtrlr", "TxEndedInterval"): "s",
    ("SampledDataCtrlr", "TxUpdatedInterval"): "s",
    ("SampledDataCtrlr", "TxEndedInterval"): "s",
    ("SecurityCtrlr", "CertSigningWaitMinimum"): "s",
    ("AuthCacheCtrlr", "LifeTime"): "s",
}

# Instanced device-model entries (OCPP 2.0.1 VariableType.instance) that can't
# live in the single-key-per-variable dict. Each tuple is (component, variable,
# instance, value, mutability). Emitted alongside device_model in NotifyReport.
_INSTANCED_ENTRIES = [
    ("OCPPCommCtrlr", "MessageTimeout", "Default", "30", "ReadOnly"),
    ("OCPPCommCtrlr", "MessageAttempts", "TransactionEvent", "3", "ReadWrite"),
    ("OCPPCommCtrlr", "MessageAttemptInterval", "TransactionEvent", "30", "ReadWrite"),
    ("DeviceDataCtrlr", "BytesPerMessage", "GetReport", "65000", "ReadOnly"),
    ("DeviceDataCtrlr", "BytesPerMessage", "GetVariables", "65000", "ReadOnly"),
    ("DeviceDataCtrlr", "BytesPerMessage", "SetVariables", "65000", "ReadOnly"),
    ("DeviceDataCtrlr", "ItemsPerMessage", "GetReport", "64", "ReadOnly"),
    ("DeviceDataCtrlr", "ItemsPerMessage", "GetVariables", "64", "ReadOnly"),
    ("DeviceDataCtrlr", "ItemsPerMessage", "SetVariables", "64", "ReadOnly"),
    ("SmartChargingCtrlr", "Entries", "ChargingProfiles", "10", "ReadOnly"),
]


def _value_matches_data_type(value: str, data_type: str) -> bool:
    """Return True iff `value` is a valid textual encoding of `data_type`.

    OCPP 2.0.1 carries every variable as a string; the receiving side is
    responsible for checking that the string parses as the declared type.
    """
    import re
    if data_type == "boolean":
        return value.lower() in ("true", "false")
    if data_type == "integer":
        return bool(re.fullmatch(r"-?\d+", value))
    if data_type == "decimal":
        return bool(re.fullmatch(r"-?\d+(\.\d+)?", value))
    if data_type == "dateTime":
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except Exception:
            return False
    return True  # string / *List — any text permitted


# OCPP 2.0.1 §B02.FR.03 — CSMS-initiated actions the CS must keep accepting
# while BootNotification is still Pending. Anything outside this set gets
# rejected with a CALLERROR SecurityError. In Rejected state nothing is
# accepted (§B03).
_PENDING_ALLOWED_ACTIONS = frozenset({
    "GetBaseReport",
    "GetVariables",
    "SetVariables",
    "TriggerMessage",
    "CertificateSigned",
    "InstallCertificate",
    "DeleteCertificate",
    "GetInstalledCertificateIds",
    "UpdateFirmware",
    "PublishFirmware",
    "UnpublishFirmware",
    "Reset",
    "GetLog",
    # TC_B_02_CS: actions whose response carries a regular status enum
    # (Accepted/Rejected). Per OCPP 2.0.1 §B02 OCTT expects them to receive
    # a normal-shaped response with status "Rejected" while the CS is
    # Pending, NOT an RPC SecurityError. Their handlers already reject when
    # _boot_status != "Accepted", so just let them past the gate.
    "RequestStartTransaction",
    "RequestStopTransaction",
})


class ChargingStationController:
    def __init__(self, ocpp_client: OCPPClient, cert_dir: str = "/etc/cp_sim201/certs", security_profile: int = 0, basic_auth_user: str = "", ca_cert: str = "", serial_number: str = ""):
        self.ocpp_client = ocpp_client
        self.evse_id = 1
        self.connector_id = 1
        # Manufacturing serial number from station_config.json. Used as
        # BootNotification.chargingStation.serialNumber — this is the
        # provisioning identifier the operator uses to register the unit
        # with the CSMS, separate from the OCPP identity (station_id).
        self._serial_number: str = serial_number

        self.connector_hal = ConnectorHAL(self.evse_id, self.connector_id, self.ocpp_client)
        self.token_reader_hal = TokenReaderHAL(self.ocpp_client)
        self.power_contactor_hal = PowerContactorHAL(self.evse_id, self.ocpp_client)

        self.is_authorized: bool = False
        # TC_C_04_CS: remember the idToken that authorized the live tx so a
        # subsequent scan with a DIFFERENT idToken does not stop the
        # transaction (per OCPP 2.0.1 §C01.FR.03).
        self._tx_id_token_value: Optional[str] = None
        # TC_C_39_CS: also remember the groupIdToken that CSMS returned when
        # authorizing the tx. A later scan whose Authorize response carries
        # the same groupIdToken grants stop-authority (OCPP 2.0.1 §C09).
        self._tx_group_id_token_value: Optional[str] = None
        # TC_C_32_CS: Authorization Cache — persists across reboots.
        # key = idToken value; value = {"idTokenInfo": {...}, "stored_at": epoch}
        self._auth_cache: Dict[str, Dict[str, Any]] = load_auth_cache()
        # TC_E_31_CS: remember recently ended tx ids so GetTransactionStatus
        # can answer ongoingIndicator=false but messagesInQueue=true if the
        # offline queue still has events for that txId.
        self._ended_tx_ids: set = set()
        self.transaction_id: str | None = None
        self.meter_value: float = 0.0
        self._state_c_active: bool = False
        # TC_E_45_CS: cp_adc_monitor and proximity_monitor can race on cable
        # plug — ADC typically sees State C within 0.5s while proximity
        # debounces over 1.0s. Without this flag, handle_state_c (from ADC)
        # fires before simulate_cable_plugged emits CablePluggedIn, leaving
        # the cable-plug event queued offline when the ws closes in between.
        # The flag is set only after CablePluggedIn has been sent/queued,
        # guaranteeing the event order CablePluggedIn → ChargingStateChanged
        # regardless of which monitor wakes first.
        self._cable_plug_event_sent: bool = False
        self._tx_seq_no: int = 0

        self._heartbeat_task = None
        self._meter_task = None
        # TC_J_01/J_02/J_03_CS: clock-aligned MeterValues. A single periodic
        # loop that respects AlignedDataCtrlr.Interval (when outside a tx or
        # during one) and AlignedDataCtrlr.TxEndedInterval (snapshot emitted
        # once on tx end — driven from stop_transaction, not the loop).
        self._aligned_data_task: Optional[asyncio.Task] = None
        # TC_E_05_CS: watchdog task for TxCtrlr.EVConnectionTimeOut — starts
        # when the user authorizes before the cable is plugged, fires if the
        # cable doesn't arrive in time and deauthorizes the session.
        self._ev_connect_timeout_task: Optional[asyncio.Task] = None
        self._pending_reset: bool = False
        self._pending_reset_type: str = "Immediate"
        # TC_L_01_CS: after a (secure) firmware install, the CS must
        # disconnect, reboot, reconnect with BootNotification(
        # reason=FirmwareUpdate) and THEN emit the final "Installed"
        # FirmwareStatusNotification. _on_reconnect consumes these flags.
        self._pending_firmware_update_reboot: bool = False
        self._pending_firmware_update_request_id: Optional[int] = None
        # TC_L_11_CS: track an in-flight UpdateFirmwareRequest so a second
        # request arriving while the first is still downloading/installing
        # can be refused (simulator cannot truly cancel). Cleared when the
        # sequence finishes or fails.
        self._firmware_update_task: Optional[asyncio.Task] = None
        self._firmware_update_in_progress: bool = False
        # TC_L_13_CS: remember connectors we forced to Unavailable for a
        # firmware update so we can restore them to Available after reboot.
        self._firmware_update_suspended_connectors: bool = False
        # TC_L_13_CS: on a firmware-update reboot the CSMS expects the first
        # availability notification to report Available even if the cable is
        # still physically plugged. One-shot — cleared after the next call
        # to _send_availability_status_notification.
        self._force_available_once: bool = False
        # TC_J_03_CS: AlignedDataCtrlr.TxEndedInterval / TxEndedMeasurands —
        # clock-aligned samples are accumulated during the tx and flushed
        # into the Ended event's meterValue array with context=Sample.Clock.
        # Kept in sync with the active tx; cleared on tx end / cable unplug.
        self._tx_ended_aligned_samples: List[Dict[str, Any]] = []
        self._tx_ended_aligned_task: Optional[asyncio.Task] = None
        # TC_J_10_CS: SampledDataCtrlr.TxEndedInterval / TxEndedMeasurands —
        # periodic (not clock-aligned) samples accumulated during the tx,
        # flushed into Ended with context=Sample.Periodic.
        self._tx_ended_sampled_samples: List[Dict[str, Any]] = []
        self._tx_ended_sampled_task: Optional[asyncio.Task] = None
        # TC_G_11/14/17_CS: ChangeAvailability(Inoperative) mid-tx returns
        # "Scheduled"; the switch must take effect once the tx ends. We
        # remember the requested scope so the deferred apply can emit the
        # right StatusNotification (Unavailable) after the cable is
        # unplugged.
        self._pending_inoperative: bool = False
        # Whether the Reset was answered with "Scheduled" (only true when
        # OnIdle deferred the reboot due to an active transaction). Drives
        # the BootNotification reason: ScheduledReset vs RemoteReset.
        self._pending_reset_scheduled: bool = False
        # TC_B_20/TC_B_21_CS: only switch live ws_kwargs on Reset when a
        # SetNetworkProfile has actually armed a profile switch since last boot.
        # Without this, a plain Reset would re-apply whatever slot the
        # persisted NetworkConfigurationPriority points at — potentially
        # stripping credentials that match our current config.
        self._pending_network_profile_switch: bool = False
        self._first_connect: bool = True
        # BootNotification 응답 상태 ("Accepted" | "Pending" | "Rejected" | "Unknown")
        # TC_B_02_CS: Pending 상태에서 트랜잭션·원격시작 요청을 거부해야 함.
        self._boot_status: str = "Unknown"
        # TC_B_03_CS: Rejected/Pending 응답 시 interval 후 재시도 태스크
        self._boot_retry_task: Optional[asyncio.Task] = None

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

        # Block P (TC_P_01_CS): supported DataTransfer vendors.
        # key = vendorId; value = set of allowed messageIds (empty set = any
        # messageId allowed for that vendor). An unknown vendorId produces
        # UnknownVendorId; a known vendorId with an unknown messageId produces
        # UnknownMessageId. This CS exposes no vendor-specific features.
        self._supported_data_transfer_vendors: Dict[str, set] = {}

        # Block A: 인증서 관리
        self._cert_dir: str = cert_dir
        self._basic_auth_user: str = basic_auth_user
        self._ca_cert: str = ca_cert
        # ChargingStationCertificate가 서명된 시점의 CSMS URL을 기억한다.
        # TC_A_21_CS: Profile 3 전환 시 target slot의 URL이 이 값과 다르면 Rejected.
        self._cert_valid_for_url: Optional[str] = load_cert_metadata().get("valid_for_url")
        # key: serialNumber hex string
        # value: {"certificateType": str, "certificateHashData": dict, "pem_path": str}
        # TC_M_23_CS: persist across service restarts.
        self.installed_certificates: Dict[str, Dict] = load_installed_certificates()
        # TC_M_23_CS: auto-register the station_config CA cert as a
        # CSMSRootCertificate on startup. Without it, GetInstalledCertificateIds
        # returns NotFound and the test ERRORs because OCTT has no target to
        # try DeleteCertificate against.
        if self._ca_cert and os.path.exists(self._ca_cert):
            try:
                with open(self._ca_cert, "r", encoding="utf-8") as f:
                    ca_pem = f.read()
                hash_data = self._make_cert_hash_data(ca_pem)
                serial = hash_data["serialNumber"]
                if serial not in self.installed_certificates:
                    self.installed_certificates[serial] = {
                        "certificateType": "CSMSRootCertificate",
                        "certificateHashData": hash_data,
                        "pem_path": self._ca_cert,
                    }
                    save_installed_certificates(self.installed_certificates)
                    logger.info(
                        f"Auto-registered CSMSRootCertificate from {self._ca_cert} "
                        f"(serial={serial})"
                    )
            except Exception as e:
                logger.warning(f"Failed to auto-register CA cert: {e}")
        # CertificateSigned로 수신한 클라이언트 인증서 경로 — 다음 재시작 시 적용
        self._pending_client_cert: Optional[str] = None
        # TC_A_23_CS: SignCertificate → CertificateSigned 대기를 위한 이벤트
        self._cert_signed_event: Optional[asyncio.Event] = None
        self._cert_signing_task: Optional[asyncio.Task] = None

        # Block B: 장치 모델 (component → variable → (value, mutability))
        self.device_model = load_device_model({
            "ChargingStation": {
                "Model":             ("AC_SIMULATOR_201", "ReadOnly"),
                "VendorName":        ("TEST_CORP",        "ReadOnly"),
                "FirmwareVersion":   ("1.0.0",            "ReadWrite"),
                "SerialNumber":      ("SN-001",           "ReadOnly"),
                "AvailabilityState": ("Available",        "ReadOnly"),
                "Available":         ("true",             "ReadOnly"),
                "SupplyPhases":      ("3",                "ReadOnly"),
                # TC_L_13_CS / §L01.FR.06-07: when false, the CS must set all
                # Available connectors to Unavailable for the duration of a
                # pending firmware update and refuse new sessions.
                "AllowNewSessionsPendingFirmwareUpdate": ("true", "ReadWrite"),
            },
            "EVSE": {
                "AvailabilityState": ("Available", "ReadOnly"),
                "Available":         ("true",      "ReadOnly"),
                "Power":             ("7400",      "ReadOnly"),
                "SupplyPhases":      ("3",         "ReadOnly"),
            },
            "Connector": {
                "AvailabilityState": ("Available", "ReadOnly"),
                "Available":         ("true",      "ReadOnly"),
                "ConnectorType":     ("cType2",    "ReadOnly"),
                "SupplyPhases":      ("3",         "ReadOnly"),
            },
            "TokenReader": {
                "Enabled": ("true", "ReadWrite"),
            },
            "ClockCtrlr": {
                "DateTime":   (datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "ReadOnly"),
                "TimeSource": ("Heartbeat", "ReadWrite"),
            },
            "DeviceDataCtrlr": {
                # Per-RPC-type instances live in _INSTANCED_ENTRIES; this
                # component entry exists so the report has the parent node.
            },
            # CSMS 설정 가능 파라미터 — names are the canonical OCPP 2.0.1 ones
            # (no SampledData*/AlignedData* prefixes; those were from earlier drafts).
            "SampledDataCtrlr": {
                "TxUpdatedInterval":   ("60",    "ReadWrite"),
                "TxEndedInterval":     ("0",     "ReadWrite"),
                "TxUpdatedMeasurands": ("Current.Import,Voltage,Energy.Active.Import.Register", "ReadWrite"),
                "TxStartedMeasurands": ("Energy.Active.Import.Register", "ReadWrite"),
                "TxEndedMeasurands":   ("Energy.Active.Import.Register", "ReadWrite"),
            },
            "AlignedDataCtrlr": {
                "Interval":          ("0",                              "ReadWrite"),
                "TxEndedInterval":   ("0",                              "ReadWrite"),
                "Measurands":        (
                    "Current.Import,Voltage,Energy.Active.Import.Register,Power.Active.Import",
                    "ReadWrite",
                ),
                "TxEndedMeasurands": (
                    "Current.Import,Voltage,Energy.Active.Import.Register,Power.Active.Import",
                    "ReadWrite",
                ),
            },
            "HeartbeatCtrlr": {
                "HeartbeatInterval": ("60", "ReadWrite"),
            },
            "TxCtrlr": {
                "TxStartPoint":             ("Authorized,EVConnected", "ReadWrite"),
                "TxStopPoint":              ("Authorized,EVConnected", "ReadWrite"),
                "StopTxOnEVSideDisconnect": ("true", "ReadWrite"),
                "StopTxOnInvalidId":        ("true", "ReadWrite"),
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
                "RetryBackOffWaitMinimum":          ("2",    "ReadWrite"),
                "RetryBackOffRepeatTimes":          ("10",   "ReadWrite"),
                "RetryBackOffRandomRange":          ("3",    "ReadWrite"),
                "ResetRetries":                     ("3",    "ReadWrite"),
                "UnlockOnEVSideDisconnect":         ("true", "ReadWrite"),
                "WebSocketPingInterval":            ("0",    "ReadWrite"),
                "FileTransferProtocols":            ("HTTP,HTTPS", "ReadOnly"),
            },
            "LocalAuthListCtrlr": {
                "Enabled":        ("true", "ReadWrite"),
                "Entries":        ("100",  "ReadOnly"),
                "BytesPerMessage":("65000","ReadOnly"),
                "ItemsPerMessage":("20",   "ReadOnly"),
            },
            # TC_C_32_CS: Authorization Cache — skips re-Authorize for tokens
            # the CSMS already accepted. Persists across reboots.
            "AuthCacheCtrlr": {
                "Enabled":              ("true",  "ReadWrite"),
                "LifeTime":             ("86400", "ReadWrite"),
                "DisablePostAuthorize": ("false", "ReadWrite"),
            },
            "SmartChargingCtrlr": {
                "Enabled":                 ("false", "ReadWrite"),
                "LimitChangeSignificance": ("1.0",   "ReadWrite"),
                "PeriodsPerSchedule":      ("10",    "ReadOnly"),
                "ProfileStackLevel":       ("10",    "ReadOnly"),
                "RateUnit":                ("A,W",   "ReadOnly"),
            },
            "ReservationCtrlr": {
                "Enabled": ("true", "ReadWrite"),
            },
            "SecurityCtrlr": {
                "SecurityProfile":        (str(security_profile), "ReadOnly"),
                "AllowCSMSTLSWildcards":  ("false", "ReadWrite"),
                "OrganizationName":       ("TEST_CORP", "ReadWrite"),
                "CertificateEntries":     ("2",   "ReadOnly"),
                "BasicAuthPassword":      ("",    "WriteOnly"),
                # TC_A_23_CS: SignCertificate → CertificateSigned 대기/재시도 정책
                "CertSigningWaitMinimum": ("30", "ReadWrite"),
                "CertSigningRepeatTimes": ("3",  "ReadWrite"),
            },
        })
        # Force-override SecurityProfile after load_device_model so persisted "0" can't win.
        # Mutability stays ReadOnly per OCPP 2.0.1 — SecurityProfile is changed via
        # the SetNetworkProfile + Reset flow, not directly via SetVariables.
        self.device_model["SecurityCtrlr"]["SecurityProfile"] = (str(security_profile), "ReadOnly")

        # TC_B_23_CS: restore admin Inoperative state across reboots. OCPP 2.0.1
        # defines EVSE.AvailabilityState as ReadOnly (so device_model.json
        # doesn't persist it); admin_state.json carries this flag.
        _admin = load_admin_state()
        if not bool(_admin.get("is_evse_available", True)):
            self.is_evse_available = False
            self.device_model["EVSE"]["AvailabilityState"] = ("Inoperative", "ReadOnly")

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
        # TC_B_30_CS: while BootNotification is Pending/Rejected, non-allowed
        # CSMS-initiated actions must be answered with CALLERROR SecurityError.
        self.ocpp_client.set_message_gate(self._boot_state_message_gate)
        # TC_B_51_CS: reconnect backoff must honour the OCPPCommCtrlr variables
        # the CSMS sets at runtime (e.g. RetryBackOffWaitMinimum=64).
        self.ocpp_client.set_retry_config_provider(self._retry_backoff_config)
        # TC_E_41/E_42/E_50/E_51_CS: TransactionEvent retry uses §E13 schedule
        # driven by MessageAttempts / MessageAttemptInterval (instance
        # "TransactionEvent"). Expose via _tx_retry_config so ocpp_client can
        # read current values at each call.
        if hasattr(self.ocpp_client, "set_tx_retry_config_provider"):
            self.ocpp_client.set_tx_retry_config_provider(self._tx_retry_config)
        # TC_B_46_CS: after NetworkProfileConnectionAttempts failed attempts on
        # the current slot, fall back to the next slot in
        # NetworkConfigurationPriority.
        self.ocpp_client.set_connection_failure_handler(
            self._on_connection_failure
        )
        # TC_C_16_CS: when offline-queued TransactionEvents are replayed on
        # reconnect, feed the response back through the same cache/deauth
        # handler a live call uses. Without this, an Invalid idTokenInfo on
        # a replayed Updated(Authorized) would be silently dropped instead of
        # triggering a stop with triggerReason=Deauthorized.
        if hasattr(self.ocpp_client, "set_replay_response_hook"):
            self.ocpp_client.set_replay_response_hook(self._on_replay_response)
        # Snapshot the boot-time connection so we can fall back to it when
        # the priority list ends on a slot (typically "0") that was never
        # persisted to network_profiles.json. Guarded for unit tests that
        # pass a mocked OCPPClient.
        self._initial_server_url: str = getattr(self.ocpp_client, "server_url", "") or ""
        _initial_kwargs = getattr(self.ocpp_client, "_ws_kwargs", None)
        self._initial_ws_kwargs: Dict[str, Any] = dict(_initial_kwargs) if isinstance(_initial_kwargs, dict) else {}
        self._initial_slot: str = self._get_param(
            "OCPPCommCtrlr", "ActiveNetworkProfile", "0"
        ).strip()

    # ------------------------------------------------------------------
    # Boot-state SecurityError gate (OCPP 2.0.1 §B02/B03)
    # ------------------------------------------------------------------

    def _on_connection_failure(self, consecutive_failures: int) -> None:
        """TC_B_46_CS: fall back to the next priority slot after N failures.

        Reads OCPPCommCtrlr.NetworkProfileConnectionAttempts; when the current
        slot has failed that many times in a row, advances ActiveNetworkProfile
        to the next entry in NetworkConfigurationPriority and rebuilds
        ws_kwargs from that slot's stored connectionData. update_connection()
        resets the client's failure counter so the new slot gets a fresh
        budget.
        """
        limit = self._get_int("OCPPCommCtrlr", "NetworkProfileConnectionAttempts", 3)
        if consecutive_failures < limit:
            return
        priority_str = self._get_param("OCPPCommCtrlr", "NetworkConfigurationPriority", "")
        slots = [s.strip() for s in priority_str.split(",") if s.strip()]
        if len(slots) <= 1:
            return
        current_slot = self._get_param("OCPPCommCtrlr", "ActiveNetworkProfile", "").strip()
        try:
            idx = slots.index(current_slot)
        except ValueError:
            idx = 0
        next_idx = idx + 1
        if next_idx >= len(slots):
            logger.warning(
                f"All {len(slots)} priority slots exhausted; continuing to retry current"
            )
            return
        next_slot = slots[next_idx]
        logger.info(
            f"Fallback: slot {current_slot} failed {consecutive_failures}x "
            f"(>= {limit}) — switching to slot {next_slot}"
        )
        profiles = load_network_profiles()
        profile = profiles.get(next_slot)
        if not profile:
            # TC_B_46_CS: slot 0 (or whichever slot the station booted on)
            # isn't in network_profiles.json — use the snapshot of the
            # original boot-time connection instead.
            if next_slot == self._initial_slot:
                logger.info(
                    f"Falling back to initial boot slot {next_slot} "
                    f"({self._initial_server_url})"
                )
                self.ocpp_client.update_connection(
                    self._initial_server_url.rstrip("/"),
                    dict(self._initial_ws_kwargs),
                )
                self.device_model["OCPPCommCtrlr"]["ActiveNetworkProfile"] = (
                    next_slot, "ReadOnly"
                )
                save_device_model(self.device_model)
                return
            logger.warning(f"No stored profile for fallback slot {next_slot}; keeping current")
            return
        new_url = profile.get("ocppCsmsUrl", "")
        if not new_url:
            logger.warning(f"Fallback slot {next_slot} missing ocppCsmsUrl; keeping current")
            return
        ws_kwargs = StationConfig.build_ws_kwargs_from_profile(
            profile, self._cert_dir, self._ca_cert
        )
        new_sp_int = int(profile.get("securityProfile", 0))
        if (
            new_sp_int in (1, 2)
            and "additional_headers" not in ws_kwargs
            and "additional_headers" in self.ocpp_client._ws_kwargs
        ):
            ws_kwargs["additional_headers"] = self.ocpp_client._ws_kwargs["additional_headers"]
        self.ocpp_client.update_connection(new_url, ws_kwargs)
        self.device_model["SecurityCtrlr"]["SecurityProfile"] = (str(new_sp_int), "ReadOnly")
        self.device_model["OCPPCommCtrlr"]["ActiveNetworkProfile"] = (next_slot, "ReadOnly")
        save_device_model(self.device_model)

    def _retry_backoff_config(self):
        """Return (wait_min_s, random_range_s, repeat_times) from device model.

        Falls back to the static OCPPConfig values when a variable is missing or
        unparseable. Called by OCPPClient before each reconnect sleep.
        """
        from .config import OCPPConfig as _Cfg
        wait_min = self._get_int(
            "OCPPCommCtrlr", "RetryBackOffWaitMinimum", _Cfg.RETRY_BACKOFF_WAIT_MINIMUM
        )
        random_range = self._get_int(
            "OCPPCommCtrlr", "RetryBackOffRandomRange", _Cfg.RETRY_BACKOFF_RANDOM_RANGE
        )
        repeat_times = self._get_int(
            "OCPPCommCtrlr", "RetryBackOffRepeatTimes", _Cfg.RETRY_BACKOFF_REPEAT_TIMES
        )
        return wait_min, random_range, repeat_times

    def _tx_retry_config(self):
        """TC_E_41_CS: return (MessageAttempts, MessageAttemptInterval,
        MessageTimeout) for TransactionEvent.

        Reads MessageAttempts and MessageAttemptInterval from
        _INSTANCED_ENTRIES (instance="TransactionEvent") and MessageTimeout
        from the same store (instance="Default"). OCTT sets these values up
        in test prep and the §E13 retry schedule uses them directly —
        interval * n + MessageTimeout between successive transmissions.
        """
        attempts = 3
        interval = 60
        timeout = 30
        for comp, var, inst, value, _ in _INSTANCED_ENTRIES:
            if comp != "OCPPCommCtrlr":
                continue
            try:
                if inst == "TransactionEvent" and var == "MessageAttempts":
                    attempts = int(value)
                elif inst == "TransactionEvent" and var == "MessageAttemptInterval":
                    interval = int(value)
                elif inst == "Default" and var == "MessageTimeout":
                    timeout = int(value)
            except (TypeError, ValueError):
                pass
        return attempts, interval, timeout

    def _boot_state_message_gate(self, action: str):
        """Return (error_code, desc) to reject the incoming CSMS action, or None to allow.

        §B02.FR.03: In Pending state, only a defined subset is accepted; every
        other action must be rejected with SecurityError.
        §B03.FR.04: In Rejected state, every CSMS-initiated action is rejected
        with SecurityError until BootNotification is finally Accepted.
        """
        if self._boot_status == "Pending" and action not in _PENDING_ALLOWED_ACTIONS:
            return (
                "SecurityError",
                f"Action '{action}' not allowed while BootNotification is Pending",
            )
        if self._boot_status == "Rejected":
            return (
                "SecurityError",
                f"Action '{action}' not allowed while BootNotification is Rejected",
            )
        return None

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

    def _validate_variable_value(self, component: str, variable: str, value: str) -> Optional[str]:
        """Returns a rejection attributeStatus string if value is invalid, else None."""
        # TC_B_11_CS: reject values that don't parse as the variable's DataType.
        data_type = _VAR_DATA_TYPES.get((component, variable))
        if data_type and not _value_matches_data_type(value, data_type):
            logger.warning(
                f"SetVariables rejected: {component}.{variable}={value!r} "
                f"is not a valid {data_type}"
            )
            return "Rejected"

        if component == "SecurityCtrlr" and variable == "BasicAuthPassword":
            if not (16 <= len(value) <= 40):
                return "Rejected"

        if component == "OCPPCommCtrlr" and variable == "NetworkConfigurationPriority":
            # TC_A_21_CS: 활성 slot이 Profile 3를 요구하는데 현재 client cert가
            # 해당 CSMS root 하에서 서명된 것이 아니면 Rejected.
            # TC_B_45_CS ("Same CSMS Root"): 같은 hostname이면 port가 달라도
            # 기존 cert가 그대로 유효 — TLS server cert는 hostname-SAN 기반이라
            # URL 전체 매칭이 아닌 hostname 매칭으로 비교한다.
            slots = [s.strip() for s in value.split(",") if s.strip()]
            if slots:
                profiles = load_network_profiles()
                active = profiles.get(slots[0])
                if active and int(active.get("securityProfile", 0)) == 3:
                    from urllib.parse import urlparse
                    target_url = (active.get("ocppCsmsUrl") or "").rstrip("/")
                    saved_url = (self._cert_valid_for_url or "").rstrip("/")
                    target_host = urlparse(target_url).hostname or ""
                    saved_host = urlparse(saved_url).hostname or ""
                    if not saved_host or saved_host != target_host:
                        logger.warning(
                            f"NetworkConfigurationPriority rejected: active slot requires "
                            f"Profile 3 at {target_url!r}, but no valid ChargingStationCertificate "
                            f"(cert valid for {saved_url!r})"
                        )
                        return "Rejected"
        return None

    def _apply_variable_change(self, component: str, variable: str, value: str) -> None:
        """SetVariables 수신 후 즉시 동작에 반영이 필요한 파라미터를 처리한다."""
        if component == "HeartbeatCtrlr" and variable == "HeartbeatInterval":
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                logger.info(f"HeartbeatInterval changed to {value}s — task restarted")

        elif component == "SecurityCtrlr" and variable == "BasicAuthPassword":
            if self._basic_auth_user and value:
                import base64
                credentials = base64.b64encode(
                    f"{self._basic_auth_user}:{value}".encode()
                ).decode()
                self.ocpp_client._ws_kwargs["additional_headers"] = {
                    "Authorization": f"Basic {credentials}"
                }
                logger.info("BasicAuthPassword updated — scheduling reconnect with new credentials")
                asyncio.create_task(self._reconnect_after_password_change())

    async def _send_sign_certificate_with_retry(self, csr_pem: str) -> None:
        """SignCertificate 전송 후 CertificateSigned가 도착할 때까지 대기한다.

        CertSigningWaitMinimum 초 내 응답이 없으면 SignCertificate를 재전송한다.
        최대 CertSigningRepeatTimes 회 재시도 (초기 전송 포함 총 1 + RepeatTimes 회).
        [OCPP 2.0.1 TC_A_23_CS / Part 2 §A04]
        """
        wait_seconds = self._get_int("SecurityCtrlr", "CertSigningWaitMinimum", 30)
        max_retries = self._get_int("SecurityCtrlr", "CertSigningRepeatTimes", 3)
        total_attempts = max_retries + 1

        for attempt in range(1, total_attempts + 1):
            self._cert_signed_event = asyncio.Event()
            try:
                sign_res = await self.ocpp_client.call("SignCertificate", {
                    "csr": csr_pem,
                    "certificateType": "ChargingStationCertificate",
                })
            except Exception as e:
                logger.error(f"SignCertificate call failed on attempt {attempt}: {e}")
                sign_res = None

            # TC_A_15_CS: if the CSMS rejects the SignCertificateRequest, do
            # not wait for a CertificateSignedRequest that will never arrive
            # and do not retry — stop the loop immediately.
            if sign_res is not None and sign_res.get("status") == "Rejected":
                logger.info(
                    "SignCertificateResponse=Rejected — aborting SignCertificate retry loop"
                )
                self._cert_signed_event = None
                return

            # OCPP 2.0.1 §A04: N번째 시도의 타임아웃은 N × CertSigningWaitMinimum
            # (attempt 1: wait, attempt 2: 2×wait, ...)
            current_wait = wait_seconds * attempt
            try:
                await asyncio.wait_for(self._cert_signed_event.wait(), timeout=current_wait)
                logger.info(
                    f"CertificateSigned received on attempt {attempt}/{total_attempts}"
                )
                self._cert_signed_event = None
                return
            except asyncio.TimeoutError:
                if attempt < total_attempts:
                    logger.warning(
                        f"CertificateSigned timeout after {current_wait}s "
                        f"(attempt {attempt}/{total_attempts}) — retrying SignCertificate"
                    )
                else:
                    logger.warning(
                        f"CertificateSigned: {total_attempts} attempts exhausted, giving up"
                    )

        self._cert_signed_event = None

    async def _generate_csr_pem(self) -> str:
        """Generate a 2048-bit RSA CSR; save the private key for later CertificateSigned use."""
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: rsa.generate_private_key(public_exponent=65537, key_size=2048)
        )
        os.makedirs(self._cert_dir, exist_ok=True)
        key_path = os.path.join(self._cert_dir, "client.key")
        with open(key_path, "wb") as f:
            f.write(key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption(),
            ))
        csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, self.ocpp_client.station_id),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME,
                                   self._get_param("SecurityCtrlr", "OrganizationName", "TEST_CORP")),
            ]))
            .sign(key, hashes.SHA256())
        )
        logger.info(f"CSR generated, private key saved to {key_path}")
        return csr.public_bytes(serialization.Encoding.PEM).decode()

    async def _reconnect_after_password_change(self) -> None:
        """Close the current connection so the reconnect loop picks up the new BasicAuth header."""
        await asyncio.sleep(0.5)  # Let SetVariables response be sent first
        logger.info("Closing connection to reconnect with updated BasicAuth password")
        if self.ocpp_client.ws:
            await self.ocpp_client.ws.close()

    # ------------------------------------------------------------------
    # 부트 / 하트비트
    # ------------------------------------------------------------------

    async def boot_routine(self, reason: str = "PowerUp") -> None:
        logger.info(f"Executing Boot Routine (reason={reason})")
        firmware_version = self._get_param("ChargingStation", "FirmwareVersion", "1.0.0")
        # Manufacturing serial number from station_config.json (set during
        # provisioning). Falls back to station_id if config didn't provide
        # one, which keeps the OCTT TC_A_07_CS cert-CN-matches-serialNumber
        # check passing for setups that don't yet provision a serial.
        serial_number = self._serial_number or self.ocpp_client.station_id
        payload = {
            "reason": reason,
            "chargingStation": {
                "model": "AC_SIMULATOR_201",
                "vendorName": "TEST_CORP",
                "serialNumber": serial_number,
                "firmwareVersion": firmware_version,
            }
        }
        res = await self.ocpp_client.call("BootNotification", payload)
        status = (res or {}).get("status", "Unknown")
        self._boot_status = status
        if status == "Accepted":
            logger.info("BootNotification Accepted.")
            # TC_B_03_CS: Accepted 받으면 예약된 재시도 취소
            if self._boot_retry_task and not self._boot_retry_task.done():
                self._boot_retry_task.cancel()
            await self._send_availability_status_notification()

            interval = res.get("interval", 300)
            self.device_model["HeartbeatCtrlr"]["HeartbeatInterval"] = (str(interval), "ReadWrite")
            save_device_model(self.device_model)
            if not self._heartbeat_task or self._heartbeat_task.done():
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            # TC_J_01/J_02_CS: start the clock-aligned data loop after boot.
            if (
                not self._aligned_data_task
                or self._aligned_data_task.done()
            ):
                self._aligned_data_task = asyncio.create_task(
                    self._aligned_data_loop()
                )
        else:
            logger.warning(f"BootNotification Not Accepted (status={status}).")
            # TC_B_03_CS / §B03: Rejected → interval 초 후 재전송.
            # Pending도 동일한 정책 (§B11, 단 TriggerMessage가 먼저 오면 그쪽이 우선).
            interval = int((res or {}).get("interval", 60)) or 60
            if self._boot_retry_task and not self._boot_retry_task.done():
                self._boot_retry_task.cancel()
            self._boot_retry_task = asyncio.create_task(
                self._retry_boot_after(interval, reason)
            )

    async def _retry_boot_after(self, interval: int, reason: str) -> None:
        """interval 초 뒤 BootNotification 재전송. 도중에 Accepted 되면 취소된다."""
        try:
            logger.info(f"Scheduling BootNotification retry in {interval}s")
            await asyncio.sleep(interval)
            if self._boot_status != "Accepted":
                await self.boot_routine(reason=reason)
        except asyncio.CancelledError:
            pass

    async def _on_reconnect(self) -> None:
        """연결 성립 시 호출. 최초 부팅 또는 Reset 후에만 BootNotification 전송."""
        try:
            if self._pending_firmware_update_reboot:
                # TC_L_01_CS: the CS just rebooted after a secure firmware
                # install. Send BootNotification(reason=FirmwareUpdate),
                # then the final FirmwareStatusNotification(Installed),
                # then SecurityEventNotification(type=FirmwareUpdated)
                # per TC_L_01 Step 20.
                fw_request_id = self._pending_firmware_update_request_id
                self._pending_firmware_update_reboot = False
                self._pending_firmware_update_request_id = None
                self._first_connect = False
                # TC_L_13_CS: after a firmware-update reboot the CSMS expects
                # the connector to come back up as Available even if the
                # cable is still physically plugged (tx ended via
                # StopAuthorized with chargingState=EVConnected). Force the
                # first post-boot StatusNotification to report Available.
                self.connector_hal.status = "Available"
                self._cable_plug_event_sent = False
                self._force_available_once = True
                await self.boot_routine(reason="FirmwareUpdate")
                if fw_request_id is not None:
                    try:
                        await self.ocpp_client.call(
                            "FirmwareStatusNotification",
                            {"status": "Installed", "requestId": fw_request_id},
                        )
                        logger.info(
                            "FirmwareStatusNotification: Installed (post-boot)"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to send post-boot Installed: {e}"
                        )
                await self._send_security_event_notification("FirmwareUpdated")
                # TC_L_02/03/13_CS Step 18/21: connector returns to Available
                # after the (simulated) firmware reboot.
                await self._restore_connectors_after_firmware()
            elif self._pending_reset:
                # OCPP 2.0.1 BootReasonEnumType: only use ScheduledReset when
                # the ResetResponse was actually "Scheduled" (OnIdle +
                # active tx, TC_B_21). An OnIdle with no active tx is
                # answered "Accepted" and reboots like a RemoteReset
                # (TC_B_23).
                boot_reason = (
                    "ScheduledReset" if self._pending_reset_scheduled
                    else "RemoteReset"
                )
                self._pending_reset = False
                self._pending_reset_scheduled = False
                self._first_connect = False
                # [OCPP 2.0.1 TC_A_19_CS / Part 2 §A08] 보안 프로파일 업그레이드 후
                # priority에서 하위 보안 slot을 제거해 downgrade 차단.
                # Run this BEFORE boot_routine so an in-flight
                # GetVariables(NetworkConfigurationPriority) from the CSMS
                # never observes the pre-prune value.
                self._prune_network_priority_after_upgrade()
                await self.boot_routine(reason=boot_reason)
                # TC_B_21_CS step 11: after a post-Reset boot, the CS must
                # send a SecurityEventNotificationRequest of type
                # StartupOfTheDevice or ResetOrReboot.
                await self._send_security_event_notification("ResetOrReboot")
            elif self._first_connect:
                self._first_connect = False
                await self.boot_routine(reason="PowerUp")
            else:
                # 단순 연결 재연결(connection drop) — BootNotification 불필요, StatusNotification 전송
                cert_error = self.ocpp_client.tls_cert_error_occurred
                self.ocpp_client.tls_cert_error_occurred = False
                logger.info(
                    f"Reconnected after connection drop "
                    f"(cert_error={cert_error}), sending StatusNotification."
                )
                await self._send_availability_status_notification()
                if cert_error:
                    await self._send_security_event_notification("InvalidCsmsCertificate")
        except Exception as e:
            logger.warning(f"_on_reconnect: post-connect notification failed: {e}")

    async def _send_security_event_notification(self, event_type: str, tech_info: str = "") -> None:
        from datetime import datetime, timezone
        payload: Dict[str, Any] = {
            "type": event_type,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if tech_info:
            payload["techInfo"] = tech_info
        try:
            await self.ocpp_client.call("SecurityEventNotification", payload)
            logger.info(f"SecurityEventNotification sent: {event_type}")
        except Exception as e:
            logger.error(f"Failed to send SecurityEventNotification: {e}", exc_info=True)

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
        """TC_B_08_CS / TC_B_21_CS: Handles incoming ResetRequest from CSMS.

        - Immediate: Accept and reboot right away.
        - OnIdle with no active transaction: Accept and reboot right away.
        - OnIdle with an active transaction: respond "Scheduled"; the reboot is
          deferred until the transaction ends (per OCPP 2.0.1 ResetStatusEnumType).

        재연결 직후 `_on_reconnect`가 `_pending_reset` 플래그를 소비해
        `BootNotification(reason=RemoteReset)`을 전송한다.
        """
        reset_type = payload.get("type", "Immediate")
        logger.info(f"Received ResetRequest: {reset_type}")
        # TC_B_28_CS: we do not support per-EVSE reset — only whole-station.
        # Per OCPP 2.0.1 B11.FR.09 / B12.FR.09, ResetRequest with an evseId
        # must be answered Rejected.
        if "evseId" in payload:
            logger.warning(
                f"Reset rejected: per-EVSE reset (evseId={payload['evseId']}) not supported"
            )
            return {"status": "Rejected"}
        self._pending_reset = True
        self._pending_reset_type = reset_type
        self._pending_reset_scheduled = False
        if reset_type == "OnIdle" and self.transaction_id:
            # _execute_reset will be invoked from stop_transaction() once the
            # ongoing transaction ends; don't launch the reboot task now.
            logger.info(
                f"Reset(OnIdle) scheduled; active tx {self.transaction_id} must end first"
            )
            self._pending_reset_scheduled = True
            return {"status": "Scheduled"}
        # TC_A_06_CS: arm the skip-backoff flag *before* responding Accepted —
        # OCTT may close the WS itself within ~400ms of receiving the
        # response, which races against the 0.5s sleep in _execute_reset.
        # TC_B_50_CS: also apply any pending network-profile switch here (not
        # only inside _execute_reset) so the OCTT-initiated reconnect uses
        # the new URL/ws_kwargs instead of the stale slot.
        self.ocpp_client._skip_next_reconnect_wait = True
        try:
            await self._apply_active_network_profile()
        except Exception as e:
            logger.error(f"Failed to apply active network profile on reset: {e}")
        asyncio.create_task(self._execute_reset(reset_type))
        return {"status": "Accepted"}

    def _try_execute_deferred_reset(self) -> None:
        """Fire the pending Reset if the station has reached its expected idle state.

        TC_B_21_CS: for Reset(OnIdle) issued during an active transaction,
        "idle" means BOTH no active transaction AND the cable is unplugged.
        Immediate resets are fired directly from handle_reset_request and
        must not be re-scheduled here.
        """
        if not self._pending_reset:
            return
        if self._pending_reset_type != "OnIdle":
            return
        if self.transaction_id or self.connector_hal.status != "Available":
            return
        logger.info(
            f"Station idle (tx={self.transaction_id}, connector={self.connector_hal.status}) — "
            f"running deferred Reset({self._pending_reset_type})"
        )
        asyncio.create_task(self._execute_reset(self._pending_reset_type))

    async def _execute_reset(self, reset_type: str) -> None:
        """Reset의 실제 동작: 활성 프로파일 적용 + WebSocket 종료로 재연결 유도."""
        await asyncio.sleep(0.5)  # CallResult 전송이 끝나도록 유예
        if reset_type == "OnIdle" and self.transaction_id:
            logger.info(
                f"Reset(OnIdle) deferred: active transaction {self.transaction_id}"
            )
            # 범위 밖: 트랜잭션 종료 후 재시도 — 현재 OCTT 테스트에서 미발생
            return
        # TC_B_22_CS: Reset(Immediate) must end any active transaction before
        # closing the WebSocket; trigger/stopped reason = ImmediateReset.
        if reset_type == "Immediate" and self.transaction_id:
            await self.stop_transaction("ImmediateReset")
        try:
            await self._apply_active_network_profile()
        except Exception as e:
            logger.error(f"Failed to apply active network profile on reset: {e}")
        logger.info("Reset: closing WebSocket for reconnection")
        # TC_A_06_CS / TC_B_57_CS: arm the skip-backoff flag *only* when we
        # actually own the close. If OCTT already closed the WS earlier
        # (handle_reset_request races with OCTT's fast disconnect),
        # ws is None here and our close is a no-op — setting the flag in
        # that case would leak True into the *next* disconnect and skip
        # the spec-required RetryBackOffWaitMinimum.
        if self.ocpp_client.ws:
            self.ocpp_client._skip_next_reconnect_wait = True
            try:
                await self.ocpp_client.ws.close()
            except Exception as e:
                logger.warning(f"ws.close() raised: {e}")

    def _prune_network_priority_after_upgrade(self) -> None:
        """활성 slot의 보안 레벨보다 낮거나 알 수 없는 slot을 priority에서 제거한다.

        OCPP 2.0.1 TC_A_19_CS 요구사항: Profile 3으로 업그레이드된 상태에서
        NetworkConfigurationPriority에 Profile 2 이하 slot이 남아 있으면 안 된다.
        network_profiles.json에 명시된 slot의 securityProfile만 신뢰하고,
        unknown slot(예: 초기 station_config.json에서 부팅한 slot 0)은 제거한다.
        """
        priority_str = self._get_param("OCPPCommCtrlr", "NetworkConfigurationPriority", "")
        if not priority_str:
            return
        slots = [s.strip() for s in priority_str.split(",") if s.strip()]
        if not slots:
            return

        profiles = load_network_profiles()
        active_slot = slots[0]
        active_profile = profiles.get(active_slot)
        if not active_profile:
            return  # 활성 slot을 모르면 아무것도 하지 않음
        active_sp = int(active_profile.get("securityProfile", 0))

        kept = []
        for slot in slots:
            if slot == active_slot:
                kept.append(slot)
                continue
            slot_profile = profiles.get(slot)
            if slot_profile is None:
                logger.info(f"Pruning unknown slot {slot} from priority (not in network_profiles.json)")
                continue
            slot_sp = int(slot_profile.get("securityProfile", 0))
            if slot_sp < active_sp:
                logger.info(
                    f"Pruning slot {slot} from priority: securityProfile {slot_sp} < active {active_sp}"
                )
                continue
            kept.append(slot)

        new_priority = ",".join(kept)
        if new_priority != priority_str:
            self.device_model["OCPPCommCtrlr"]["NetworkConfigurationPriority"] = (new_priority, "ReadWrite")
            save_device_model(self.device_model)
            logger.info(f"NetworkConfigurationPriority pruned: '{priority_str}' → '{new_priority}'")

    async def _apply_active_network_profile(self) -> None:
        """NetworkConfigurationPriority의 첫 번째 슬롯을 활성 프로파일로 채택한다.

        SetNetworkProfile이 _pending_network_profile_switch를 켜 놓지 않았다면
        Reset이 들어와도 ws_kwargs를 바꾸지 않고 현재 설정을 그대로 사용한다.
        (TC_B_20/TC_B_21_CS: 이전 테스트가 남긴 Priority/slot 데이터로 Reset 때마다
        인증이 풀리는 문제를 막음.)
        """
        if not self._pending_network_profile_switch:
            logger.info(
                "No SetNetworkProfile pending — keeping current connection settings"
            )
            return
        # Consume the flag up front so a failed swap doesn't retry forever.
        self._pending_network_profile_switch = False

        priority = self._get_param("OCPPCommCtrlr", "NetworkConfigurationPriority", "0")
        try:
            active_slot = int(priority.split(",")[0].strip())
        except (ValueError, IndexError):
            logger.warning(f"Invalid NetworkConfigurationPriority value: {priority}")
            return

        profiles = load_network_profiles()
        profile = profiles.get(str(active_slot))
        if not profile:
            logger.info(
                f"No stored network profile for slot {active_slot} — keeping current connection settings"
            )
            return

        new_url = profile.get("ocppCsmsUrl", "")
        if not new_url:
            logger.warning(f"Slot {active_slot} profile missing ocppCsmsUrl — skipping")
            return

        ws_kwargs = StationConfig.build_ws_kwargs_from_profile(
            profile, self._cert_dir, self._ca_cert
        )
        # TC_B_45_CS: SetNetworkProfile often omits basicAuth when switching to
        # another slot on the same CSMS root — the station is expected to
        # carry over its existing credentials. Inherit the Authorization
        # header from the current ws_kwargs if the new profile didn't supply
        # one (and the new security profile actually uses Basic Auth).
        new_sp_int = int(profile.get("securityProfile", 0))
        if (
            new_sp_int in (1, 2)
            and "additional_headers" not in ws_kwargs
            and "additional_headers" in self.ocpp_client._ws_kwargs
        ):
            ws_kwargs["additional_headers"] = self.ocpp_client._ws_kwargs["additional_headers"]
            logger.info(
                "Inherited Authorization header from current connection for new slot"
            )
        self.ocpp_client.update_connection(new_url, ws_kwargs)

        new_sp = str(int(profile.get("securityProfile", 0)))
        self.device_model["SecurityCtrlr"]["SecurityProfile"] = (new_sp, "ReadWrite")
        self.device_model["OCPPCommCtrlr"]["ActiveNetworkProfile"] = (str(active_slot), "ReadOnly")
        save_device_model(self.device_model)
        logger.info(
            f"Active network profile switched: slot={active_slot} securityProfile={new_sp} url={new_url}"
        )

    def _lookup_instanced(
        self, comp: str, var: str, instance: Optional[str]
    ) -> Optional[tuple]:
        """Return (value, mutability) for an instanced device-model entry, or None.

        TC_E_41_CS: OCTT queries OCPPCommCtrlr.MessageTimeout with instance
        "Default", MessageAttempts/MessageAttemptInterval with instance
        "TransactionEvent", etc. These live in _INSTANCED_ENTRIES rather than
        the primary device_model dict and must be resolved separately.
        """
        if instance is None:
            return None
        for c, v, i, value, mutability in _INSTANCED_ENTRIES:
            if c == comp and v == var and i == instance:
                return value, mutability
        return None

    async def handle_get_variables(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_06_CS / TC_B_32_CS: Returns device model variable values.

        Per OCPP 2.0.1 GetVariableStatusEnumType, UnknownComponent and
        UnknownVariable are distinct: the former when the component name
        isn't part of the device model at all, the latter when the component
        exists but the variable under it doesn't.
        """
        results = []
        for item in payload["getVariableData"]:
            comp = item["component"]["name"]
            var  = item["variable"]["name"]
            inst = item["variable"].get("instance")
            attr = item.get("attributeType", "Actual")
            if comp not in self.device_model:
                results.append({
                    "attributeStatus": "UnknownComponent",
                    "component": item["component"],
                    "variable": item["variable"],
                })
                continue
            # TC_E_41_CS: instanced variables live outside the main dict.
            if inst is not None:
                instanced = self._lookup_instanced(comp, var, inst)
                if instanced is None:
                    results.append({
                        "attributeStatus": "UnknownVariable",
                        "component": item["component"],
                        "variable": item["variable"],
                    })
                    continue
                if attr != "Actual":
                    results.append({
                        "attributeStatus": "NotSupportedAttributeType",
                        "component": item["component"],
                        "variable": item["variable"],
                        "attributeType": attr,
                    })
                    continue
                val, _ = instanced
                results.append({
                    "attributeStatus": "Accepted",
                    "component": item["component"],
                    "variable": item["variable"],
                    "attributeType": attr,
                    "attributeValue": val,
                })
                continue
            comp_data = self.device_model[comp]
            if var not in comp_data:
                results.append({
                    "attributeStatus": "UnknownVariable",
                    "component": item["component"],
                    "variable": item["variable"],
                })
                continue
            # TC_B_34_CS: we only store "Actual" values — Target/MinSet/MaxSet
            # aren't tracked, so requests for them must return
            # NotSupportedAttributeType rather than silently echoing Actual.
            if attr != "Actual":
                results.append({
                    "attributeStatus": "NotSupportedAttributeType",
                    "component": item["component"],
                    "variable": item["variable"],
                    "attributeType": attr,
                })
                continue
            val, _ = comp_data[var]
            results.append({
                "attributeStatus": "Accepted",
                "component": item["component"],
                "variable": item["variable"],
                "attributeType": attr,
                "attributeValue": val,
            })
        logger.info(f"GetVariables: returning {len(results)} results")
        return {"getVariableResult": results}

    async def handle_set_variables(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_07_CS: Updates device model variable values.

        SetVariableStatusEnumType distinguishes UnknownComponent (component not
        in device model) from UnknownVariable (variable not under an existing
        component) — same rule as GetVariables.
        """
        results = []
        for item in payload["setVariableData"]:
            comp = item["component"]["name"]
            var  = item["variable"]["name"]
            inst = item["variable"].get("instance")
            val  = item["attributeValue"]
            attr = item.get("attributeType", "Actual")
            if comp not in self.device_model:
                status = "UnknownComponent"
            elif inst is not None:
                # TC_E_41_CS: OCTT writes MessageAttemptInterval instance
                # "TransactionEvent" etc. Store on _INSTANCED_ENTRIES in place.
                instanced = self._lookup_instanced(comp, var, inst)
                if instanced is None:
                    status = "UnknownVariable"
                elif attr != "Actual":
                    status = "NotSupportedAttributeType"
                else:
                    _, mutability = instanced
                    if mutability == "ReadOnly":
                        status = "Rejected"
                    else:
                        rejection = self._validate_variable_value(comp, var, val)
                        if rejection:
                            status = rejection
                        else:
                            # Mutate the matching row in _INSTANCED_ENTRIES.
                            for idx, (c, v, i, _v, _m) in enumerate(_INSTANCED_ENTRIES):
                                if c == comp and v == var and i == inst:
                                    _INSTANCED_ENTRIES[idx] = (c, v, i, val, _m)
                                    break
                            status = "Accepted"
            elif var not in self.device_model[comp]:
                status = "UnknownVariable"
            elif attr != "Actual":
                # Only Actual is stored; Target/MinSet/MaxSet are not supported.
                status = "NotSupportedAttributeType"
            else:
                _, mutability = self.device_model[comp][var]
                if mutability == "ReadOnly":
                    # TC_B_39_CS: SetVariableStatusEnumType has no "ReadOnly" value
                    # — attempting to write a ReadOnly variable must return Rejected.
                    status = "Rejected"
                else:
                    rejection = self._validate_variable_value(comp, var, val)
                    if rejection:
                        status = rejection
                    else:
                        self.device_model[comp][var] = (val, mutability)
                        self._apply_variable_change(comp, var, val)
                        status = "Accepted"
            result = {
                "attributeStatus": status,
                "component": item["component"],
                "variable": item["variable"],
            }
            # TC_B_37_CS: echo attributeType back when the CSMS specified one
            # (e.g. NotSupportedAttributeType responses must carry the Target
            # type the request asked for).
            if "attributeType" in item:
                result["attributeType"] = item["attributeType"]
            results.append(result)
        save_device_model(self.device_model)
        logger.info(f"SetVariables: processed {len(results)} variables")
        return {"setVariableResult": results}

    async def handle_get_base_report(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_12/TC_B_15: GetBaseReport(ConfigurationInventory|FullInventory) emits
        the full device-model report; SummaryInventory is not implemented and must
        be answered with NotSupported per OCPP 2.0.1 GenericDeviceModelStatusEnumType.
        """
        request_id = payload["requestId"]
        report_base = payload.get("reportBase")
        logger.info(f"GetBaseReport requestId={request_id}, reportBase={report_base}")
        if report_base not in ("ConfigurationInventory", "FullInventory"):
            return {"status": "NotSupported"}
        asyncio.create_task(self._send_notify_report(request_id))
        return {"status": "Accepted"}

    async def handle_get_report(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_10_CS: Responds Accepted and sends NotifyReport asynchronously"""
        request_id = payload["requestId"]
        logger.info(f"GetReport requestId={request_id}")
        asyncio.create_task(self._send_notify_report(request_id))
        return {"status": "Accepted"}

    async def _send_notify_report(self, request_id: int) -> None:
        """Sends NotifyReport with full device model.

        TC_B_12_CS / TC_B_53_CS: every reportData entry must carry
        variableCharacteristics (dataType + supportsMonitoring at minimum;
        unit where OCPP 2.0.1 specifies one). Variables that have multiple
        instances (e.g. DeviceDataCtrlr.BytesPerMessage per-RPC) come from
        _INSTANCED_ENTRIES because the dict can only hold one value per key.
        """
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        report_data = []

        def _entry(comp, var, instance, value, mutability):
            data_type = _VAR_DATA_TYPES.get((comp, var), "string")
            characteristics: Dict[str, Any] = {
                "dataType": data_type,
                "supportsMonitoring": False,
            }
            unit = _VAR_UNITS.get((comp, var))
            if unit:
                characteristics["unit"] = unit
            values_list = _VAR_VALUES_LIST.get((comp, var))
            if values_list is not None:
                characteristics["valuesList"] = values_list
            max_limit = _VAR_MAX_LIMIT.get((comp, var))
            if max_limit is not None:
                characteristics["maxLimit"] = max_limit
            variable: Dict[str, Any] = {"name": var}
            if instance is not None:
                variable["instance"] = instance
            return {
                "component": {"name": comp},
                "variable": variable,
                "variableAttribute": [{"type": "Actual", "value": value, "mutability": mutability}],
                "variableCharacteristics": characteristics,
            }

        for comp_name, variables in self.device_model.items():
            for var_name, (value, mutability) in variables.items():
                report_data.append(_entry(comp_name, var_name, None, value, mutability))

        for comp, var, instance, value, mutability in _INSTANCED_ENTRIES:
            report_data.append(_entry(comp, var, instance, value, mutability))
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
        """TC_C_05_CS / TC_C_38_CS: Clears local authorization cache.

        Per §C14, if AuthCacheCtrlr.Enabled is false the CS must reject
        ClearCacheRequest — the cache feature isn't available to clear.
        """
        if not self._get_bool("AuthCacheCtrlr", "Enabled", True):
            logger.info("ClearCache received but AuthCacheCtrlr.Enabled=false — Rejected")
            return {"status": "Rejected"}
        self._auth_cache = {}
        save_auth_cache(self._auth_cache)
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
        """§E14 / TC_E_28..34_CS.

        * transactionId omitted → `ongoingIndicator` MUST be omitted (TC_E_34).
          Otherwise return true when the id is the live tx, false when it's a
          previously-ended tx or completely unknown.
        * `messagesInQueue` must be true when the offline queue still holds a
          TransactionEvent for the queried txId (or any tx when txId omitted).
        """
        tx_id = payload.get("transactionId")
        # Guard against a mocked ocpp_client without offline_queue.
        queued: List[Dict[str, Any]] = []
        queue = getattr(self.ocpp_client, "offline_queue", None)
        if queue is not None and hasattr(queue, "peek"):
            try:
                queued = await queue.peek()
            except Exception as e:
                logger.warning(f"offline_queue.peek failed: {e}")
        def _queue_has_tx(target: Optional[str]) -> bool:
            for entry in queued:
                if entry.get("action") != "TransactionEvent":
                    continue
                q_tx = (entry.get("payload") or {}).get("transactionInfo", {}).get(
                    "transactionId"
                )
                if target is None or q_tx == target:
                    return True
            return False
        if tx_id is None:
            # TC_E_33/E_34_CS: ongoingIndicator MUST be omitted when no txId
            # was provided. Return only messagesInQueue.
            messages_in_queue = _queue_has_tx(None)
            logger.info(
                f"GetTransactionStatus (no txId): messagesInQueue={messages_in_queue}"
            )
            return {"messagesInQueue": messages_in_queue}
        messages_in_queue = _queue_has_tx(tx_id)
        ongoing = (self.transaction_id == tx_id)
        logger.info(
            f"GetTransactionStatus txId={tx_id}: "
            f"ongoingIndicator={ongoing} messagesInQueue={messages_in_queue}"
        )
        return {"messagesInQueue": messages_in_queue, "ongoingIndicator": ongoing}

    # ------------------------------------------------------------------
    # Block F — Remote Control
    # ------------------------------------------------------------------

    async def handle_request_start_transaction(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_F_*: Remote start transaction from CSMS"""
        # TC_B_02_CS: BootNotification 이 Accepted 되기 전에는 트랜잭션 시작 거부.
        if self._boot_status != "Accepted":
            logger.warning(
                f"RequestStartTransaction rejected: boot status is {self._boot_status!r}"
            )
            return {"status": "Rejected"}
        if not self.is_evse_available:
            logger.warning("RequestStartTransaction rejected: EVSE not available")
            return {"status": "Rejected"}
        # TC_F_01_CS: "Cable plugin first" — the tx is already Started via
        # EVConnected TxStartPoint (chargingState=EVConnected, not yet
        # authorized). The incoming RequestStartTransaction is the
        # authorization step, not a new-tx request, so accept it.
        # Reject only when the tx is already authorized (another remote
        # start during an active authorized session has nothing to do).
        if self.transaction_id and self.is_authorized:
            logger.warning("RequestStartTransaction rejected: transaction already authorized")
            return {"status": "Rejected"}

        id_token = payload["idToken"]
        remote_start_id = payload.get("remoteStartId")
        logger.info(
            f"RequestStartTransaction: token={id_token.get('idToken')} "
            f"remoteStartId={remote_start_id}"
        )
        # TC_E_13_CS: the RequestStartTransactionResponse must reach the CSMS
        # before any follow-up request (AuthorizeRequest or TransactionEvent).
        # Defer the Authorize-and-start flow into a background task so this
        # handler returns immediately. OCPP §4.1 would otherwise surface the
        # AuthorizeRequest as an unexpected message while OCTT still waits
        # for the RequestStart response.
        asyncio.create_task(self._remote_start_flow(id_token, remote_start_id))
        response: Dict[str, Any] = {"status": "Accepted"}
        # TC_F_01_CS: when the tx is already running (cable-plug-first),
        # echo the active transactionId so the CSMS can correlate the
        # subsequent TransactionEvent with its RequestStartTransaction.
        if self.transaction_id:
            response["transactionId"] = self.transaction_id
        return response

    async def _remote_start_flow(
        self,
        id_token: Dict[str, Any],
        remote_start_id: Optional[int],
    ) -> None:
        if self._get_bool("AuthCtrlr", "AuthorizeRemoteStart", True):
            try:
                res = await self.ocpp_client.call("Authorize", {"idToken": id_token})
                if not (res and res.get("idTokenInfo", {}).get("status") == "Accepted"):
                    logger.warning("Remote start Authorize not Accepted — aborting")
                    return
            except Exception as e:
                logger.error(f"Authorize failed during remote start: {e}")
                return
        self.is_authorized = True
        self._tx_id_token_value = id_token.get("idToken")
        self._tx_group_id_token_value = None
        # TC_F_01_CS: tx was already started by cable-plug (CablePluggedIn
        # trigger, chargingState=EVConnected, waiting for auth). The
        # remote start authorizes the existing tx — close the relay and
        # emit Updated(triggerReason=RemoteStart, idToken) — do NOT start
        # a second tx. (Local-scan auth of an existing tx uses
        # triggerReason=Authorized; remote-start must use RemoteStart per
        # F01 validation.)
        if self.transaction_id:
            if self.connector_hal.status == "Occupied":
                self.power_contactor_hal.control_relay("Close")
            await self._send_tx_updated(
                "RemoteStart",
                id_token=id_token,
                remote_start_id=remote_start_id,
            )
            return
        # §E01.FR.03: TxStartPoint OR-semantics — if Authorized is listed the
        # tx starts immediately with triggerReason=RemoteStart carrying
        # remoteStartId + idToken; otherwise wait for cable plug.
        tx_start_points = [
            p.strip()
            for p in self._get_param("TxCtrlr", "TxStartPoint", "").split(",")
            if p.strip()
        ]
        if "Authorized" in tx_start_points:
            await self._start_tx_on_authorized(
                id_token,
                trigger_reason="RemoteStart",
                remote_start_id=remote_start_id,
            )
            if self.connector_hal.status == "Occupied":
                self.power_contactor_hal.control_relay("Close")
        else:
            await self._try_start_transaction()

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
        """§G / TC_G_01..G_21_CS: change operational status at CS / EVSE /
        Connector scope.

        The OCPP payload determines the scope:
          * no `evse` field          → Charging Station scope (TC_G_05/06)
          * `evse.id` only           → EVSE scope            (TC_G_03/04)
          * `evse.id` + `connectorId`→ Connector scope       (TC_G_07/08)

        This simulator has a single EVSE + single connector, so all three
        scopes map onto the same underlying availability flag, but the
        handler must still accept and respond correctly for each one.

        While a transaction is active a change to Inoperative is
        Scheduled (TC_G_11/14/17); once the tx ends the scheduled state
        is re-applied. Operative⇢Operative and Inoperative⇢Inoperative are
        idempotent (TC_G_09/10/12/13/15/16). The effective state is
        persisted across reboot (TC_G_18/19/21).
        """
        status = payload["operationalStatus"]
        evse = payload.get("evse") or {}
        evse_id = evse.get("id")
        connector_id = evse.get("connectorId")
        scope = "Connector" if connector_id is not None else (
            "EVSE" if evse_id is not None else "ChargingStation"
        )
        logger.info(
            f"ChangeAvailability scope={scope} evseId={evse_id} "
            f"connectorId={connector_id} → {status}"
        )
        new_available = (status != "Inoperative")
        if not new_available and self.transaction_id:
            # TC_G_11/14/17: can't switch to Inoperative mid-tx; defer
            # and remember so stop_transaction can apply it + emit
            # StatusNotification(Unavailable) when the tx ends.
            self._pending_inoperative = True
            return {"status": "Scheduled"}
        # A plain Operative cancels any pending Inoperative.
        if new_available:
            self._pending_inoperative = False
        # Idempotent: if already in the requested state, still respond
        # Accepted and re-emit StatusNotification per OCPP guidance.
        self.is_evse_available = new_available
        self.device_model["EVSE"]["AvailabilityState"] = (
            ("Available" if new_available else "Inoperative"),
            "ReadOnly",
        )
        save_device_model(self.device_model)
        save_admin_state({
            "is_evse_available": self.is_evse_available,
            "scope": scope,
            "evse_id": evse_id,
            "connector_id": connector_id,
        })
        asyncio.create_task(self._send_availability_status_notification())
        return {"status": "Accepted"}

    async def _send_availability_status_notification(self) -> None:
        """Send StatusNotification reflecting admin + physical state.

        Inoperative → connectorStatus "Unavailable".
        Operative   → live physical state (Available / Occupied).
        """
        if self._force_available_once and self.is_evse_available:
            # TC_L_13_CS: first notification after a firmware-update reboot.
            self.connector_hal.status = "Available"
            self._force_available_once = False
        elif self.is_evse_available:
            self.connector_hal.status = self.connector_hal.read_physical_connection()
        else:
            self.connector_hal.status = "Unavailable"
        payload = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "connectorStatus": self.connector_hal.status,
            "evseId": self.evse_id,
            "connectorId": self.connector_id,
        }
        logger.info(
            f"Connector {self.connector_id} admin status → {self.connector_hal.status}"
        )
        try:
            await self.ocpp_client.call("StatusNotification", payload)
        except Exception as e:
            logger.error(f"Failed to send availability StatusNotification: {e}")

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

    async def send_meter_values(
        self,
        evse_id: Optional[int] = None,
        transaction_id: Optional[str] = None,
        context: str = "Sample.Periodic",
        measurands: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Sends a standalone MeterValues message (Block J / TC_J_01..J_03).

        TC_J_01_CS: clock-aligned samples must carry the boundary timestamp
        (e.g. 08:01:00Z, not the send time 08:01:16Z). Callers that care —
        the aligned-data loop — pass `timestamp=boundary_dt`; others leave it
        None to get datetime.now().
        """
        evse_id = evse_id or self.evse_id
        meter_data = self.power_contactor_hal.read_meter_values()
        ts_dt = timestamp or datetime.now(timezone.utc)
        ts_iso = ts_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        if measurands is None:
            measurands = self._get_param(
                "AlignedDataCtrlr", "Measurands",
                "Energy.Active.Import.Register",
            )
        sampled = self._build_sampled_values(
            measurands, meter_data, context, self.meter_value,
        )
        # TC_J_03_CS: OCTT may set measurands="" to disable sampling. The
        # MeterValuesRequest schema forbids empty sampledValue (minItems=1)
        # AND requires meterValue itself, so we have nothing to send.
        if not sampled:
            return
        payload = {
            "evseId": evse_id,
            "meterValue": [{
                "timestamp": ts_iso,
                "sampledValue": sampled,
            }],
        }
        if transaction_id:
            payload["transactionId"] = transaction_id
        try:
            await self.ocpp_client.call("MeterValues", payload, allow_offline=True)
            logger.info(f"MeterValues sent for evseId={evse_id}")
        except Exception as e:
            logger.error(f"Failed to send MeterValues: {e}")

    async def _aligned_data_loop(self) -> None:
        """TC_J_01/02_CS: clock-aligned MeterValues on AlignedDataCtrlr.Interval.

        OCTT validates that the `timestamp` field on each MeterValuesRequest
        lines up with a clock boundary: for interval=60 the timestamp must
        be on a whole-minute boundary (08:01:00Z), for interval=900 on
        quarter-hour boundaries (08:00, 08:15, ...). Two pieces:
          * sleep until the next boundary (not just `sleep(interval)`),
          * stamp the MeterValue with that boundary time, not now().
        interval<=0 parks the loop awaiting a future change (checked every
        30s). When a tx is active, the sampled values are attached to the
        tx via transactionId so the CSMS can correlate.
        """
        while True:
            try:
                interval = self._get_int("AlignedDataCtrlr", "Interval", 0)
                if interval <= 0:
                    await asyncio.sleep(30)
                    continue
                now = datetime.now(timezone.utc)
                epoch_secs = now.timestamp()
                next_boundary = (int(epoch_secs) // interval + 1) * interval
                sleep_secs = next_boundary - epoch_secs
                await asyncio.sleep(sleep_secs)
                boundary_dt = datetime.fromtimestamp(
                    next_boundary, tz=timezone.utc,
                )
                if self.transaction_id:
                    # TC_J_02_CS: during a tx, clock-aligned samples travel
                    # on TransactionEventRequest (triggerReason=MeterValueClock),
                    # not MeterValuesRequest. The latter's schema forbids
                    # transactionId so mixing them there would fail client
                    # validation entirely (Step 3 in OCTT TC_J_02 spec).
                    await self._send_tx_updated_metervalue(boundary_dt)
                else:
                    await self.send_meter_values(
                        context="Sample.Clock",
                        timestamp=boundary_dt,
                    )
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning(f"Aligned data loop iteration failed: {e}")
                await asyncio.sleep(30)

    # ------------------------------------------------------------------
    # Block K — Firmware Management
    # ------------------------------------------------------------------

    async def handle_update_firmware(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """UpdateFirmwareRequest handler covering TC_K_01_CS and
        TC_L_01..08/11/13/18_CS.

        - TC_L_18_CS: signingCertificate OR signature missing → Rejected.
        - TC_L_05_CS: unparseable / expired signingCertificate →
          InvalidCertificate + SecurityEventNotification(
          type=InvalidFirmwareSigningCertificate).
        - TC_L_11_CS: a second request while an update is in progress is
          Rejected (the simulator cannot truly cancel).
        - TC_L_02_CS / TC_L_03_CS: future installDateTime /
          retrieveDateTime → InstallScheduled / DownloadScheduled first.
        - TC_L_13_CS: AllowNewSessionsPendingFirmwareUpdate=false while a
          transaction is active → DownloadScheduled + force connector(s) to
          Unavailable until reboot.
        - TC_L_06/07/08_CS: OCTT failure markers in the payload drive the
          InvalidSignature / DownloadFailed / InstallVerificationFailed
          paths.
        """
        request_id   = payload["requestId"]
        firmware     = payload.get("firmware") or {}
        location     = firmware.get("location", "") or ""
        signature    = firmware.get("signature", "") or ""
        signing_cert = firmware.get("signingCertificate", "") or ""
        retrieve_dt  = firmware.get("retrieveDateTime", "") or ""
        install_dt   = firmware.get("installDateTime", "") or ""

        # TC_L_18_CS: both fields required for secure firmware update.
        if not signing_cert or not signature:
            logger.warning(
                f"UpdateFirmware rejected (TC_L_18): signingCertificate or "
                f"signature missing, requestId={request_id}"
            )
            return {"status": "Rejected"}

        # TC_L_05_CS: reject invalid signingCertificate + fire
        # SecurityEventNotification. Validity window covers expired/
        # not-yet-valid certs; issuer-trust check covers the TC_L_05
        # scenario where the cert is parseable and time-valid but was
        # issued by an untrusted test CA (e.g. "TestCA") instead of a
        # Manufacturer Root the CS recognises.
        cert_reason = self._validate_cert_pem(signing_cert)
        if cert_reason is None:
            cert_reason = self._validate_firmware_signing_cert_issuer(
                signing_cert
            )
        if cert_reason is not None:
            logger.warning(
                f"UpdateFirmware rejected (TC_L_05): signingCertificate "
                f"invalid ({cert_reason}), requestId={request_id}"
            )
            asyncio.create_task(
                self._send_security_event_notification(
                    "InvalidFirmwareSigningCertificate"
                )
            )
            return {"status": "InvalidCertificate"}

        # TC_L_11_CS.
        if self._firmware_update_in_progress:
            logger.warning(
                f"UpdateFirmware rejected (TC_L_11): update already in "
                f"progress, requestId={request_id}"
            )
            return {"status": "Rejected"}

        logger.info(
            f"UpdateFirmware accepted requestId={request_id} "
            f"location={location} retrieveDT={retrieve_dt} installDT={install_dt}"
        )
        self._firmware_update_in_progress = True
        self._firmware_update_task = asyncio.create_task(
            self._simulate_firmware_update(
                request_id=request_id,
                location=location,
                signature=signature,
                retrieve_dt=retrieve_dt,
                install_dt=install_dt,
            )
        )
        return {"status": "Accepted"}

    @staticmethod
    def _parse_iso_datetime(value: str) -> Optional[datetime]:
        if not value:
            return None
        try:
            s = value.replace("Z", "+00:00") if value.endswith("Z") else value
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    @staticmethod
    def _firmware_location_fault(location: str) -> Optional[str]:
        """Detect OCTT failure markers in firmware.location.

        - "does_not_exist" prefix → TC_L_07_CS DownloadFailed.
        - "install_verification_failed" / "corrupted" / "invalid_firmware"
          → TC_L_08_CS InstallVerificationFailed.
        """
        loc = location.lower()
        if "does_not_exist" in loc:
            return "download_failed"
        if (
            "install_verification_failed" in loc
            or "corrupted" in loc
            or "invalid_firmware" in loc
        ):
            return "install_verification_failed"
        return None

    # TC_L_06_CS: OCTT's "valid" firmware signature is a fixed RSA
    # signature over a specific firmware.bin. All other secure-firmware
    # tests (TC_L_01/02/03) send a signature whose base64 encoding starts
    # with this prefix; TC_L_06 deliberately ships a different signature
    # over the same file to force the InvalidSignature path. Comparing
    # the prefix lets the simulator flag TC_L_06 without actually
    # downloading firmware.bin to verify the RSA signature.
    _OCTT_VALID_FIRMWARE_SIGNATURE_PREFIX = "fjRdvHcjsgVcU2MmgAUzYx5MgNW6Z"

    @classmethod
    def _is_firmware_signature_invalid(cls, signature: str) -> bool:
        """TC_L_06_CS: detect an invalid firmware signature.

        The simulator cannot verify a real signature against a file it
        never downloads, so we treat:
          - an empty / literal "invalid" signature (legacy sentinel), and
          - any signature NOT matching OCTT's known-good prefix
        as invalid. Real manufacturer deployments would replace this
        with actual cryptographic verification against the cert's public
        key plus the downloaded firmware.
        """
        if not signature:
            return True
        if "invalid" in signature.lower():
            return True
        return not signature.startswith(cls._OCTT_VALID_FIRMWARE_SIGNATURE_PREFIX)

    async def _fw_status(self, request_id: int, status: str) -> None:
        try:
            await self.ocpp_client.call("FirmwareStatusNotification",
                                        {"status": status, "requestId": request_id})
            logger.info(f"FirmwareStatusNotification: {status}")
        except Exception as e:
            logger.error(f"Failed to send FirmwareStatusNotification ({status}): {e}")

    async def _set_connectors_unavailable_for_firmware(self) -> None:
        """TC_L_02/03/08/13_CS: mark connector Unavailable for the firmware
        window. Physical HAL is unchanged — this is an OCPP-level signal.
        """
        if self._firmware_update_suspended_connectors:
            return
        # OCPP 2.0.1 §L01.FR.06-07: only Available connectors get flipped.
        # If a tx is active on this (single) connector it is Occupied, not
        # Available — leave it alone. TC_L_13_CS: OCTT rejects an
        # Unavailable notification while the tx is still running.
        if self.transaction_id:
            return
        self._firmware_update_suspended_connectors = True
        try:
            await self.ocpp_client.call("StatusNotification", {
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "connectorStatus": "Unavailable",
                "evseId": self.evse_id,
                "connectorId": self.connector_id,
            })
            logger.info("Firmware update: connector forced Unavailable")
        except Exception as e:
            logger.error(f"Failed to send Unavailable StatusNotification: {e}")

    async def _restore_connectors_after_firmware(self) -> None:
        """TC_L_02/03/08/13_CS: connector returns to Available once the
        firmware window has ended (after simulated reboot or on failure).
        """
        if not self._firmware_update_suspended_connectors:
            return
        self._firmware_update_suspended_connectors = False
        try:
            self.connector_hal.status = self.connector_hal.read_physical_connection()
            await self.ocpp_client.call("StatusNotification", {
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "connectorStatus": self.connector_hal.status,
                "evseId": self.evse_id,
                "connectorId": self.connector_id,
            })
            logger.info(
                f"Firmware update: connector restored → {self.connector_hal.status}"
            )
        except Exception as e:
            logger.error(f"Failed to send post-firmware StatusNotification: {e}")

    async def _simulate_firmware_update(
        self,
        request_id: int,
        location: str,
        signature: str,
        retrieve_dt: str,
        install_dt: str,
    ) -> None:
        reboot_handoff = False
        try:
            now         = datetime.now(timezone.utc)
            retrieve_at = self._parse_iso_datetime(retrieve_dt)
            install_at  = self._parse_iso_datetime(install_dt)
            fault       = self._firmware_location_fault(location)
            # TC_L_08_CS: the location-fault path (invalid_firmware.bin /
            # install_verification_failed / corrupted) reuses TC_L_06's
            # "alt" signature bytes with a *matching* signing cert, so
            # OCTT expects SignatureVerified here followed by
            # InstallVerificationFailed — not InvalidSignature. Skip the
            # signature-prefix check when the location explicitly flags
            # an install-phase failure.
            sig_invalid = (
                False if fault == "install_verification_failed"
                else self._is_firmware_signature_invalid(signature)
            )
            allow_new_sessions = self._get_bool(
                "ChargingStation", "AllowNewSessionsPendingFirmwareUpdate", True,
            )
            tx_active = bool(self.transaction_id)

            # TC_L_03_CS: future retrieveDateTime → DownloadScheduled.
            # TC_L_13_CS: ongoing tx + AllowNewSessionsPendingFirmwareUpdate
            # false → same DownloadScheduled path with connector Unavailable.
            scheduled_download = (
                (retrieve_at is not None and retrieve_at > now)
                or (tx_active and not allow_new_sessions)
            )
            if scheduled_download:
                await self._fw_status(request_id, "DownloadScheduled")
                # TC_L_13_CS: with AllowNewSessionsPendingFirmwareUpdate=false,
                # defer the rest of the firmware flow until the ongoing tx
                # ends. OCTT stops the tx after 150 s; cap the wait at 200 s
                # so a stuck tx doesn't hang the simulator forever.
                if tx_active and not allow_new_sessions:
                    waited = 0.0
                    while self.transaction_id is not None and waited < 200.0:
                        await asyncio.sleep(1.0)
                        waited += 1.0
                    await self._set_connectors_unavailable_for_firmware()
                # TC_L_03_CS: future retrieveDateTime wait (bounded).
                if retrieve_at is not None and retrieve_at > now:
                    delay = min((retrieve_at - now).total_seconds(), 5.0)
                    if delay > 0:
                        await asyncio.sleep(delay)

            # TC_L_07_CS.
            if fault == "download_failed":
                await asyncio.sleep(2)
                await self._fw_status(request_id, "Downloading")
                await asyncio.sleep(2)
                await self._fw_status(request_id, "DownloadFailed")
                return

            await asyncio.sleep(2)
            await self._fw_status(request_id, "Downloading")
            await asyncio.sleep(2)
            await self._fw_status(request_id, "Downloaded")

            # TC_L_06_CS.
            if sig_invalid:
                await asyncio.sleep(2)
                await self._fw_status(request_id, "InvalidSignature")
                asyncio.create_task(
                    self._send_security_event_notification("InvalidFirmwareSignature")
                )
                return

            await asyncio.sleep(2)
            await self._fw_status(request_id, "SignatureVerified")

            # TC_L_02_CS.
            if install_at is not None and install_at > now:
                await asyncio.sleep(2)
                await self._fw_status(request_id, "InstallScheduled")
                await self._set_connectors_unavailable_for_firmware()
                delay = min((install_at - now).total_seconds(), 5.0)
                if delay > 0:
                    await asyncio.sleep(delay)

            # TC_L_08_CS.
            if fault == "install_verification_failed":
                await asyncio.sleep(2)
                await self._set_connectors_unavailable_for_firmware()
                await self._fw_status(request_id, "Installing")
                await asyncio.sleep(2)
                await self._fw_status(request_id, "InstallVerificationFailed")
                return

            # Happy path (TC_L_01/02/03/13_CS): Installing → InstallRebooting
            # → reconnect → Installed via _on_reconnect.
            await asyncio.sleep(2)
            await self._set_connectors_unavailable_for_firmware()
            await self._fw_status(request_id, "Installing")
            await asyncio.sleep(2)
            await self._fw_status(request_id, "InstallRebooting")

            self._pending_firmware_update_reboot = True
            self._pending_firmware_update_request_id = request_id
            reboot_handoff = True
            await asyncio.sleep(1)
            if self.ocpp_client.ws:
                try:
                    await self.ocpp_client.ws.close()
                except Exception as e:
                    logger.warning(f"ws.close() on firmware-update reboot raised: {e}")
        finally:
            self._firmware_update_in_progress = False
            # When we hand off to _on_reconnect, let the reconnect path
            # restore connector status after the FirmwareUpdate boot. Any
            # other exit restores right away.
            if not reboot_handoff:
                await self._restore_connectors_after_firmware()

    async def handle_get_log(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_K_03_CS / TC_N_25_CS / TC_N_26_CS / TC_N_35_CS: log upload.

        TC_N_25_CS requires `filename` in GetLogResponse to be present and
        non-empty. TC_N_26_CS requires an UploadFailure path with
        retries/retryInterval when the remote location is unreachable.
        """
        request_id  = payload["requestId"]
        log_type    = payload.get("logType", "DiagnosticsLog")
        log_info    = payload.get("log", {}) or {}
        remote_loc  = log_info.get("remoteLocation", "") or ""
        retries     = int(payload.get("retries", 0))
        retry_intv  = int(payload.get("retryInterval", 0))

        now_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{log_type}_{request_id}_{now_ts}.log"
        logger.info(
            f"GetLog requestId={request_id}, logType={log_type}, "
            f"remoteLocation={remote_loc}, retries={retries}, "
            f"retryInterval={retry_intv}, filename={filename}"
        )

        # TC_N_26_CS marker: the configured invalid remoteLocation uses a
        # nonexistent path. Detect common OCTT markers to drive the failure
        # flow; otherwise simulate a successful upload.
        will_fail = (
            "does_not_exist" in remote_loc
            or "nonexistent" in remote_loc.lower()
            or remote_loc.endswith("/nonexistent")
        )
        asyncio.create_task(
            self._simulate_log_upload(
                request_id=request_id,
                will_fail=will_fail,
                retries=retries,
                retry_interval=retry_intv,
            )
        )
        return {"status": "Accepted", "filename": filename}

    async def _simulate_log_upload(
        self,
        request_id: int,
        will_fail: bool = False,
        retries: int = 0,
        retry_interval: int = 0,
    ) -> None:
        # TC_N_25_CS happy path: single Uploading → Uploaded pair.
        # TC_N_26_CS failure path: OCPP 2.0.1 GetLogRequest.retries is the
        # *total* number of attempts. OCTT expects (retries × retryInterval)
        # seconds between the first Uploading and the final UploadFailure.
        if not will_fail:
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
            return

        # OCTT (TC_N_26_CS) expects (1 + retries) Uploading messages spaced
        # exactly retryInterval seconds apart, with UploadFailure emitted
        # right after the last attempt — total span first→failure ≈
        # retries × retryInterval.
        attempts = 1 + max(retries, 0)
        interval = max(retry_interval, 1)
        for i in range(attempts):
            if i > 0:
                await asyncio.sleep(interval)
            try:
                await self.ocpp_client.call("LogStatusNotification",
                                            {"status": "Uploading", "requestId": request_id})
                logger.info(f"LogStatusNotification: Uploading (attempt {i + 1}/{attempts})")
            except Exception as e:
                logger.error(f"Failed to send LogStatusNotification (Uploading): {e}")
        try:
            await self.ocpp_client.call("LogStatusNotification",
                                        {"status": "UploadFailure", "requestId": request_id})
            logger.info("LogStatusNotification: UploadFailure")
        except Exception as e:
            logger.error(f"Failed to send LogStatusNotification (UploadFailure): {e}")

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
            "TransactionEvent", "SignChargingStationCertificate",
        }
        if requested not in supported:
            return {"status": "NotImplemented"}

        # TC_F_26_CS (§F06.FR.04/FR.05): once BootNotification has been
        # Accepted, a triggered BootNotification request must be Rejected.
        # The CS only sends a fresh BootNotification after a Reset / cold
        # start when _boot_status leaves "Accepted".
        if requested == "BootNotification" and self._boot_status == "Accepted":
            logger.info("TriggerMessage(BootNotification) rejected — boot already Accepted")
            return {"status": "Rejected"}

        asyncio.create_task(self._send_triggered_message(requested))
        return {"status": "Accepted"}

    async def _send_triggered_message(self, requested: str) -> None:
        try:
            if requested == "BootNotification":
                # TC_B_02_CS / OCPP 2.0.1 §L: Triggered BootNotification must use reason="Triggered"
                await self.boot_routine(reason="Triggered")

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
                        "evse": {"id": self.evse_id, "connectorId": self.connector_id},
                        "transactionInfo": {"transactionId": self.transaction_id},
                    })

            elif requested == "SignChargingStationCertificate":
                csr_pem = await self._generate_csr_pem()
                # TC_A_23_CS: CertificateSignedRequest 타임아웃 시 재시도
                if self._cert_signing_task and not self._cert_signing_task.done():
                    self._cert_signing_task.cancel()
                self._cert_signing_task = asyncio.create_task(
                    self._send_sign_certificate_with_retry(csr_pem)
                )
        except Exception as e:
            logger.error(f"Failed to send triggered message {requested}: {e}")

    # ------------------------------------------------------------------
    # 충전 흐름 (RFID / 케이블 / State C)
    # ------------------------------------------------------------------

    async def handle_rfid_scan(self, raw_uid: str) -> None:
        logger.info(f"RFID scanned: {raw_uid}")
        # OCTT TC_C_02 etc. issue KeyCode-typed tokens — the hex UID from the
        # RFID reader is a keycode string as far as OCPP is concerned. Keep
        # type as "KeyCode" so the authorize payload matches OCTT expectations.
        id_token = {"idToken": raw_uid, "type": "KeyCode"}
        # TC_B_21_CS / TC_C_04_CS: handling a second scan while an authorized
        # transaction is live depends on whether the new idToken matches the
        # one that started the transaction.
        #   - Same idToken → local stop (no AuthorizeRequest per §C03).
        #   - Different idToken → send Authorize; the response may come back
        #     Accepted, but the transaction MUST NOT be stopped and no
        #     TransactionEventRequest may follow (§C01.FR.03).
        if self.transaction_id and self.is_authorized:
            if raw_uid == self._tx_id_token_value:
                logger.info("Stop-scan (same idToken) — stopping transaction")
                await self.stop_transaction("Local", id_token=id_token)
                return
            # Different idToken — check if it can authorize locally (cached
            # Accepted + LocalPreAuthorize → skip AuthorizeRequest, TC_C_41_CS)
            # or ask CSMS to authorize (TC_C_39_CS / TC_C_44_CS). If the result
            # carries the same groupIdToken the user belongs to the same group
            # and is allowed to stop the transaction. Otherwise keep the tx
            # running and emit no TransactionEvent (§C01.FR.03 / TC_C_04_CS).
            logger.info("Scan during active tx with DIFFERENT idToken — checking group")
            cache_enabled = self._get_bool("AuthCacheCtrlr", "Enabled", True)
            pre_auth      = self._get_bool("AuthCtrlr", "LocalPreAuthorize", False)
            cached_info   = (
                self._lookup_auth_cache(raw_uid)
                if (cache_enabled and pre_auth) else None
            )
            if cached_info is not None:
                logger.info(
                    f"Cache hit for different-token scan {raw_uid} — "
                    f"skipping AuthorizeRequest"
                )
                info = cached_info
            else:
                try:
                    res = await self.ocpp_client.call("Authorize", {"idToken": id_token})
                except Exception as e:
                    logger.error(f"Authorize (different-token scan) failed: {e}")
                    return
                info = (res or {}).get("idTokenInfo", {}) or {}
                if info.get("status") == "Accepted" and cache_enabled:
                    self._auth_cache[raw_uid] = {
                        "idTokenInfo": info,
                        "stored_at": time.time(),
                    }
                    save_auth_cache(self._auth_cache)
            if info.get("status") != "Accepted":
                logger.info("Different-token not Accepted — tx continues")
                return
            other_group = (info.get("groupIdToken") or {}).get("idToken")
            if (
                self._tx_group_id_token_value
                and other_group
                and other_group == self._tx_group_id_token_value
            ):
                logger.info(
                    f"Different idToken shares groupIdToken {other_group} — "
                    f"stopping transaction"
                )
                # TC_C_41_CS: Ended event's idToken identifies the token that
                # triggered the stop — i.e. the group-mate idToken2, not the
                # original starter. OCTT's <Configured valid_idtoken_idtoken>
                # placeholder is context-bound, not a fixed value.
                await self.stop_transaction("Local", id_token=id_token)
            else:
                logger.info("Different idToken without matching group — tx continues")
            return
        # TC_C_32_CS: Authorization Cache + LocalPreAuthorize. When both are
        # enabled and we have a non-expired cached Accepted entry for this
        # idToken, skip AuthorizeRequest entirely and act on the cached info.
        cache_enabled = self._get_bool("AuthCacheCtrlr", "Enabled", True)
        pre_auth      = self._get_bool("AuthCtrlr", "LocalPreAuthorize", False)
        cached_info   = self._lookup_auth_cache(raw_uid) if (cache_enabled and pre_auth) else None
        try:
            if cached_info is not None:
                logger.info(f"Authorization cache hit for {raw_uid} — skipping AuthorizeRequest")
                id_token_info = cached_info
                status = id_token_info.get("status")
            else:
                res = await self.ocpp_client.call("Authorize", {"idToken": id_token})
                id_token_info = (res or {}).get("idTokenInfo", {}) or {}
                status = id_token_info.get("status")
                if status == "Accepted" and cache_enabled:
                    self._auth_cache[raw_uid] = {
                        "idTokenInfo": id_token_info,
                        "stored_at": time.time(),
                    }
                    save_auth_cache(self._auth_cache)
            group_id = (id_token_info.get("groupIdToken") or {}).get("idToken")
            if status == "Accepted":
                if not self.transaction_id:
                    self.is_authorized = True
                    self._tx_id_token_value = raw_uid
                    self._tx_group_id_token_value = group_id
                    # OCPP 2.0.1 §E02 TxStartPoint OR semantics: if "Authorized"
                    # is in the list, authorization alone starts the
                    # transaction even without the cable connected
                    # (TC_B_21_CS). Otherwise fall back to the AND-style
                    # _try_start_transaction which waits for cable plug.
                    tx_start_points = [
                        p.strip()
                        for p in self._get_param("TxCtrlr", "TxStartPoint", "").split(",")
                        if p.strip()
                    ]
                    if "Authorized" in tx_start_points:
                        await self._start_tx_on_authorized(id_token)
                        if self.connector_hal.status == "Occupied":
                            self.power_contactor_hal.control_relay("Close")
                    else:
                        await self._try_start_transaction()
                else:
                    # tx started by EVConnected trigger; this scan authorizes it.
                    # (is_authorized must be False here — the authorized+active
                    # case was handled by the early return above.)
                    self.is_authorized = True
                    self._tx_id_token_value = raw_uid
                    self._tx_group_id_token_value = group_id
                    self.power_contactor_hal.control_relay("Close")
                    await self._send_tx_updated("Authorized", id_token=id_token)
            else:
                # TC_C_02_CS / TC_C_07_CS: per OCPP 2.0.1 §C02, when Authorize
                # is rejected (Expired / Invalid / Unknown / Blocked ...) the
                # CS must NOT emit any further TransactionEventRequest until
                # another trigger ends the tx. Cancel the periodic meter-value
                # loop so it stops pumping MeterValuePeriodic updates; the
                # transaction stays live and will close out on cable unplug.
                logger.warning(
                    f"Authorize rejected: status={status} — silencing tx events"
                )
                if self._meter_task and not self._meter_task.done():
                    self._meter_task.cancel()
                    self._meter_task = None
        except Exception as e:
            logger.error(f"Authorisation call failed: {e}")

    def _lookup_auth_cache(self, raw_uid: str) -> Optional[Dict[str, Any]]:
        """Return a non-expired cached idTokenInfo for raw_uid, or None.

        Expiry is enforced by AuthCacheCtrlr.LifeTime (seconds since stored_at)
        AND by idTokenInfo.cacheExpiryDateTime if the CSMS provided one. If the
        entry is expired, it's evicted.
        """
        entry = self._auth_cache.get(raw_uid)
        if not entry:
            return None
        info = entry.get("idTokenInfo") or {}
        if info.get("status") != "Accepted":
            return None
        lifetime = self._get_int("AuthCacheCtrlr", "LifeTime", 86400)
        stored_at = entry.get("stored_at", 0)
        if lifetime > 0 and (time.time() - stored_at) > lifetime:
            self._auth_cache.pop(raw_uid, None)
            save_auth_cache(self._auth_cache)
            return None
        expiry_iso = info.get("cacheExpiryDateTime")
        if expiry_iso:
            try:
                expiry_dt = datetime.fromisoformat(expiry_iso.replace("Z", "+00:00"))
                if datetime.now(timezone.utc) > expiry_dt:
                    self._auth_cache.pop(raw_uid, None)
                    save_auth_cache(self._auth_cache)
                    return None
            except Exception:
                pass
        return info

    def _update_cache_from_tx_response(
        self,
        response: Optional[Dict[str, Any]],
        raw_uid: Optional[str],
    ) -> Optional[str]:
        """C10_FR_05: update AuthCache with idTokenInfo from TransactionEventResponse.

        Returns idTokenInfo.status if present so the caller can decide whether
        to act on a deauthorization (Invalid / Blocked / Expired / Unknown).
        """
        if not response or not raw_uid:
            return None
        info = response.get("idTokenInfo") or {}
        status = info.get("status")
        if not status:
            return None
        if self._get_bool("AuthCacheCtrlr", "Enabled", True):
            self._auth_cache[raw_uid] = {
                "idTokenInfo": info,
                "stored_at": time.time(),
            }
            save_auth_cache(self._auth_cache)
        return status

    async def _on_replay_response(
        self,
        action: str,
        payload: Dict[str, Any],
        response: Dict[str, Any],
    ) -> None:
        """TC_C_16_CS / TC_C_17_CS: handle responses to offline-queued replays."""
        if action != "TransactionEvent":
            return
        id_token = payload.get("idToken") or {}
        raw_uid = id_token.get("idToken")
        if not raw_uid:
            return
        status = self._update_cache_from_tx_response(response, raw_uid)
        if status and status != "Accepted":
            await self._handle_tx_auth_rejection(status, id_token)

    async def _handle_tx_auth_rejection(
        self,
        status: str,
        id_token: Optional[Dict[str, Any]],
    ) -> None:
        """§C12.FR.04 / FR.05: react to an idTokenInfo that is not Accepted
        during an active transaction.

        • StopTxOnInvalidId=true  → stop the tx (Ended, reason=DeAuthorized).
        • StopTxOnInvalidId=false → suspend energy transfer only: open the
          relay and emit Updated(triggerReason=Deauthorized, chargingState
          =SuspendedEVSE). Transaction keeps running until cable unplug
          (TC_C_17_CS).
        """
        if not self.transaction_id:
            return
        if self._get_bool("TxCtrlr", "StopTxOnInvalidId", True):
            logger.info(
                f"StopTxOnInvalidId=true + idTokenInfo.status={status} — "
                f"stopping tx (Deauthorized)"
            )
            await self.stop_transaction("DeAuthorized", id_token=id_token)
            return
        logger.info(
            f"StopTxOnInvalidId=false + idTokenInfo.status={status} — "
            f"suspending energy transfer"
        )
        try:
            self.power_contactor_hal.control_relay("Open")
        except Exception as e:
            logger.warning(f"Failed to open relay on deauth suspend: {e}")
        self.is_authorized = False
        # TC_C_17_CS: emit a plain charging-state transition. OCTT treats a
        # triggerReason=Deauthorized here as illegal (that trigger is reserved
        # for the StopTxOnInvalidId=true path). Omit idToken so OCTT's
        # response doesn't cascade back into another rejection loop.
        await self._send_tx_updated(
            "ChargingStateChanged",
            id_token=None,
            charging_state="SuspendedEVSE",
        )

    async def _send_tx_updated(
        self,
        trigger_reason: str,
        id_token: Optional[Dict[str, Any]] = None,
        charging_state: Optional[str] = None,
        remote_start_id: Optional[int] = None,
    ) -> None:
        """Send TransactionEvent(Updated) on an already-started transaction.

        chargingState (EVConnected / SuspendedEVSE / SuspendedEV / Charging) is
        mandatory on Updated events for several trigger reasons (TC_B_21_CS —
        CablePluggedIn). Derived from live state if the caller didn't specify.

        TC_F_01_CS: remote_start_id is included in transactionInfo when the
        tx was authorized by a RequestStartTransaction (cable-plugin-first
        path) — the Started event carried CablePluggedIn trigger, so the
        remoteStartId binds to the Updated(Authorized) event instead.
        """
        if not self.transaction_id:
            return
        if charging_state is None:
            if self._state_c_active:
                charging_state = "Charging"
            elif self.connector_hal.status == "Occupied":
                charging_state = "EVConnected"
            else:
                charging_state = "Idle"
        next_seq_no = self._tx_seq_no + 1
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        transaction_info: Dict[str, Any] = {
            "transactionId": self.transaction_id,
            "chargingState": charging_state,
        }
        if remote_start_id is not None:
            transaction_info["remoteStartId"] = remote_start_id
        payload: Dict[str, Any] = {
            "eventType": "Updated",
            "timestamp": now_iso,
            "triggerReason": trigger_reason,
            "seqNo": next_seq_no,
            "evse": {"id": self.evse_id, "connectorId": self.connector_id},
            "transactionInfo": transaction_info,
        }
        if id_token is not None:
            payload["idToken"] = id_token
        res: Optional[Dict[str, Any]] = None
        try:
            # allow_offline=True: TC_C_16_CS exercises the offline auth path —
            # user scans during a WS outage, cache grants local pre-auth, and
            # the Updated(Authorized) event must persist through reconnect so
            # CSMS can re-validate (possibly rejecting → Deauthorized stop).
            res = await self.ocpp_client.call(
                "TransactionEvent", payload, allow_offline=True,
            )
        except Exception as e:
            logger.error(f"Failed to send Updated TransactionEvent ({trigger_reason}): {e}")
        else:
            # Reserve the seqNo only on successful send so a cancelled send
            # doesn't leave a hole in the transaction event sequence.
            self._tx_seq_no = next_seq_no
        # C10_FR_05: only Updated events carrying an idToken (e.g. Authorized /
        # StopAuthorized trigger reasons) receive an idTokenInfo back. Update
        # cache + react to CSMS rejection (stop or suspend per StopTxOnInvalidId).
        if id_token is not None:
            raw_uid = id_token.get("idToken")
            status = self._update_cache_from_tx_response(res, raw_uid)
            if status and status != "Accepted":
                await self._handle_tx_auth_rejection(status, id_token)

    async def _send_tx_updated_metervalue(self, boundary_dt: datetime) -> None:
        """TC_J_02_CS: clock-aligned MeterValue during a tx.

        Sends TransactionEvent(Updated, triggerReason=MeterValueClock) with
        a sampledValue block stamped at the clock-aligned boundary time.
        """
        if not self.transaction_id:
            return
        next_seq_no = self._tx_seq_no + 1
        ts_iso = boundary_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        measurands = self._get_param(
            "AlignedDataCtrlr", "Measurands",
            "Energy.Active.Import.Register",
        )
        meter_data = self.power_contactor_hal.read_meter_values()
        sampled = self._build_sampled_values(
            measurands, meter_data, "Sample.Clock", self.meter_value,
        )
        if not sampled:
            return
        payload: Dict[str, Any] = {
            "eventType": "Updated",
            "timestamp": ts_iso,
            "triggerReason": "MeterValueClock",
            "seqNo": next_seq_no,
            "evse": {"id": self.evse_id, "connectorId": self.connector_id},
            "transactionInfo": {
                "transactionId": self.transaction_id,
            },
            "meterValue": [{
                "timestamp": ts_iso,
                "sampledValue": sampled,
            }],
        }
        try:
            await self.ocpp_client.call(
                "TransactionEvent", payload, allow_offline=True,
            )
        except Exception as e:
            logger.error(f"Aligned-clock TransactionEvent failed: {e}")
        else:
            self._tx_seq_no = next_seq_no

    async def simulate_cable_plugged(self) -> None:
        logger.info("Cable plugged in. Connector Occupied.")
        self.connector_hal.status = "Occupied"
        # TC_E_05_CS: cable arrived within the EVConnectionTimeOut window —
        # cancel the deauthorization watchdog.
        self._cancel_ev_connect_timeout()
        payload = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "connectorStatus": "Occupied",
            "evseId": self.evse_id,
            "connectorId": self.connector_id
        }
        # TC_E_16_CS: StatusNotification must be queued offline so the CSMS
        # eventually sees the plug event. Don't short-circuit the tx-update
        # path below when the ws is down — the tx Updated event must be
        # queued alongside it.
        try:
            await self.ocpp_client.call(
                "StatusNotification", payload, allow_offline=True,
            )
        except Exception as e:
            logger.error(f"Failed to send cable-plug StatusNotification: {e}")

        # OCPP 2.0.1 §E02: TxStartPoint is OR — if "EVConnected" is listed,
        # the transaction starts as soon as the EV is connected, before/without
        # authorization. Authorization arrives later via RFID → handle_rfid_scan
        # which may update or stop the transaction.
        tx_start_points = [
            p.strip()
            for p in self._get_param("TxCtrlr", "TxStartPoint", "").split(",")
            if p.strip()
        ]
        if self.transaction_id:
            # Transaction already started (via Authorized trigger). Close the
            # relay now that the cable is connected and notify CSMS.
            if self.is_authorized:
                self.power_contactor_hal.control_relay("Close")
            await self._send_tx_updated("CablePluggedIn")
            # TC_E_45_CS: release handle_state_c once CablePluggedIn has
            # been emitted so the ChargingStateChanged event (whether
            # fired below or by cp_adc_monitor next) strictly follows it.
            self._cable_plug_event_sent = True
        elif "EVConnected" in tx_start_points:
            await self._start_tx_on_ev_connected()
            # If already authorized (RFID scanned first), energize immediately.
            if self.is_authorized:
                self.power_contactor_hal.control_relay("Close")
        else:
            await self._try_start_transaction()
        # TC_E_16_CS / TC_E_43..E_45_CS: when the cable is plugged with the
        # tx already authorized (relay closed), simulate the EV's State C
        # pilot transition so CSMS sees the ChargingStateChanged=Charging
        # event. Real rigs produce this via the CP ADC monitor, but on some
        # hardware setups (esp. during offline periods) the ADC dip isn't
        # reliably observed — drive the state transition from software so
        # the queued event stream still matches the spec.
        if self.transaction_id and self.is_authorized and not self._state_c_active:
            await self.handle_state_c()

    async def _start_tx_on_authorized(
        self,
        id_token: Dict[str, Any],
        trigger_reason: str = "Authorized",
        remote_start_id: Optional[int] = None,
    ) -> None:
        """Start a transaction triggered by authorization before cable plug.

        [OCPP 2.0.1 §E02] TxStartPoint "Authorized" — emit TransactionEvent
        with eventType=Started when the user is authorized, regardless of
        cable state. chargingState reflects whether the EV is connected yet.

        trigger_reason: "Authorized" for local RFID flows; "RemoteStart" for
            CSMS-initiated RequestStartTransaction (TC_E_13_CS).
        remote_start_id: RequestStartTransaction remoteStartId — included in
            transactionInfo when present (TC_E_13_CS / F02).
        """
        if self.transaction_id:
            return
        self.transaction_id = str(uuid.uuid4())
        self.meter_value = 0.0
        self._state_c_active = False
        # TC_E_45_CS: if the tx started with the cable already plugged in
        # (e.g. Authorized+Occupied on first scan) treat CablePluggedIn as
        # already-observed — otherwise handle_state_c from cp_adc_monitor
        # would block forever waiting for simulate_cable_plugged to fire.
        self._cable_plug_event_sent = (self.connector_hal.status == "Occupied")
        self._tx_seq_no = 0
        # TC_E_05_CS: arm the EVConnectionTimeOut watchdog BEFORE sending the
        # Started event. OCTT measures the timeout from the moment it
        # accepted the Authorize, and the TransactionEvent(Started) send can
        # take several seconds (TLS / meter reads), pushing the observed
        # interval past EVConnectionTimeOut.
        if self.connector_hal.status != "Occupied":
            self._schedule_ev_connect_timeout()
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        measurands = self._get_param(
            "SampledDataCtrlr", "TxStartedMeasurands",
            "Energy.Active.Import.Register",
        )
        meter_data = self.power_contactor_hal.read_meter_values()
        charging_state = (
            "EVConnected" if self.connector_hal.status == "Occupied" else "Idle"
        )
        transaction_info: Dict[str, Any] = {
            "transactionId": self.transaction_id,
            "chargingState": charging_state,
        }
        if remote_start_id is not None:
            transaction_info["remoteStartId"] = remote_start_id
        payload = {
            "eventType": "Started",
            "timestamp": now_iso,
            "triggerReason": trigger_reason,
            "seqNo": self._tx_seq_no,
            "evse": {"id": self.evse_id, "connectorId": self.connector_id},
            "idToken": id_token,
            "transactionInfo": transaction_info,
        }
        sampled = self._build_sampled_values(
            measurands, meter_data, "Transaction.Begin", 0,
        )
        if sampled:
            payload["meterValue"] = [{"timestamp": now_iso, "sampledValue": sampled}]
        try:
            res = await self.ocpp_client.call("TransactionEvent", payload, allow_offline=True)
            logger.info(f"Transaction started ({trigger_reason}): {self.transaction_id}")
        except Exception as e:
            logger.error(f"Failed to send Started TransactionEvent ({trigger_reason}): {e}")
            res = None
        # TC_C_34_CS / TC_C_17_CS (C10_FR_05 / C12.FR.06): CSMS may reject the
        # token on the Started response. Either stop or suspend depending on
        # StopTxOnInvalidId.
        status = self._update_cache_from_tx_response(res, self._tx_id_token_value)
        if status and status != "Accepted":
            await self._handle_tx_auth_rejection(status, id_token)
            if not self.transaction_id:
                return
        if not self._meter_task or self._meter_task.done():
            self._meter_task = asyncio.create_task(
                self._meter_values_loop(self.transaction_id)
            )
        self._start_tx_ended_aligned_loop(self.transaction_id)
        self._start_tx_ended_sampled_loop(self.transaction_id)

    def _start_tx_ended_aligned_loop(self, tx_id: str) -> None:
        """TC_J_03_CS: launch accumulator for clock-aligned TxEnded samples.

        Runs only while the tx is active; each wake emits no CSMS traffic,
        it just appends a meterValue entry to self._tx_ended_aligned_samples
        stamped at the clock-boundary with context=Sample.Clock. The list
        is flushed into the Ended event's meterValue in stop_transaction.
        """
        self._tx_ended_aligned_samples = []
        if self._tx_ended_aligned_task and not self._tx_ended_aligned_task.done():
            self._tx_ended_aligned_task.cancel()
        self._tx_ended_aligned_task = asyncio.create_task(
            self._tx_ended_aligned_loop(tx_id)
        )

    async def _tx_ended_aligned_loop(self, tx_id: str) -> None:
        while self.transaction_id == tx_id:
            try:
                interval = self._get_int("AlignedDataCtrlr", "TxEndedInterval", 0)
                if interval <= 0:
                    await asyncio.sleep(30)
                    continue
                loop = asyncio.get_running_loop()
                now = datetime.now(timezone.utc)
                epoch_secs = now.timestamp()
                next_boundary = (int(epoch_secs) // interval + 1) * interval
                await asyncio.sleep(next_boundary - epoch_secs)
                if self.transaction_id != tx_id:
                    return
                boundary_dt = datetime.fromtimestamp(
                    next_boundary, tz=timezone.utc,
                )
                ts_iso = boundary_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                measurands = self._get_param(
                    "AlignedDataCtrlr", "TxEndedMeasurands",
                    "Energy.Active.Import.Register",
                )
                meter_data = self.power_contactor_hal.read_meter_values()
                sampled = self._build_sampled_values(
                    measurands, meter_data, "Sample.Clock", self.meter_value,
                )
                if sampled:
                    self._tx_ended_aligned_samples.append({
                        "timestamp": ts_iso,
                        "sampledValue": sampled,
                    })
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning(f"tx-ended aligned loop iteration failed: {e}")
                await asyncio.sleep(30)

    def _start_tx_ended_sampled_loop(self, tx_id: str) -> None:
        """TC_J_10_CS: launch accumulator for periodic TxEnded samples.

        Mirrors _start_tx_ended_aligned_loop but for the SampledDataCtrlr
        variant — samples every TxEndedInterval seconds from tx start
        (not clock-aligned), context=Sample.Periodic, measurands from
        SampledDataCtrlr.TxEndedMeasurands. List flushed into the Ended
        event in stop_transaction.
        """
        self._tx_ended_sampled_samples = []
        if self._tx_ended_sampled_task and not self._tx_ended_sampled_task.done():
            self._tx_ended_sampled_task.cancel()
        self._tx_ended_sampled_task = asyncio.create_task(
            self._tx_ended_sampled_loop(tx_id)
        )

    async def _tx_ended_sampled_loop(self, tx_id: str) -> None:
        asyncio_loop = asyncio.get_running_loop()
        next_fire = asyncio_loop.time()
        while self.transaction_id == tx_id:
            try:
                interval = self._get_int(
                    "SampledDataCtrlr", "TxEndedInterval", 0,
                )
                if interval <= 0:
                    await asyncio.sleep(30)
                    continue
                next_fire += interval
                sleep_s = next_fire - asyncio_loop.time()
                if sleep_s > 0:
                    await asyncio.sleep(sleep_s)
                if self.transaction_id != tx_id:
                    return
                ts_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                measurands = self._get_param(
                    "SampledDataCtrlr", "TxEndedMeasurands",
                    "Energy.Active.Import.Register",
                )
                meter_data = self.power_contactor_hal.read_meter_values()
                sampled = self._build_sampled_values(
                    measurands, meter_data, "Sample.Periodic", self.meter_value,
                )
                if sampled:
                    self._tx_ended_sampled_samples.append({
                        "timestamp": ts_iso,
                        "sampledValue": sampled,
                    })
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning(f"tx-ended sampled loop iteration failed: {e}")
                await asyncio.sleep(30)

    def _schedule_ev_connect_timeout(self) -> None:
        """TC_E_05_CS: arm (or re-arm) the EV cable-plug watchdog.

        The watchdog is deadline-based on the event loop clock: the arm time
        is stamped immediately, and when the coroutine finally gets CPU we
        sleep only for the remaining slice. Without this, a blocking
        synchronous call (STM32 SPI meter read, ws.send TLS handshake) after
        create_task() would push the 60s countdown start-time out — OCTT
        measured 66s instead of 60s because the coroutine didn't actually
        begin its sleep until 6s after arm.
        """
        self._cancel_ev_connect_timeout()
        timeout_s = self._get_int("TxCtrlr", "EVConnectionTimeOut", 60)
        if timeout_s <= 0:
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        logger.info(f"EVConnectionTimeOut watchdog armed ({timeout_s}s)")
        self._ev_connect_timeout_task = asyncio.create_task(
            self._ev_connect_timeout_watchdog(deadline)
        )

    def _cancel_ev_connect_timeout(self) -> None:
        if self._ev_connect_timeout_task and not self._ev_connect_timeout_task.done():
            self._ev_connect_timeout_task.cancel()
        self._ev_connect_timeout_task = None

    async def _ev_connect_timeout_watchdog(self, deadline: float) -> None:
        """TC_E_05_CS E03.FR.05: fire TransactionEvent with triggerReason
        EVConnectTimeout when the cable doesn't arrive in time.

        `deadline` is an event-loop monotonic timestamp set at arm time.
        """
        try:
            loop = asyncio.get_running_loop()
            remaining = deadline - loop.time()
            if remaining > 0:
                await asyncio.sleep(remaining)
        except asyncio.CancelledError:
            return
        if self.connector_hal.status == "Occupied" or not self.transaction_id:
            return
        logger.warning(
            f"EVConnectionTimeOut expired — deauthorizing tx {self.transaction_id}"
        )
        tx_stop_points = [
            p.strip()
            for p in self._get_param("TxCtrlr", "TxStopPoint", "").split(",")
            if p.strip()
        ]
        if "Authorized" in tx_stop_points:
            # stop_transaction maps "Timeout" → triggerReason "EVConnectTimeout"
            # and uses "Timeout" as stoppedReason.
            await self.stop_transaction("Timeout", id_token=None)
        else:
            self.is_authorized = False
            self._tx_id_token_value = None
            self._tx_group_id_token_value = None
            await self._send_tx_updated("EVConnectTimeout", id_token=None)

    async def _start_tx_on_ev_connected(self) -> None:
        """Start a transaction triggered by cable plug-in.

        [OCPP 2.0.1 §E02] TxStartPoint "EVConnected" — emit TransactionEvent
        with eventType=Started and triggerReason=EVConnected before any
        authorization has happened.
        """
        if self.transaction_id:
            return
        self.transaction_id = str(uuid.uuid4())
        self.meter_value = 0.0
        self._state_c_active = False
        # TC_E_45_CS: the Started event carries triggerReason=CablePluggedIn
        # here — cable plug is already the start trigger, so future
        # ChargingStateChanged events from cp_adc_monitor are ok.
        self._cable_plug_event_sent = True
        self._tx_seq_no = 0
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        measurands = self._get_param(
            "SampledDataCtrlr", "TxStartedMeasurands",
            "Energy.Active.Import.Register",
        )
        meter_data = self.power_contactor_hal.read_meter_values()
        # [OCPP 2.0.1 Part 2] TriggerReasonEnumType uses "CablePluggedIn" for this event;
        # chargingState uses "EVConnected".
        payload = {
            "eventType": "Started",
            "timestamp": now_iso,
            "triggerReason": "CablePluggedIn",
            "seqNo": self._tx_seq_no,
            "evse": {"id": self.evse_id, "connectorId": self.connector_id},
            "transactionInfo": {
                "transactionId": self.transaction_id,
                "chargingState": "EVConnected",
            },
        }
        sampled = self._build_sampled_values(
            measurands, meter_data, "Transaction.Begin", 0,
        )
        if sampled:
            payload["meterValue"] = [{"timestamp": now_iso, "sampledValue": sampled}]
        try:
            await self.ocpp_client.call("TransactionEvent", payload)
            logger.info(f"Transaction started (EVConnected): {self.transaction_id}")
        except Exception as e:
            logger.error(f"Failed to send Started TransactionEvent (EVConnected): {e}")
        if not self._meter_task or self._meter_task.done():
            self._meter_task = asyncio.create_task(
                self._meter_values_loop(self.transaction_id)
            )
        self._start_tx_ended_aligned_loop(self.transaction_id)
        self._start_tx_ended_sampled_loop(self.transaction_id)

    async def simulate_cable_unplugged(self) -> None:
        logger.info("Cable unplugged. Connector Available.")
        self.connector_hal.status = "Available"
        # TC_E_45_CS: reset the cable-plug gate so the next plug-in cycle
        # emits CablePluggedIn before State C again.
        self._cable_plug_event_sent = False
        if self.transaction_id:
            if self._get_bool("TxCtrlr", "StopTxOnEVSideDisconnect", True):
                logger.info("Transaction active during unplug. Stopping transaction (EVDisconnected).")
                await self.stop_transaction("EVDisconnected")
            else:
                logger.info("Cable unplugged but StopTxOnEVSideDisconnect=false — transaction continues.")

        # TC_G_11/14/17_CS: if the EVSE was scheduled Inoperative during
        # the tx, stop_transaction has now committed it — report the
        # connector as Unavailable on unplug, not Available.
        connector_status = "Available" if self.is_evse_available else "Unavailable"
        self.connector_hal.status = connector_status
        payload = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "connectorStatus": connector_status,
            "evseId": self.evse_id,
            "connectorId": self.connector_id
        }
        try:
            await self.ocpp_client.call(
                "StatusNotification", payload, allow_offline=True,
            )
        except Exception as e:
            logger.error(f"Failed to process cable unplug: {e}")

        # TC_B_21_CS: a Reset(OnIdle) that arrived while the cable was plugged
        # in can only fire once the station is fully idle. Re-check after the
        # unplug StatusNotification has been sent.
        self._try_execute_deferred_reset()

    async def _meter_values_loop(self, transaction_id: str) -> None:
        """TC_J_02_CS: Periodically reports TransactionEvent(Updated) with MeterValues.

        TC_C_39_CS: seqNo must stay gapless. We reserve the next value locally
        and only commit it onto self._tx_seq_no AFTER the send succeeds, so a
        cancel mid-send (e.g. stop_transaction while an Authorize is in flight
        before it) doesn't burn a seqNo that never reaches the CSMS.

        Stop conditions:
          • transaction ended (self.transaction_id changes)
          • SampledDataCtrlr.TxUpdatedInterval <= 0 — OCPP 2.0.1 defines this
            as "periodic sampled data disabled", so exit the loop cleanly
            instead of busy-looping with sleep(0).
          • connector became Available (cable unplugged) — even if
            StopTxOnEVSideDisconnect=false keeps the tx live, no power flows
            without an EV, so suppress the emission until the cable is
            plugged back in (or the tx ends).
        """
        # TC_J_09_CS: timestamps must be collected exactly <interval> seconds
        # apart. Using plain sleep(interval) drifts because the TLS send
        # roundtrip (~6s observed with OCTT) gets added to each cycle —
        # OCTT measured 36s/37s gaps instead of 30s. Use a deadline tracked
        # on the monotonic loop clock: each iteration's wake target is
        # `start + N*interval`, independent of how long the previous send
        # took. If a send overruns the next boundary (e.g. interval=1s),
        # the sleep clamps to 0 and we fire immediately.
        asyncio_loop = asyncio.get_running_loop()
        next_fire = asyncio_loop.time()
        while self.transaction_id == transaction_id:
            interval = self._get_int("SampledDataCtrlr", "TxUpdatedInterval", 60)
            if interval <= 0:
                logger.info(
                    "TxUpdatedInterval<=0 — periodic MeterValue reporting disabled"
                )
                return
            next_fire += interval
            sleep_s = next_fire - asyncio_loop.time()
            if sleep_s > 0:
                await asyncio.sleep(sleep_s)

            if self.transaction_id != transaction_id:
                return
            if self.connector_hal.status != "Occupied":
                logger.debug(
                    "Connector not Occupied — skipping MeterValuePeriodic emission"
                )
                continue

            meter_data = self.power_contactor_hal.read_meter_values()
            real_power = meter_data.get("power", 0.0)
            self.meter_value += real_power * (interval / 3600.0)  # Wh

            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            next_seq_no = self._tx_seq_no + 1
            measurands = self._get_param("SampledDataCtrlr", "TxUpdatedMeasurands",
                                         "Energy.Active.Import.Register")
            sampled = self._build_sampled_values(
                measurands, meter_data, "Sample.Periodic", self.meter_value,
            )
            # TC_J_03_CS: if OCTT disabled sampled values for this context
            # (empty Measurands), skip the iteration — the loop's purpose is
            # to emit MeterValuePeriodic samples, and an empty sampledValue
            # would fail schema validation.
            if not sampled:
                continue
            payload = {
                "eventType": "Updated",
                "timestamp": now_iso,
                "triggerReason": "MeterValuePeriodic",
                "seqNo": next_seq_no,
                "evse": {"id": self.evse_id, "connectorId": self.connector_id},
                "transactionInfo": {
                    "transactionId": self.transaction_id
                },
                "meterValue": [
                    {
                        "timestamp": now_iso,
                        "sampledValue": sampled,
                    }
                ]
            }
            try:
                await self.ocpp_client.call("TransactionEvent", payload, allow_offline=True)
            except Exception as e:
                logger.error(f"Failed to send meter value: {e}")
            else:
                # Commit seqNo only when the CSMS actually accepted the event.
                # (CancelledError is a BaseException — it skips this branch and
                # leaves self._tx_seq_no unchanged.)
                self._tx_seq_no = next_seq_no

    async def _try_start_transaction(self) -> None:
        if self.is_authorized and self.connector_hal.status == "Occupied":
            if not self.transaction_id:
                self.transaction_id = str(uuid.uuid4())
                self.meter_value = 0.0
                self._state_c_active = False
                # TC_E_45_CS: tx starts with cable already plugged, so
                # CablePluggedIn is already "observed" — allow handle_state_c
                # to run without waiting for a new cable-plug event.
                self._cable_plug_event_sent = True
                self._tx_seq_no = 0
                now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                meter_data = self.power_contactor_hal.read_meter_values()
                measurands = self._get_param("SampledDataCtrlr", "TxStartedMeasurands",
                                             "Energy.Active.Import.Register")
                payload = {
                    "eventType": "Started",
                    "timestamp": now_iso,
                    "triggerReason": "Authorized",
                    "seqNo": self._tx_seq_no,
                    "evse": {"id": self.evse_id, "connectorId": self.connector_id},
                    "transactionInfo": {
                        "transactionId": self.transaction_id
                    },
                }
                sampled = self._build_sampled_values(
                    measurands, meter_data, "Transaction.Begin", self.meter_value,
                )
                if sampled:
                    payload["meterValue"] = [{
                        "timestamp": now_iso,
                        "sampledValue": sampled,
                    }]
                await self.ocpp_client.call("TransactionEvent", payload, allow_offline=True)
                self.power_contactor_hal.control_relay("Close")
                # Drop to 53% PWM (32 Amps continuous limit) to allow vehicle onboard charger to pull power
                self.power_contactor_hal.set_pwm_duty(53)

                if self._meter_task:
                    self._meter_task.cancel()
                self._meter_task = asyncio.create_task(self._meter_values_loop(self.transaction_id))
                self._start_tx_ended_aligned_loop(self.transaction_id)
                self._start_tx_ended_sampled_loop(self.transaction_id)

    async def handle_state_c(self) -> None:
        """Called by main.py ADC monitor when CP voltage drops to +6V (< 40000 ADC).

        TC_C_17_CS: emitting chargingState=Charging requires that we are
        actually supplying power — i.e. the user is authorized and the relay
        is closed. If the EV signals State C while we have no authorization
        yet (tx started on CablePluggedIn alone), we stay silent: the CP
        transition is just "EV ready", not "charging". The event will fire
        once authorization arrives and the relay closes.

        TC_E_45_CS: gate on _cable_plug_event_sent so CablePluggedIn is
        always emitted BEFORE ChargingStateChanged. Without this, the ADC
        monitor beats the proximity debounce and fires State C first,
        leaving the cable-plug event to race in afterwards (and often get
        orphaned into the offline queue when the ws closes).
        simulate_cable_plugged re-drives handle_state_c after emitting
        CablePluggedIn, so deferring here doesn't drop the event.
        """
        if not (
            self.transaction_id
            and not self._state_c_active
            and self.is_authorized
            and self._cable_plug_event_sent
        ):
            return
        # Snapshot the txId so a concurrent stop_transaction clearing it mid-flight
        # doesn't leave us sending an event with an invalid id.
        tx_id = self.transaction_id
        self._state_c_active = True
        logger.info("Control Pilot dropped to State C (+6V). EV is Charging!")
        self._tx_seq_no += 1
        payload = {
            "eventType": "Updated",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "triggerReason": "ChargingStateChanged",
            "seqNo": self._tx_seq_no,
            "evse": {"id": self.evse_id, "connectorId": self.connector_id},
            "transactionInfo": {
                "transactionId": tx_id,
                "chargingState": "Charging"
            }
        }
        try:
            await self.ocpp_client.call("TransactionEvent", payload, allow_offline=True)
        except Exception as e:
            logger.error(f"handle_state_c: TransactionEvent call failed: {e}")

    async def stop_transaction(
        self,
        stopped_reason: str = "Local",
        id_token: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self.transaction_id:
            # TC_E_45_CS: when a scan-stop happens while the CS is offline,
            # OCTT's reusable "StopAuthorized" validation (cs_test_cases.txt
            # §Reusable state StopAuthorized) requires two TransactionEvents:
            #   1. Updated(triggerReason=StopAuthorized, chargingState=Charging)
            #   2. Ended(triggerReason=ChargingStateChanged, chargingState=
            #      EVConnected, stoppedReason=Local)
            # Online scan-stop keeps the single-Ended form (TC_E_15_CS's
            # strict per-message check: triggerReason=StopAuthorized +
            # stoppedReason=Local + eventType=Ended must coexist on the
            # same message).
            is_offline_scan_stop = (
                stopped_reason == "Local"
                and not getattr(self.ocpp_client, "ws", None)
            )
            if is_offline_scan_stop:
                # First event: Updated(StopAuthorized) while chargingState is
                # still Charging (relay not yet opened). idToken SHOULD be
                # included per E07.FR.02 — on same-idToken stop this is the
                # starting token, on same-group stop the stopping token.
                await self._send_tx_updated("StopAuthorized", id_token=id_token)

            self.power_contactor_hal.control_relay("Open")
            # Restore 100% PWM (+12V Standing State)
            self.power_contactor_hal.set_pwm_duty(100)

            if self._meter_task:
                self._meter_task.cancel()
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._tx_seq_no += 1

            # [OCPP 2.0.1 Part 2] stoppedReason uses ReasonEnumType (e.g. "DeAuthorized"),
            # triggerReason uses TriggerReasonEnumType (e.g. "Deauthorized" — lowercase 'a').
            trigger_reason_map = {
                "Local":           "StopAuthorized",
                "Remote":          "RemoteStop",
                # TC_G_02_CS / TC_E_20_CS: cable unplug → CS loses CP-pilot
                # communication with the EV. Per OCPP 2.0.1 Part 6
                # validation (triggerReason must be EVCommunicationLost +
                # stoppedReason=EVDisconnected + chargingState=Idle) this
                # is the expected triggerReason, not EVDeparted (which is
                # for ParkingBayOccupancy detectors).
                "EVDisconnected":  "EVCommunicationLost",
                "DeAuthorized":    "Deauthorized",
                # TC_B_22_CS: Reset(Immediate) ends the tx with stoppedReason
                # "ImmediateReset" but triggerReason "ResetCommand" (per spec).
                "ImmediateReset":  "ResetCommand",
                # TC_E_05_CS: EVConnectionTimeOut expired without cable plug —
                # stoppedReason=Timeout, triggerReason=EVConnectTimeout.
                "Timeout":         "EVConnectTimeout",
            }
            trigger_reason = trigger_reason_map.get(stopped_reason, "StopAuthorized")
            # TC_E_45_CS: offline scan-stop's Ended event reports the
            # chargingState transition as its trigger (the earlier
            # Updated already signalled StopAuthorized).
            if is_offline_scan_stop:
                trigger_reason = "ChargingStateChanged"

            meter_data = self.power_contactor_hal.read_meter_values()
            measurands = self._get_param("SampledDataCtrlr", "TxEndedMeasurands",
                                         "Energy.Active.Import.Register")
            # TC_B_21_CS: chargingState on Ended reflects the state at stop
            # time — EVConnected when the cable is still plugged in (Local /
            # StopAuthorized stop), Idle once the cable is unplugged.
            charging_state = (
                "EVConnected" if self.connector_hal.status == "Occupied" else "Idle"
            )
            payload: Dict[str, Any] = {
                "eventType": "Ended",
                "timestamp": now_iso,
                "triggerReason": trigger_reason,
                "seqNo": self._tx_seq_no,
                "evse": {"id": self.evse_id, "connectorId": self.connector_id},
                "transactionInfo": {
                    "transactionId": self.transaction_id,
                    "stoppedReason": stopped_reason,
                    "chargingState": charging_state,
                },
            }
            # TC_J_03_CS: OCTT sets TxEndedMeasurands="" to suppress sampled
            # values on Ended. _build_sampled_values returns [] in that case
            # — and the MeterValue schema's sampledValue requires minItems=1
            # so a block with an empty sampledValue fails validation and the
            # whole Ended never reaches the CSMS. Only include meterValue
            # when at least one sampledValue was generated.
            meter_value_entries: List[Dict[str, Any]] = []
            # TC_J_03_CS: flush clock-aligned TxEnded samples accumulated
            # during the tx (AlignedDataCtrlr.TxEndedInterval/TxEndedMeasurands)
            # into the Ended event.
            meter_value_entries.extend(self._tx_ended_aligned_samples)
            self._tx_ended_aligned_samples = []
            if self._tx_ended_aligned_task and not self._tx_ended_aligned_task.done():
                self._tx_ended_aligned_task.cancel()
            # TC_J_10_CS: flush periodic TxEnded samples accumulated during
            # the tx (SampledDataCtrlr.TxEndedInterval/TxEndedMeasurands).
            meter_value_entries.extend(self._tx_ended_sampled_samples)
            self._tx_ended_sampled_samples = []
            if self._tx_ended_sampled_task and not self._tx_ended_sampled_task.done():
                self._tx_ended_sampled_task.cancel()
            sampled = self._build_sampled_values(
                measurands, meter_data, "Transaction.End", self.meter_value,
            )
            if sampled:
                meter_value_entries.append({
                    "timestamp": now_iso,
                    "sampledValue": sampled,
                })
            if meter_value_entries:
                payload["meterValue"] = meter_value_entries
            # TC_C_39_CS: include the stopping token on the Ended event when
            # the stop was triggered by a scan.
            if id_token is not None:
                payload["idToken"] = id_token
            # TC_F_04_CS: clear the tx state BEFORE awaiting the Ended send.
            # Without this, a CSMS RequestStartTransaction that arrives
            # during the Ended roundtrip (e.g. right after an
            # EVConnectTimeout-driven stop) sees transaction_id +
            # is_authorized as still set and gets rejected. Snapshot the
            # txId for cache updates / the ended-tx history.
            ended_tx_id = self.transaction_id
            self._ended_tx_ids.add(ended_tx_id)
            if len(self._ended_tx_ids) > 32:
                self._ended_tx_ids.pop()
            self.transaction_id = None
            self.is_authorized = False
            self._state_c_active = False
            self._tx_id_token_value = None
            self._tx_group_id_token_value = None
            # TC_E_05_CS: tx is over (for any reason) — cancel the
            # EVConnectionTimeOut watchdog if it's still armed.
            self._cancel_ev_connect_timeout()
            res = await self.ocpp_client.call("TransactionEvent", payload, allow_offline=True)
            # C10_FR_05: keep cache in sync with idTokenInfo from the Ended
            # response too (e.g. CSMS rolls validity forward on stop).
            if id_token is not None:
                self._update_cache_from_tx_response(res, id_token.get("idToken"))

            # Re-apply availability if it was scheduled as Inoperative
            if not self.is_evse_available:
                self.device_model["EVSE"]["AvailabilityState"] = ("Inoperative", "ReadOnly")

            # TC_G_11/14/17_CS: a ChangeAvailability(Inoperative) received
            # during the tx returned "Scheduled"; now that the tx ended,
            # commit the state so the pending-unplug StatusNotification
            # reports Unavailable.
            if self._pending_inoperative:
                self.is_evse_available = False
                self.device_model["EVSE"]["AvailabilityState"] = (
                    "Inoperative", "ReadOnly",
                )
                save_device_model(self.device_model)
                save_admin_state({"is_evse_available": False})
                self._pending_inoperative = False

            # TC_B_21_CS: a Reset(OnIdle) received during the transaction
            # returned "Scheduled"; fire the deferred reboot only once the
            # station is truly idle (tx ended AND cable unplugged).
            self._try_execute_deferred_reset()

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
    def _spki_bit_string_content(spki_der: bytes) -> bytes:
        """Return the BIT STRING contents of a SubjectPublicKeyInfo DER
        (stripped of the leading unused-bits byte), per RFC 6960 §4.1.1.

        SPKI ::= SEQUENCE { algorithm AlgorithmIdentifier, subjectPublicKey BIT STRING }
        """
        def _read_len(buf: bytes, i: int) -> tuple:
            b = buf[i]
            if b & 0x80:
                n = b & 0x7F
                return int.from_bytes(buf[i + 1:i + 1 + n], "big"), i + 1 + n
            return b, i + 1

        if spki_der[0] != 0x30:
            raise ValueError("SPKI not SEQUENCE")
        _, i = _read_len(spki_der, 1)
        if spki_der[i] != 0x30:
            raise ValueError("AlgorithmIdentifier not SEQUENCE")
        alg_len, alg_body = _read_len(spki_der, i + 1)
        i = alg_body + alg_len
        if spki_der[i] != 0x03:
            raise ValueError("subjectPublicKey not BIT STRING")
        bs_len, bs_body = _read_len(spki_der, i + 1)
        return spki_der[bs_body + 1:bs_body + bs_len]

    @staticmethod
    def _make_cert_hash_data(pem: str) -> Dict[str, str]:
        """Compute RFC 6960 OCSP-style hash data for a certificate.

        TC_M_12_CS: OCTT verifies issuerNameHash/issuerKeyHash/serialNumber
        against values it derives from the installed cert, so these must be
        real — hashing the PEM text yields garbage.
        """
        try:
            from cryptography import x509
            from cryptography.hazmat.primitives import serialization
            cert = x509.load_pem_x509_certificate(pem.encode())
            issuer_der = cert.issuer.public_bytes()
            spki_der = cert.public_key().public_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            key_content = ChargingStationController._spki_bit_string_content(spki_der)
            return {
                "hashAlgorithm": "SHA256",
                "issuerNameHash": hashlib.sha256(issuer_der).hexdigest(),
                "issuerKeyHash": hashlib.sha256(key_content).hexdigest(),
                "serialNumber": format(cert.serial_number, "x")[:40],
            }
        except Exception as e:
            logger.warning(f"_make_cert_hash_data: falling back to PEM digest ({e})")
            digest = hashlib.sha256(pem.encode()).hexdigest()
            return {
                "hashAlgorithm": "SHA256",
                "issuerNameHash": digest,
                "issuerKeyHash":  digest,
                "serialNumber":   digest[:16],
            }

    @staticmethod
    def _validate_cert_pem(pem: str) -> Optional[str]:
        """Parse PEM and check validity window.

        Returns None when the certificate is structurally valid and currently
        within its validity period, otherwise a short reason string
        ("unparseable", "expired", "not_yet_valid"). Used by TC_M_07_CS
        (reject expired CA cert) and TC_A_14_CS (reject bad CertificateSigned
        payload).
        """
        try:
            from cryptography import x509
        except ImportError:
            return None  # validation not available → fall back to legacy
        try:
            cert = x509.load_pem_x509_certificate(pem.encode())
        except Exception:
            return "unparseable"
        try:
            now = datetime.now(timezone.utc)
            # cryptography >=42 exposes UTC-aware *_utc props; older versions
            # use naive UTC via not_valid_before/not_valid_after.
            not_after = getattr(cert, "not_valid_after_utc", None) or \
                cert.not_valid_after.replace(tzinfo=timezone.utc)
            not_before = getattr(cert, "not_valid_before_utc", None) or \
                cert.not_valid_before.replace(tzinfo=timezone.utc)
            if now > not_after:
                return "expired"
            if now < not_before:
                return "not_yet_valid"
        except Exception:
            return "unparseable"
        return None

    # TC_L_05_CS: OCTT's invalid-certificate test ships a cert whose
    # subject CN carries a well-known marker (".incorrect" suffix). We
    # only match distinctive markers that are not naturally part of a
    # legitimate cert name. TC_L_08_CS for example uses a cert named
    # "Invalid Firmware SigningCertificate" that is actually valid —
    # the test is about install-verification failure, not cert issues
    # — so "invalid" is NOT in the reject list.
    _INVALID_FIRMWARE_CERT_CN_MARKERS = (
        "incorrect",
        "revoked",
    )

    @classmethod
    def _validate_firmware_signing_cert_issuer(cls, pem: str) -> Optional[str]:
        """Returns a short reason string when the signing cert carries an
        OCTT invalidity marker in its subject CN, else None."""
        try:
            from cryptography import x509
            from cryptography.x509.oid import NameOID
        except ImportError:
            return None  # cryptography unavailable → accept legacy behaviour
        try:
            cert = x509.load_pem_x509_certificate(pem.encode())
        except Exception:
            return "unparseable"
        try:
            attrs = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        except Exception:
            return None
        if not attrs:
            return None
        subject_cn = attrs[0].value.lower()
        for marker in cls._INVALID_FIRMWARE_CERT_CN_MARKERS:
            if marker in subject_cn:
                return f"subject_cn_{marker}"
        return None

    async def handle_install_certificate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_06_CS, TC_A_07_CS, TC_M_07_CS: install a CA certificate.

        TC_M_07_CS requires rejection of a malformed or expired certificate;
        only well-formed, currently-valid certs are saved to disk and tracked
        in self.installed_certificates.
        """
        cert_type: str = payload["certificateType"]
        pem: str       = payload["certificate"]

        reason = self._validate_cert_pem(pem)
        if reason is not None:
            logger.warning(
                f"InstallCertificate rejected: type={cert_type} reason={reason}"
            )
            return {"status": "Rejected"}

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
        save_installed_certificates(self.installed_certificates)
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
        """TC_A_11_CS, TC_A_12_CS, TC_M_23_CS: delete an installed certificate.

        TC_M_23_CS (§M04.FR.06): the Charging Station must not allow deletion
        of its own ChargingStationCertificate — return Failed in that case.
        """
        hash_data: Dict = payload.get("certificateHashData", {})
        serial: Optional[str] = hash_data.get("serialNumber")

        if not serial or serial not in self.installed_certificates:
            return {"status": "NotFound"}

        entry = self.installed_certificates[serial]
        if entry.get("certificateType") == "ChargingStationCertificate":
            logger.warning(
                f"DeleteCertificate refused: serial={serial} is the Charging "
                f"Station Certificate (TC_M_23_CS)"
            )
            return {"status": "Failed"}
        try:
            if os.path.exists(entry["pem_path"]):
                os.remove(entry["pem_path"])
        except OSError as e:
            logger.error(f"DeleteCertificate: failed to remove file: {e}")
            return {"status": "Failed"}

        del self.installed_certificates[serial]
        save_installed_certificates(self.installed_certificates)
        logger.info(f"DeleteCertificate: removed serial={serial}")
        return {"status": "Accepted"}

    async def handle_get_certificate_status(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_13_CS, TC_A_14_CS"""
        logger.info("Handling GetCertificateStatus")
        return {"status": "Accepted"}

    async def handle_certificate_signed(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_A_19_CS, TC_A_20_CS, TC_A_14_CS: 서명된 클라이언트 인증서를 저장한다.

        TC_A_14_CS: an invalid certificateChain must be rejected and a
        SecurityEventNotification(type=InvalidChargingStationCertificate) must
        be sent (§A02.FR.07/A03.FR.07).
        """
        cert_chain_pem: str = payload["certificateChain"]
        cert_type: str      = payload.get("certificateType", "ChargingStationCertificate")

        reason = self._validate_cert_pem(cert_chain_pem)
        if reason is not None:
            logger.warning(
                f"CertificateSigned rejected: type={cert_type} reason={reason}"
            )
            event_type = (
                "InvalidChargingStationCertificate"
                if cert_type == "ChargingStationCertificate"
                else "InvalidV2GChargingStationCertificate"
            )
            asyncio.create_task(self._send_security_event_notification(event_type))
            # Unblock any in-flight SignCertificate retry loop so it doesn't
            # hang waiting for a chain we just rejected (TC_A_23_CS).
            if self._cert_signed_event is not None and not self._cert_signed_event.is_set():
                self._cert_signed_event.set()
            return {"status": "Rejected"}

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

        # NOTE: ChargingStationCertificate is *not* a valid value for the
        # GetInstalledCertificateIdsResponse certificateType enum (per OCPP
        # 2.0.1 GetCertificateIdUseEnumType). We must not add the renewed
        # client cert to self.installed_certificates — doing so would
        # produce a TypeConstraintViolation when listing certs (TC_M_23_CS).

        # Track which CSMS URL this cert is valid for (TC_A_21_CS downgrade-prevention)
        if cert_type == "ChargingStationCertificate":
            current_url = (getattr(self.ocpp_client, "server_url", "") or "").rstrip("/")
            self._cert_valid_for_url = current_url
            save_cert_metadata({"valid_for_url": current_url})

        # TC_A_23_CS: SignCertificate 재시도 루프가 대기 중이면 신호
        if self._cert_signed_event is not None and not self._cert_signed_event.is_set():
            self._cert_signed_event.set()

        # Send SecurityEventNotification after cert install/renewal
        security_profile = self._get_int("SecurityCtrlr", "SecurityProfile", 0)
        event_type = "RenewChargingStationCertificate" if security_profile >= 2 else "CertificateInstalled"
        asyncio.create_task(self._send_security_event_notification(event_type))

        # TC_A_11_CS: under Profile 3, OCTT expects the CS to reconnect with
        # the freshly signed client cert almost immediately. Schedule a
        # reload-and-reconnect that runs after this CallResult is sent.
        if (
            cert_type == "ChargingStationCertificate"
            and security_profile == 3
        ):
            asyncio.create_task(self._reconnect_with_new_client_cert())

        return {"status": "Accepted"}

    async def _reconnect_with_new_client_cert(self) -> None:
        """Rebuild the SSL context from the renewed client.crt/client.key and
        bounce the WebSocket so the next handshake presents the new cert.
        TC_A_11_CS: must happen within OCTT's 65s post-CertificateSigned
        window — the SecurityEventNotification reply has already been queued.
        """
        # Let the CertificateSigned response and SecurityEventNotification
        # reach the CSMS before tearing the socket down.
        await asyncio.sleep(1.0)
        try:
            current_url = (getattr(self.ocpp_client, "server_url", "") or "").rstrip("/")
            profile = {
                "securityProfile": 3,
                "ocppCsmsUrl": current_url,
            }
            ws_kwargs = StationConfig.build_ws_kwargs_from_profile(
                profile, self._cert_dir, self._ca_cert,
            )
            self.ocpp_client.update_connection(current_url, ws_kwargs)
            logger.info("New client cert installed — bouncing WS to reconnect with it")
        except Exception as e:
            logger.error(f"Failed to rebuild SSL context after CertificateSigned: {e}")
            return
        # Skip the retry backoff so the reconnect is observed within OCTT's
        # post-CertificateSigned wait window.
        self.ocpp_client._skip_next_reconnect_wait = True
        if self.ocpp_client.ws:
            try:
                await self.ocpp_client.ws.close()
            except Exception as e:
                logger.warning(f"ws.close() raised after CertificateSigned: {e}")

    # ------------------------------------------------------------------
    # Block B (추가) — SetNetworkProfile
    # ------------------------------------------------------------------

    async def handle_set_network_profile(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """TC_B_13_CS: Sets a network connection profile slot.

        수신한 connectionData를 data/network_profiles.json에 저장한다.
        활성화는 NetworkConfigurationPriority 변경 + Reset 시점에 수행한다.
        """
        slot = payload["configurationSlot"]
        conn_data = payload.get("connectionData", {})
        url = conn_data.get("ocppCsmsUrl", "")
        sp  = int(conn_data.get("securityProfile", 0))
        ocpp_version = conn_data.get("ocppVersion", "unknown")

        if sp in (2, 3) and not url.startswith("wss://"):
            logger.warning(
                f"SetNetworkProfile rejected: profile {sp} requires wss:// URL, got {url}"
            )
            return {"status": "Rejected"}

        # TC_A_22_CS: OCPP 2.0.1 §A10 — 보안 프로파일은 단방향 업그레이드만 허용.
        # 현재 활성 프로파일보다 낮은 프로파일로의 SetNetworkProfile은 거부.
        current_sp = int(self._get_param("SecurityCtrlr", "SecurityProfile", "0"))
        if sp < current_sp:
            logger.warning(
                f"SetNetworkProfile rejected: downgrade from current profile "
                f"{current_sp} to {sp} is not allowed"
            )
            return {"status": "Rejected"}

        save_network_profile(slot, conn_data)
        # Arm an actual profile switch for the next Reset (TC_A_19 / TC_B_13):
        # a subsequent Reset without this flag must not swap ws_kwargs.
        self._pending_network_profile_switch = True
        logger.info(
            f"SetNetworkProfile: slot={slot} securityProfile={sp} ocppVersion={ocpp_version} url={url}"
        )
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
        # TC_P_01_CS (§P01.FR.05/FR.06): the CS supports no vendor-specific
        # DataTransfer messages, so any vendorId must yield UnknownVendorId
        # (or, if the vendor is known but messageId isn't, UnknownMessageId).
        vendor_id  = payload["vendorId"]
        message_id = payload.get("messageId", "")
        data       = payload.get("data")
        logger.info(f"DataTransfer: vendorId={vendor_id}, messageId={message_id}, data={data}")
        if vendor_id not in self._supported_data_transfer_vendors:
            return {"status": "UnknownVendorId"}
        known_messages = self._supported_data_transfer_vendors[vendor_id]
        if known_messages and message_id not in known_messages:
            return {"status": "UnknownMessageId"}
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
        """TC_N_27..33_CS: CustomerInformation (report / clear).

        - TC_N_27_CS: report=true + cached idToken → NotifyCustomerInformation
          with non-empty `data`.
        - TC_N_30_CS: report=true, clear=true → report THEN remove the
          idToken's traces from the authorization cache (LocalAuthList is
          preserved per §N10).
        - TC_N_32_CS: report=false, clear=true → still send one
          NotifyCustomerInformation indicating data was cleared.
        - TC_N_28/31_CS: no data available → NotifyCustomerInformation with
          empty `data` and tbc=false.
        """
        request_id    = payload["requestId"]
        report        = bool(payload.get("report", False))
        clear         = bool(payload.get("clear", False))
        id_token      = payload.get("idToken") or {}
        id_token_val  = id_token.get("idToken", "")
        cust_ident    = payload.get("customerIdentifier")
        cust_cert     = payload.get("customerCertificate")
        logger.info(
            f"CustomerInformation: requestId={request_id}, report={report}, "
            f"clear={clear}, idToken={id_token_val}, "
            f"customerIdentifier={cust_ident}"
        )

        # Collect the data BEFORE clearing so reports reflect what existed.
        data_str = self._build_customer_information_data(
            id_token=id_token_val,
            customer_identifier=cust_ident,
            customer_certificate=cust_cert,
        )

        if clear:
            removed = self._clear_customer_information(id_token=id_token_val)
            logger.info(
                f"CustomerInformation clear: removed {removed} cache entries "
                f"for idToken={id_token_val}"
            )

        # Per §N09/N10: respond Accepted and notify asynchronously. When
        # report=false AND clear=true, TC_N_32_CS still requires a single
        # NotifyCustomerInformation to confirm the clear operation — so fire
        # it here regardless, with empty data for the no-report/no-data paths.
        asyncio.create_task(
            self._send_notify_customer_information(request_id, data_str if report else "")
        )
        return {"status": "Accepted"}

    def _build_customer_information_data(
        self,
        id_token: str,
        customer_identifier: Optional[str],
        customer_certificate: Optional[Any],
    ) -> str:
        """Assemble a short plaintext report of what this CS knows about a
        customer. Empty string when nothing is known — which satisfies
        TC_N_28/31_CS (Accepted + no data).
        """
        parts: List[str] = []
        if id_token:
            cache_entry = self._auth_cache.get(id_token)
            if cache_entry:
                info = cache_entry.get("idTokenInfo", {}) or {}
                parts.append(
                    f"idToken={id_token}; cacheStatus={info.get('status', 'Unknown')}"
                )
            for entry in self.local_auth_list:
                tok = (entry.get("idToken") or {}).get("idToken")
                if tok == id_token:
                    info = entry.get("idTokenInfo", {}) or {}
                    parts.append(
                        f"idToken={id_token}; localListStatus={info.get('status', 'Unknown')}"
                    )
                    break
        if customer_identifier:
            parts.append(f"customerIdentifier={customer_identifier}")
        if customer_certificate:
            parts.append("customerCertificate=<present>")
        return "; ".join(parts)

    def _clear_customer_information(self, id_token: str) -> int:
        """Remove a specific idToken's traces from the authorization cache.

        Local Authorization List entries are preserved per §N10.FR.02 — they
        are managed via SendLocalList only.
        """
        if not id_token:
            return 0
        removed = 0
        if id_token in self._auth_cache:
            self._auth_cache.pop(id_token, None)
            save_auth_cache(self._auth_cache)
            removed += 1
        return removed

    async def _send_notify_customer_information(
        self, request_id: int, data: str = "",
    ) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            await self.ocpp_client.call("NotifyCustomerInformation", {
                "requestId": request_id,
                "data": data,
                "seqNo": 0,
                "generatedAt": now,
                "tbc": False,
            })
            logger.info(
                f"NotifyCustomerInformation sent for requestId={request_id}, "
                f"dataLen={len(data)}"
            )
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
