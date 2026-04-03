import logging
from typing import Optional

logger = logging.getLogger(__name__)

class HardwareAPI:
    """
    물리 장치(GPIO, 센서, 릴레이 등)와의 연동을 추상화하기 위한 더미 API 클래스입니다.
    추후 실제 라즈베리파이나 임베디드 보드의 제어 인터페이스로 교체됩니다.
    """
    @staticmethod
    def check_proximity(connector_id: int) -> bool:
        # PP(Proximity Pilot) 저항 측정 로직 대체
        return False
    
    @staticmethod
    def get_relay_status(evse_id: int) -> bool:
        # 실제 릴레이 접점이 닫혀있는지 확인
        return False

    @staticmethod
    def relay_on(evse_id: int):
        # 릴레이 전원 인가 (MC Close)
        pass

    @staticmethod
    def relay_off(evse_id: int):
        # 릴레이 전원 차단 (MC Open)
        pass

    @staticmethod
    def set_cp_pwm(evse_id: int, duty_percent: int):
        # Control Pilot PWM 동적 제어
        pass
        
    @staticmethod
    def read_cp_adc(evse_id: int) -> int:
        # Control Pilot ADC raw measurement 측정
        return 0


class ConnectorHAL:
    def __init__(self, evse_id: int, connector_id: int, ocpp_client=None):
        self.evse_id = evse_id
        self.connector_id = connector_id
        self.ocpp_client = ocpp_client
        self.component_name = "Connector"
        self.status = None

    def read_physical_connection(self) -> str:
        """물리 센서값을 읽어 상태(Available / Occupied) 반환"""
        is_plugged = HardwareAPI.check_proximity(self.connector_id)
        return "Occupied" if is_plugged else "Available"

    async def on_status_change(self):
        """이벤트: 플러그 체결 상태가 변경되었을 때 호출"""
        new_status = self.read_physical_connection()
        if new_status != self.status:
            self.status = new_status
            if self.ocpp_client:
                payload = {
                    # Note: 실제 환경에선 ISO8601 타임스탬프 동적 생성 필요
                    "timestamp": "2026-04-02T12:00:00Z",
                    "connectorStatus": self.status,
                    "evseId": self.evse_id,
                    "connectorId": self.connector_id
                }
                logger.info(f"Connector {self.connector_id} status changed to {self.status}")
                await self.ocpp_client.call("StatusNotification", payload)


class TokenReaderHAL:
    def __init__(self, ocpp_client=None):
        self.component_name = "TokenReader"
        self.ocpp_client = ocpp_client
        self.enabled = True

    async def on_rfid_scanned(self, raw_data: str):
        """이벤트: RFID 리더기에서 태그 데이터가 읽혔을 때 호출"""
        if not self.enabled:
            logger.warning("TokenReader is disabled. Ignoring scan.")
            return

        id_token = {
            "idToken": raw_data,
            "type": "ISO14443"
        }
        logger.info(f"RFID Scanned: {raw_data}. Requesting Authorization.")
        if self.ocpp_client:
            await self.ocpp_client.call("Authorize", {"idToken": id_token})

    def set_enabled(self, status: bool):
        """OCPP 통신에 의해 리더기의 활성 여부 제어 (SetVariables)"""
        self.enabled = status
        logger.debug(f"TokenReader enabled status set to: {self.enabled}")


class PowerContactorHAL:
    def __init__(self, evse_id: int, ocpp_client=None):
        self.evse_id = evse_id
        self.component_name = "PowerContactor"
        self.ocpp_client = ocpp_client

    def control_relay(self, action: str):
        """action: 'Close' (전원 공급), 'Open' (공급 차단)"""
        if action == "Close":
            HardwareAPI.relay_on(self.evse_id)
            logger.info(f"Power Contactor for EVSE {self.evse_id} is CLOSED (Power ON)")
        else:
            HardwareAPI.relay_off(self.evse_id)
            logger.info(f"Power Contactor for EVSE {self.evse_id} is OPEN (Power OFF)")

    def get_actual_active_state(self) -> bool:
        """물리적으로 릴레이 접점이 붙어 전류가 흐를 수 있는지 측정"""
        return HardwareAPI.get_relay_status(self.evse_id)

    def read_cp_voltage(self) -> int:
        """ADC 측정값으로 State 판별용 (ex. State C = ~36500)"""
        return HardwareAPI.read_cp_adc(self.evse_id)

    def set_pwm_duty(self, duty_percent: int):
        HardwareAPI.set_cp_pwm(self.evse_id, duty_percent)
        logger.info(f"Control Pilot PWM on EVSE {self.evse_id} set to {duty_percent}%")
