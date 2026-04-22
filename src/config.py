import os

class OCPPConfig:
    # 1. OCPP-J 전송 설정
    MESSAGE_TYPE_CALL = 2
    MESSAGE_TYPE_RESULT = 3
    MESSAGE_TYPE_ERROR = 4
    WEBSOCKET_SUBPROTOCOL = "ocpp2.0.1"
    MESSAGE_ID_MAX_LENGTH = 36

    # 2. 보안 설정 (Security Profile 2 & 3)
    TLS_VERSION_MIN = "TLSv1.2"
    RSA_KEY_MIN_LENGTH = 2048
    ECC_KEY_MIN_LENGTH = 224
    BASIC_AUTH_PW_LEN_MIN = 16
    BASIC_AUTH_PW_LEN_MAX = 40

    # 3. 네트워크 재연결 (Retry Logic)
    # Low minimum + small jitter so the first retry hits OCTT's invalid-cert window
    # (OCTT presents the bad cert for ~8-10s; 5s min + up to 10s jitter was too slow)
    RETRY_BACKOFF_WAIT_MINIMUM = 2      # Seconds
    RETRY_BACKOFF_RANDOM_RANGE = 3      # Seconds
    RETRY_BACKOFF_REPEAT_TIMES = 10     # Max Doubling steps

    # 4. 트랜잭션 설정
    OFFLINE_THRESHOLD = 60              # Seconds
    TX_START_POINTS = ["Authorized", "EVConnected"]
    TX_STOP_POINTS = ["Authorized", "EVConnected"]

class ACChargingConfig:
    # 1. AC 전용 측정 단위 (Amperes 권장)
    CHARGING_RATE_UNIT = "A" 
    # 2. 공급 상 수 (Supply Phases)
    SUPPLY_PHASES = 3 
    # 3. AC 측정 데이터 (Meter Values)
    MEASURANDS = [
        "Current.Import",               # 전류 (RMS AC Amperes)
        "Voltage",                      # 전압 (RMS AC Voltage)
        "Energy.Active.Import.Register" # 누적 에너지 (Wh/kWh)
    ]
    # 4. 트랜잭션 시작점
    TX_START_POINT = ["PowerPathClosed", "Authorized"] 

class RPCErrorCodes:
    FORMAT_VIOLATION = "FormatViolation"
    NOT_SUPPORTED = "NotSupported"
    PROTOCOL_ERROR = "ProtocolError"
    SECURITY_ERROR = "SecurityError"
    TYPE_CONSTRAINT_VIOLATION = "TypeConstraintViolation"
    INTERNAL_ERROR = "InternalError"
