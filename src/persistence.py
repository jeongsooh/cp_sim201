"""
G5 — Device Model Persistence
ReadWrite 속성 변수의 변경을 data/device_model.json에 저장하고
재부팅 시 복원한다. [OCPP 2.0.1 Part 2 - 3.1.6]
"""
import json
import logging
import os
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DATA_DIR = os.path.join(_PROJECT_ROOT, "data")
_DEVICE_MODEL_FILE = os.path.join(_DATA_DIR, "device_model.json")
_NETWORK_PROFILES_FILE = os.path.join(_DATA_DIR, "network_profiles.json")
_CERT_METADATA_FILE = os.path.join(_DATA_DIR, "cert_metadata.json")
_ADMIN_STATE_FILE = os.path.join(_DATA_DIR, "admin_state.json")
_AUTH_CACHE_FILE = os.path.join(_DATA_DIR, "auth_cache.json")
_INSTALLED_CERTS_FILE = os.path.join(_DATA_DIR, "installed_certificates.json")


def _ensure_data_dir() -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)


def load_device_model(defaults: Dict[str, Dict[str, Tuple[str, str]]]) -> Dict[str, Dict[str, Tuple[str, str]]]:
    """재부팅 후 저장된 ReadWrite 값을 기본값 위에 덮어씌워 반환한다."""
    _ensure_data_dir()
    if not os.path.exists(_DEVICE_MODEL_FILE):
        return defaults

    try:
        with open(_DEVICE_MODEL_FILE, "r", encoding="utf-8") as f:
            saved: Dict[str, Dict[str, str]] = json.load(f)

        for comp, variables in saved.items():
            if comp not in defaults:
                continue
            for var, val in variables.items():
                if var in defaults[comp] and defaults[comp][var][1] == "ReadWrite":
                    defaults[comp][var] = (val, "ReadWrite")

        logger.info(f"Device model loaded from {_DEVICE_MODEL_FILE}")
    except Exception as e:
        logger.warning(f"Failed to load device model: {e}")

    return defaults


def save_device_model(model: Dict[str, Dict[str, Tuple[str, str]]]) -> None:
    """ReadWrite 변수만 추출해 JSON 파일로 저장한다."""
    _ensure_data_dir()
    to_save: Dict[str, Dict[str, str]] = {}
    for comp, variables in model.items():
        for var, (val, mutability) in variables.items():
            if mutability == "ReadWrite":
                to_save.setdefault(comp, {})[var] = val

    try:
        with open(_DEVICE_MODEL_FILE, "w", encoding="utf-8") as f:
            json.dump(to_save, f, indent=2)
        logger.debug(f"Device model saved to {_DEVICE_MODEL_FILE}")
    except Exception as e:
        logger.warning(f"Failed to save device model: {e}")


def load_network_profiles() -> Dict[str, Dict]:
    """SetNetworkProfile로 저장된 슬롯별 프로파일을 반환한다. [OCPP 2.0.1 B10]"""
    _ensure_data_dir()
    if not os.path.exists(_NETWORK_PROFILES_FILE):
        return {}
    try:
        with open(_NETWORK_PROFILES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("slots", {})
    except Exception as e:
        logger.warning(f"Failed to load network profiles: {e}")
        return {}


def save_network_profile(slot: int, profile: Dict) -> None:
    """단일 슬롯의 connectionData를 upsert 한다."""
    _ensure_data_dir()
    slots = load_network_profiles()
    slots[str(slot)] = profile
    try:
        with open(_NETWORK_PROFILES_FILE, "w", encoding="utf-8") as f:
            json.dump({"slots": slots}, f, indent=2)
        logger.info(f"Network profile slot {slot} saved to {_NETWORK_PROFILES_FILE}")
    except Exception as e:
        logger.warning(f"Failed to save network profile: {e}")


def load_cert_metadata() -> Dict:
    """CertificateSigned 수신 시점의 메타데이터(어느 CSMS URL에 유효한지 등)를 로드."""
    _ensure_data_dir()
    if not os.path.exists(_CERT_METADATA_FILE):
        return {}
    try:
        with open(_CERT_METADATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load cert metadata: {e}")
        return {}


def save_cert_metadata(metadata: Dict) -> None:
    _ensure_data_dir()
    try:
        with open(_CERT_METADATA_FILE, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Cert metadata saved to {_CERT_METADATA_FILE}")
    except Exception as e:
        logger.warning(f"Failed to save cert metadata: {e}")


def load_admin_state() -> Dict:
    """운영자/CSMS가 지정한 상태(AvailabilityState 등)를 로드.

    OCPP 2.0.1은 AvailabilityState를 ReadOnly로 정의하므로 device_model.json에
    저장되지 않는다. TC_B_23_CS 처럼 Inoperative 상태를 Reset 이후에도 유지해야
    할 때는 이 파일에서 복원한다.
    """
    _ensure_data_dir()
    if not os.path.exists(_ADMIN_STATE_FILE):
        return {}
    try:
        with open(_ADMIN_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load admin state: {e}")
        return {}


def save_admin_state(state: Dict) -> None:
    _ensure_data_dir()
    try:
        with open(_ADMIN_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        logger.info(f"Admin state saved to {_ADMIN_STATE_FILE}")
    except Exception as e:
        logger.warning(f"Failed to save admin state: {e}")


def load_auth_cache() -> Dict[str, Dict]:
    """TC_C_32_CS: 재부팅 후에도 유지되는 AuthorizeRequest 캐시.

    key: idToken 문자열, value: {"idTokenInfo": {...}, "stored_at": epoch seconds}
    """
    _ensure_data_dir()
    if not os.path.exists(_AUTH_CACHE_FILE):
        return {}
    try:
        with open(_AUTH_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load auth cache: {e}")
        return {}


def save_auth_cache(cache: Dict[str, Dict]) -> None:
    _ensure_data_dir()
    try:
        with open(_AUTH_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
        logger.debug(f"Auth cache saved to {_AUTH_CACHE_FILE} ({len(cache)} entries)")
    except Exception as e:
        logger.warning(f"Failed to save auth cache: {e}")


def load_installed_certificates() -> Dict[str, Dict]:
    """TC_M_23_CS: persist InstallCertificate-installed CA certs across
    service restarts so DeleteCertificate can target a previously installed
    cert even after a reboot."""
    _ensure_data_dir()
    if not os.path.exists(_INSTALLED_CERTS_FILE):
        return {}
    try:
        with open(_INSTALLED_CERTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load installed certificates: {e}")
        return {}


def save_installed_certificates(certs: Dict[str, Dict]) -> None:
    _ensure_data_dir()
    try:
        with open(_INSTALLED_CERTS_FILE, "w", encoding="utf-8") as f:
            json.dump(certs, f, indent=2)
        logger.debug(
            f"Installed certificates saved to {_INSTALLED_CERTS_FILE} "
            f"({len(certs)} entries)"
        )
    except Exception as e:
        logger.warning(f"Failed to save installed certificates: {e}")
