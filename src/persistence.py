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
