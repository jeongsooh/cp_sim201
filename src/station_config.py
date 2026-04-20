import json
import ssl
import base64
import logging
import os
from typing import Dict, Any

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "station_config.json",
)


class StationConfigError(Exception):
    pass


class StationConfig:
    def __init__(self, path: str = _DEFAULT_CONFIG_PATH) -> None:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.serial_number: str = str(data.get("serial_number", ""))
        self.station_id: str    = data.get("station_id", "")
        self.csms_url: str      = data.get("csms_url", "")
        self.security_profile: int = int(data.get("security_profile", 0))

        auth = data.get("basic_auth", {})
        self.basic_auth_user: str     = auth.get("user", "")
        self.basic_auth_password: str = auth.get("password", "")

        tls = data.get("tls", {})
        self.cert_dir: str     = tls.get("cert_dir", "/etc/cp_sim201/certs")
        self.ca_cert: str      = tls.get("ca_cert", "")
        self.client_cert: str  = tls.get("client_cert", "")
        self.client_key: str   = tls.get("client_key", "")

        self._validate()

    def _validate(self) -> None:
        if not self.serial_number.isdigit() or len(self.serial_number) != 6:
            raise StationConfigError(
                f"serial_number must be exactly 6 digits, got: '{self.serial_number}'"
            )
        if not self.station_id:
            raise StationConfigError("station_id is required")
        if not self.csms_url:
            raise StationConfigError("csms_url is required")
        if self.security_profile not in (0, 1, 2, 3):
            raise StationConfigError(
                f"security_profile must be 0, 1, 2, or 3, got: {self.security_profile}"
            )

        is_tls = self.csms_url.startswith("wss://")
        if self.security_profile in (2, 3) and not is_tls:
            raise StationConfigError(
                f"security_profile {self.security_profile} requires wss:// URL"
            )
        if self.security_profile in (1, 2) and not (
            self.basic_auth_user and self.basic_auth_password
        ):
            raise StationConfigError(
                f"security_profile {self.security_profile} requires basic_auth user and password"
            )
        if self.security_profile == 3 and not (self.client_cert and self.client_key):
            raise StationConfigError(
                "security_profile 3 requires tls.client_cert and tls.client_key"
            )

    def build_ws_kwargs(self) -> Dict[str, Any]:
        """websockets.connect()에 전달할 키워드 인자를 반환한다."""
        kwargs: Dict[str, Any] = {}

        if self.security_profile in (2, 3):
            kwargs["ssl"] = self._build_ssl_context()

        if self.security_profile in (1, 2):
            credentials = base64.b64encode(
                f"{self.basic_auth_user}:{self.basic_auth_password}".encode()
            ).decode()
            kwargs["additional_headers"] = {
                "Authorization": f"Basic {credentials}"
            }

        return kwargs

    def _build_ssl_context(self) -> ssl.SSLContext:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2

        if self.ca_cert:
            ctx.load_verify_locations(self.ca_cert)
        else:
            ctx.load_default_certs()

        if self.security_profile == 3:
            ctx.load_cert_chain(certfile=self.client_cert, keyfile=self.client_key)

        return ctx

    def __repr__(self) -> str:
        return (
            f"StationConfig(serial={self.serial_number}, "
            f"station_id={self.station_id}, "
            f"profile={self.security_profile}, "
            f"url={self.csms_url})"
        )
