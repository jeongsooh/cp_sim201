import json
import ssl
import base64
import logging
import os
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class _NoWildcardSSLObject:
    """Wraps ssl.SSLObject to reject wildcard server certificates at handshake time.

    asyncio calls wrap_bio() → do_handshake() on the returned object.
    We raise after the real handshake completes but before the WebSocket upgrade,
    so OCTT never records "Successfully connected".
    """
    __slots__ = ('_obj',)

    def __init__(self, obj: ssl.SSLObject) -> None:
        object.__setattr__(self, '_obj', obj)

    def do_handshake(self) -> None:
        obj = object.__getattribute__(self, '_obj')
        obj.do_handshake()
        cert = obj.getpeercert()
        if cert:
            for san_type, san_value in cert.get('subjectAltName', []):
                if san_type == 'DNS' and '*' in san_value:
                    raise ssl.SSLCertVerificationError(
                        f"CERTIFICATE_VERIFY_FAILED: wildcard certificate not allowed (SAN): {san_value}"
                    )
            for rdns in cert.get('subject', ()):
                for name_type, name_value in rdns:
                    if name_type == 'commonName' and '*' in name_value:
                        raise ssl.SSLCertVerificationError(
                            f"CERTIFICATE_VERIFY_FAILED: wildcard certificate not allowed (CN): {name_value}"
                        )

    def __getattr__(self, name: str):
        return getattr(object.__getattribute__(self, '_obj'), name)

    def __setattr__(self, name: str, value) -> None:
        setattr(object.__getattribute__(self, '_obj'), name, value)


class _NoWildcardSSLContext(ssl.SSLContext):
    """SSL context whose wrap_bio() returns a wildcard-rejecting SSLObject."""

    def wrap_bio(self, incoming, outgoing, server_side=False, server_hostname=None, session=None):
        obj = super().wrap_bio(
            incoming, outgoing,
            server_side=server_side,
            server_hostname=server_hostname,
            session=session,
        )
        return _NoWildcardSSLObject(obj)

_DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "station_config.json",
)

# Shadow copy of the boot CA. OCTT runs cert-management cases that
# DeleteCertificate the on-disk CSMSRootCertificate.pem (which is also
# station_config.tls.ca_cert) and may then trigger Reset(Immediate) —
# the resulting systemd-driven daemon restart can't find the primary
# file and used to crash-loop. Every successful primary load refreshes
# this shadow, and a missing primary triggers fallback to the shadow.
# Lives under data/ which is outside the OCPP-managed cert_dir.
_BOOT_CA_SHADOW = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "boot_ca.pem",
)


def _load_ca_pem(ca_cert_path: str) -> str:
    """Resolve a usable CA PEM from the configured path with shadow fallback.

    Returns the PEM content as a string. Raises FileNotFoundError only when
    neither the configured path nor the shadow holds a readable cert.
    """
    if os.path.exists(ca_cert_path):
        with open(ca_cert_path, "r", encoding="utf-8") as f:
            pem = f.read()
        try:
            os.makedirs(os.path.dirname(_BOOT_CA_SHADOW), exist_ok=True)
            with open(_BOOT_CA_SHADOW, "w", encoding="utf-8") as f:
                f.write(pem)
        except OSError as e:
            logger.warning(f"Failed to refresh boot CA shadow {_BOOT_CA_SHADOW}: {e}")
        return pem
    if os.path.exists(_BOOT_CA_SHADOW):
        logger.warning(
            f"Primary ca_cert {ca_cert_path} missing — "
            f"falling back to boot CA shadow {_BOOT_CA_SHADOW}"
        )
        with open(_BOOT_CA_SHADOW, "r", encoding="utf-8") as f:
            return f.read()
    raise FileNotFoundError(
        f"ca_cert '{ca_cert_path}' and boot CA shadow "
        f"'{_BOOT_CA_SHADOW}' both missing — cannot build SSL context"
    )


def _build_ssl_context(
    security_profile: int,
    ca_cert: str,
    client_cert: str,
    client_key: str,
    ca_cert_data: Optional[str] = None,
) -> ssl.SSLContext:
    """Build an SSL context for OCPP Profile 2/3 connections.

    ca_cert_data, when given, takes precedence over ca_cert (a filesystem
    path). This lets callers anchor trust on an in-memory PEM cached at
    boot, immune to OCPP runtime cert lifecycle: TC_M_23_CS exposed that
    OCTT issues DeleteCertificate on the CSMSRootCertificate moments
    before CertificateSigned forces an SSL context rebuild, so the file
    path read at rebuild time can hit FileNotFoundError. The in-memory
    PEM keeps rebuilds deterministic regardless of disk state.
    """
    ctx = _NoWildcardSSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    # OCTT (post-Phase-1) advertises only TLS 1.2 ciphers in its cipher
    # suite list (TLS_ECDHE_ECDSA_* without the TLS_AES_*_SHA* TLS 1.3
    # entries that appear during the initial Phase 1 setup). Offering
    # TLS 1.3 in our ClientHello during the Phase 2 acceptance window
    # makes the listener RST the handshake instead of negotiating down.
    # Cap our offered range at TLS 1.2 so the ClientHello matches what
    # the OCTT acceptance listener actually parses. OCPP 2.0.1 mandates
    # TLSv1.2+, doesn't require TLS 1.3, so this is a spec-safe
    # workaround for the OCTT quirk surfaced in TC_A_06_CS Phase 2.
    ctx.maximum_version = ssl.TLSVersion.TLSv1_2

    if ca_cert_data:
        ctx.load_verify_locations(cadata=ca_cert_data)
    elif ca_cert:
        # _load_ca_pem handles the OCTT delete-cycle case where the
        # primary file is gone post-restart: it falls back to a
        # shadow copy refreshed on every successful primary load.
        ctx.load_verify_locations(cadata=_load_ca_pem(ca_cert))
    else:
        ctx.load_default_certs()

    if security_profile == 3:
        if not (client_cert and client_key):
            raise StationConfigError(
                "security_profile 3 requires client_cert and client_key paths"
            )
        ctx.load_cert_chain(certfile=client_cert, keyfile=client_key)

    return ctx


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
        # ChargingStation.FirmwareVersion reported in BootNotification.
        # OCTT compares this against the PICS-declared value; mismatch
        # surfaces as "Reported Firmware Version ... does not match value
        # from PICS" (TC_L_02_CS log).
        self.firmware_version: str = str(data.get("firmware_version", "2.1.1"))
        # Key algorithm used by _generate_csr_pem when responding to a CSMS
        # TriggerMessage(SignChargingStationCertificate). OCTT's "configured
        # security algorithm" must match — sending an RSA CSR to an
        # ECDSA-configured OCTT yields Rejected with an "algorithm mismatch"
        # log line.
        self.signature_algorithm: str = str(
            data.get("signature_algorithm", "RSA")
        ).upper()

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
        # serial_number is a free-form identifier (manufacturing serial, OCPP
        # station id, or anything in between) — its only spec-relevant
        # constraint is that it matches the client certificate's commonName
        # so OCTT TC_A_07_CS's BootNotification.serialNumber vs cert CN
        # check passes. Reject only empty / overly long values.
        if not self.serial_number or len(self.serial_number) > 25:
            raise StationConfigError(
                f"serial_number must be 1-25 characters, got: '{self.serial_number}'"
            )
        if not self.station_id:
            raise StationConfigError("station_id is required")
        if not self.csms_url:
            raise StationConfigError("csms_url is required")
        if self.security_profile not in (0, 1, 2, 3):
            raise StationConfigError(
                f"security_profile must be 0, 1, 2, or 3, got: {self.security_profile}"
            )
        if self.signature_algorithm not in ("RSA", "ECDSA"):
            raise StationConfigError(
                f"signature_algorithm must be 'RSA' or 'ECDSA', got: {self.signature_algorithm!r}"
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
        return _build_ssl_context(
            security_profile=self.security_profile,
            ca_cert=self.ca_cert,
            client_cert=self.client_cert,
            client_key=self.client_key,
        )

    @staticmethod
    def build_ws_kwargs_from_profile(
        profile: Dict[str, Any],
        cert_dir: str,
        ca_cert: str,
        ca_cert_data: Optional[str] = None,
    ) -> Dict[str, Any]:
        """SetNetworkProfile로 수신한 connectionData에서 websockets.connect() kwargs를 생성한다.

        profile schema는 OCPP SetNetworkProfileRequest의 connectionData 일부:
          - ocppCsmsUrl: str
          - securityProfile: int
          - basicAuth (optional): {"user": str, "password": str}

        CSR/CertificateSigned로 저장된 기본 client cert 경로는
        cert_dir/client.crt, cert_dir/client.key를 사용한다 (Profile 3).

        ca_cert_data: in-memory CA PEM. When provided, takes precedence
        over ca_cert (filesystem path) — see _build_ssl_context docstring
        for the TC_M_23_CS rationale.
        """
        sp = int(profile.get("securityProfile", 0))
        kwargs: Dict[str, Any] = {}

        if sp in (2, 3):
            client_cert = os.path.join(cert_dir, "client.crt") if sp == 3 else ""
            client_key  = os.path.join(cert_dir, "client.key") if sp == 3 else ""
            kwargs["ssl"] = _build_ssl_context(
                security_profile=sp,
                ca_cert=ca_cert,
                client_cert=client_cert,
                client_key=client_key,
                ca_cert_data=ca_cert_data,
            )

        if sp in (1, 2):
            auth = profile.get("basicAuth") or {}
            user = auth.get("user", "")
            pw   = auth.get("password", "")
            if user and pw:
                credentials = base64.b64encode(f"{user}:{pw}".encode()).decode()
                kwargs["additional_headers"] = {
                    "Authorization": f"Basic {credentials}"
                }

        return kwargs

    def __repr__(self) -> str:
        return (
            f"StationConfig(serial={self.serial_number}, "
            f"station_id={self.station_id}, "
            f"profile={self.security_profile}, "
            f"firmware={self.firmware_version}, "
            f"sig_alg={self.signature_algorithm}, "
            f"url={self.csms_url})"
        )
