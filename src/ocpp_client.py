import asyncio
import json
import logging
import time
import uuid
import random
import os
import ssl
import websockets
from websockets.exceptions import ConnectionClosed
from typing import Dict, Any, Optional, Callable, Awaitable, Tuple
from jsonschema import validate, ValidationError

from .config import OCPPConfig, RPCErrorCodes
from .message_queue import OfflineMessageQueue

logger = logging.getLogger(__name__)


class WaitQueueError(Exception):
    pass


class OCPPClient:
    def __init__(
        self,
        station_id: str,
        server_url: str,
        ws_kwargs: Optional[dict] = None,
    ) -> None:
        self.station_id = station_id
        self.server_url = server_url if server_url.endswith("/") else server_url + "/"
        self.uri = f"{self.server_url}{self.station_id}"
        self._ws_kwargs = ws_kwargs or {}

        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._pending_calls: Dict[str, asyncio.Future] = {}
        self._pending_actions: Dict[str, str] = {}
        # OCPP 2.0.1 §4.1: only one CALL may be outstanding from a given side
        # at a time. Serialize outgoing CS->CSMS CALLs so that meter-value
        # TransactionEvents, StatusNotifications, etc. can't race.
        # CSMS-initiated CALLs (handled via _send_result/_send_error) are
        # unaffected — they don't go through this lock.
        self._call_lock = asyncio.Lock()
        self._listen_task: Optional[asyncio.Task] = None
        self._is_running = False

        self._action_handlers: Dict[str, Callable[[Dict], Awaitable[Dict]]] = {}
        self._on_connect_callback: Optional[Callable[[], Awaitable[None]]] = None
        # Pre-dispatch gate: given an incoming CSMS action name, returns None to
        # allow, or (error_code, error_desc) to reject with a CALLERROR.
        # Used for OCPP 2.0.1 §B02/B03 — SecurityError gating while in
        # Pending/Rejected boot state.
        self._message_gate: Optional[Callable[[str], Optional[Tuple[str, str]]]] = None
        # Reconnect backoff — provider returns (wait_min_s, random_range_s,
        # repeat_times) from OCPPCommCtrlr device-model variables, which the
        # CSMS may change at runtime. Without a provider, the static
        # OCPPConfig defaults are used.
        self._retry_config_provider: Optional[Callable[[], Tuple[int, int, int]]] = None
        # TC_E_41/E_42/E_50/E_51_CS: TransactionEvent retry uses a different
        # OCPP 2.0.1 §E13 mechanism (MessageAttempts + MessageAttemptInterval,
        # instance=TransactionEvent). Provider returns (attempts, interval).
        self._tx_retry_config_provider: Optional[Callable[[], Tuple[int, int]]] = None
        # TC_B_46_CS: after N consecutive failed connection attempts on the
        # current slot, let the controller advance to the next slot in
        # NetworkConfigurationPriority. Handler is invoked with the running
        # failure count; it may call update_connection() to swap ws_kwargs.
        self._connection_failure_handler: Optional[Callable[[int], None]] = None
        self._consecutive_failures: int = 0
        # TC_A_06_CS: when the controller closes the WS deliberately (e.g.
        # for a Reset), the immediate reconnect should not pay the full
        # RetryBackOffWaitMinimum penalty — that backoff is for *failed*
        # attempts, not intentional cycle-the-connection cases. The
        # controller sets this just before ws.close(); the connect loop
        # consumes it on the next iteration.
        self._skip_next_reconnect_wait: bool = False
        # TC_A_06_CS: one-shot bounded retry on a TLS-protocol-version error.
        # After the first SSL/TLS-version rejection, fire the next attempt
        # at exactly wait_min (not 0, not wait_min*2). Today's OCTT runs a
        # Phase 1 TLSv1.1 listener for ~65s and a Phase 2 TLSv1.2 listener
        # that comes up only briefly afterwards — attempt 2 needs to land
        # in that narrow Phase 2 window. wait_min=90 puts attempt 2 at
        # ~T+90s, just past the 65s TLSv1.1 boundary.
        self._tls_protocol_retry_done: bool = False
        # TC_A_05_CS: one-shot fast retry on a CSMS-cert verification failure.
        # OCTT presents an expired cert for only ~3s and then reverts to a
        # valid one, leaving the CS ~95s to reconnect before the test times
        # out. attempt=1 → step=1 → wait_min*2 (=180s with wait_min=90) would
        # miss that window, so collapse the *next* retry to 0 once. Reset
        # on every successful connect.
        self._cert_error_retry_done: bool = False
        # TC_A_05 vs TC_B_51/52 trade-off: optional override predicate consulted
        # on the first reconnect attempt after a clean WS close. Default None
        # → use the connection-age heuristic in _should_fast_retry_on_clean_drop.
        self._clean_drop_fast_retry_provider: Optional[Callable[[], bool]] = None
        # TC_A_05_CS retry-burst window: when the clean-drop fast retry fires
        # we stay in 1s-retry mode for a short window so subsequent failures
        # (OSError while OCTT is mid cert-swap, repeat cert_errors after the
        # one-shot _cert_error_retry_done is consumed) don't fall back to
        # exponential and miss OCTT's ~3s bad-cert phase.
        self._fast_retry_until: float = 0.0
        # Monotonic timestamp of the most recent successful connect. Used to
        # detect cert-recovery pattern: a clean drop very soon (<10s) after
        # connect almost always means CSMS is bouncing the WS as part of a
        # cert flow (TC_A_05 round 2) — fast retry. A clean drop after a
        # long-lived connection (TC_B_51 / TC_B_52 prep + offline test)
        # is the spec-compliant wait_min case.
        self._last_connect_time: float = 0.0
        # TC_B_57_CS distinction: TC_B_57's back-to-back disconnect probe also
        # has connection_age < 10s but is NOT a cert-recovery context — it
        # explicitly verifies wait_min is honoured. _last_connect_time alone
        # can't tell them apart. Adding "was there a cert error recently?"
        # narrows fast retry to the actual TC_A_05 pattern.
        self._last_cert_error_time: float = 0.0
        self._schemas = self._load_schemas()
        self.offline_queue = OfflineMessageQueue()
        self.tls_cert_error_occurred = False
        # TC_A_06_CS step 14/16: after a TLSv1.1 (or any sub-1.2) Server Hello
        # is rejected, the CS must report SecurityEventNotification of type
        # InvalidTLSVersion on the next successful reconnect. The controller
        # consumes-and-clears this flag in _on_reconnect, the same way it
        # handles tls_cert_error_occurred for InvalidCsmsCertificate.
        self.tls_protocol_error_occurred = False
        # TC_C_16_CS: when drained offline messages get a response, route it
        # back to the controller so per-action post-processing (e.g.
        # TransactionEvent idTokenInfo → deauth-stop / cache update) runs just
        # as it would on a live call.
        self._replay_response_hook: Optional[
            Callable[[str, Dict[str, Any], Dict[str, Any]], Awaitable[None]]
        ] = None

    def set_replay_response_hook(
        self,
        hook: Callable[[str, Dict[str, Any], Dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._replay_response_hook = hook

    def _load_schemas(self) -> Dict[str, dict]:
        schema_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schemas")
        schemas: Dict[str, dict] = {}
        if not os.path.isdir(schema_dir):
            logger.warning(f"Schema directory not found: {schema_dir}")
            return schemas

        for file in os.listdir(schema_dir):
            if file.endswith(".json"):
                path = os.path.join(schema_dir, file)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        schemas[file] = json.load(f)
                except Exception as e:
                    logger.error(f"Failed to parse schema {file}: {e}")
        return schemas

    def _validate_payload(self, schema_name: str, payload: dict) -> None:
        if schema_name not in self._schemas:
            logger.warning(f"No schema found for {schema_name}, skipping validation.")
            return
        try:
            validate(instance=payload, schema=self._schemas[schema_name])
        except ValidationError as e:
            # TypeError/path 정보로 FormatViolation vs TypeConstraintViolation 구분
            if e.validator in ("type", "enum"):
                raise ValueError(
                    f"{RPCErrorCodes.TYPE_CONSTRAINT_VIOLATION}: {e.message}"
                )
            raise ValueError(
                f"{RPCErrorCodes.FORMAT_VIOLATION}: {e.message}"
            )

    def register_action_handler(
        self, action: str, handler: Callable[[Dict], Awaitable[Dict]]
    ) -> None:
        self._action_handlers[action] = handler

    def describe_ws_kwargs(self) -> str:
        """Return a single-line, grep-friendly summary of what the current
        ws_kwargs will present to the CSMS during the next handshake.

        Profile-mismatch bugs (CS thinks it's Profile 3 mTLS while CSMS
        expects Profile 2 Basic Auth, or vice versa) surface as opaque
        HTTP 401 / ConnectionResetError without context. Logging this
        before every connect attempt makes the mismatch visible in
        journalctl ``security_profile=`` grep.
        """
        has_ssl = "ssl" in self._ws_kwargs
        headers = self._ws_kwargs.get("additional_headers") or {}
        has_basic_auth = "Authorization" in headers
        # Profile inference is heuristic — the actual SecurityProfile is in
        # device_model. ws_kwargs only tells us what we present on the wire.
        if has_ssl and has_basic_auth:
            likely = 2
        elif has_ssl and not has_basic_auth:
            likely = 3
        elif has_basic_auth:
            likely = 1
        else:
            likely = 0
        return (
            f"likely_security_profile={likely} ssl={has_ssl} "
            f"basic_auth={has_basic_auth}"
        )

    def update_connection(self, server_url: str, ws_kwargs: Dict[str, Any]) -> None:
        """다음 재연결부터 적용될 서버 URL과 websockets.connect kwargs를 교체한다.

        SetNetworkProfile + Reset으로 보안 프로파일이 바뀔 때 호출한다.
        현재 연결을 닫지는 않으므로, 호출 후 별도로 self.ws.close()를 수행해야
        재연결 루프가 새 설정으로 접속한다.
        """
        self.server_url = server_url if server_url.endswith("/") else server_url + "/"
        self.uri = f"{self.server_url}{self.station_id}"
        self._ws_kwargs = ws_kwargs
        # New slot starts with a fresh attempt count so the fallback threshold
        # applies per-slot (TC_B_46_CS).
        self._consecutive_failures = 0
        logger.info(
            f"Connection settings updated — next reconnect will use {self.uri} "
            f"({self.describe_ws_kwargs()})"
        )

    def register_on_connect(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._on_connect_callback = callback

    def set_message_gate(
        self, gate: Optional[Callable[[str], Optional[Tuple[str, str]]]]
    ) -> None:
        self._message_gate = gate

    def set_retry_config_provider(
        self, provider: Optional[Callable[[], Tuple[int, int, int]]]
    ) -> None:
        self._retry_config_provider = provider

    def set_tx_retry_config_provider(
        self, provider: Optional[Callable[[], Tuple[int, int, int]]]
    ) -> None:
        """TC_E_41_CS: provider returns (MessageAttempts, MessageAttemptInterval,
        MessageTimeout) for TransactionEvent — read from OCPPCommCtrlr
        instance=TransactionEvent / instance=Default.
        """
        self._tx_retry_config_provider = provider

    def _tx_retry_config(self) -> Tuple[int, int, int]:
        if self._tx_retry_config_provider is not None:
            try:
                res = self._tx_retry_config_provider()
                if len(res) == 2:
                    # Backwards-compat: old provider only returned 2.
                    return (res[0], res[1], 30)
                return res
            except Exception as e:
                logger.warning(
                    f"tx_retry_config_provider failed, using defaults: {e}"
                )
        return (3, 60, 30)

    def set_connection_failure_handler(
        self, handler: Optional[Callable[[int], None]]
    ) -> None:
        self._connection_failure_handler = handler

    def set_clean_drop_fast_retry_provider(
        self, provider: Optional[Callable[[], bool]]
    ) -> None:
        """Predicate consulted on the first reconnect after a clean WS close.
        Return True to retry immediately (TC_A_05 cert-window scenario),
        False to honour wait_min (TC_B_51 offline-window scenario)."""
        self._clean_drop_fast_retry_provider = provider

    def _should_fast_retry_on_clean_drop(self) -> bool:
        """Decide whether the first reconnect after a clean WS close should
        be immediate (TC_A_05 cert-recovery context) or honour wait_min
        (TC_B_51 / TC_B_52 offline-period tests, TC_B_57 back-to-back probe).

        Heuristic — fast retry requires BOTH:
        - connection_age < 10s: the close came right after a fresh connect
          (TC_A_05 second close pattern, OCTT swaps cert ~2s after the
          intermediate reconnect)
        - last cert error < 10s ago: we're in active cert-recovery context.
          Without this, TC_B_57's back-to-back probe (also short-lived
          connection but no cert errors) would falsely trigger fast retry
          and break the wait_min verification.

        Both must be true together. Long-lived stable connection (TC_B_51/52)
        fails the first check. Short-lived without cert history (TC_B_57)
        fails the second. Only the cert-recovery sequence satisfies both.

        An optional provider can override this — controller wires None by
        default which leaves the heuristic in charge.
        """
        if self._clean_drop_fast_retry_provider is not None:
            try:
                return bool(self._clean_drop_fast_retry_provider())
            except Exception as e:
                logger.warning(
                    f"clean_drop_fast_retry_provider failed, falling back "
                    f"to connection-age heuristic: {e}"
                )
        if self._last_connect_time <= 0.0 or self._last_cert_error_time <= 0.0:
            return False
        now = time.monotonic()
        connection_age = now - self._last_connect_time
        cert_error_age = now - self._last_cert_error_time
        return connection_age < 10.0 and cert_error_age < 10.0

    def _retry_config(self) -> Tuple[int, int, int]:
        if self._retry_config_provider is not None:
            try:
                return self._retry_config_provider()
            except Exception as e:
                logger.warning(f"retry_config_provider failed, using defaults: {e}")
        return (
            OCPPConfig.RETRY_BACKOFF_WAIT_MINIMUM,
            OCPPConfig.RETRY_BACKOFF_RANDOM_RANGE,
            OCPPConfig.RETRY_BACKOFF_REPEAT_TIMES,
        )

    async def connect(self) -> None:
        self._is_running = True
        attempt = 0
        while self._is_running:
            try:
                logger.info(
                    f"Attempting connection to {self.uri} "
                    f"({self.describe_ws_kwargs()})"
                )
                self.ws = await websockets.connect(
                    self.uri,
                    subprotocols=[OCPPConfig.WEBSOCKET_SUBPROTOCOL],
                    **self._ws_kwargs,
                )
                logger.info("Connection established.")
                attempt = 0
                self._consecutive_failures = 0
                self._tls_protocol_retry_done = False
                self._cert_error_retry_done = False
                self._fast_retry_until = 0.0
                self._last_connect_time = time.monotonic()
                if self._on_connect_callback:
                    asyncio.create_task(self._on_connect_callback())
                # G4: 재연결 직후 오프라인 큐 재전송
                asyncio.create_task(self._drain_offline_queue())
                self._listen_task = asyncio.create_task(self._listen())
                await self._listen_task
            except (ConnectionClosed, ConnectionRefusedError, OSError,
                    websockets.exceptions.InvalidStatus) as e:
                e_str = str(e)
                # TC_A_05_CS / TC_B_57_CS distinction: ConnectionClosed means
                # the WS was live and got dropped (CSMS-initiated close or
                # network-level drop after handshake). InvalidStatus /
                # ConnectionRefused / OSError mean the connection never
                # established (HTTP reject, TCP reject, TLS handshake
                # failure). Only the "was live" case warrants an immediate
                # first retry — never-established failures must back off.
                is_clean_drop = isinstance(e, ConnectionClosed)
                is_cert_error = (
                    isinstance(e, ssl.SSLCertVerificationError)
                    or "CERTIFICATE_VERIFY_FAILED" in e_str
                    or "certificate verify failed" in e_str.lower()
                    or "UNKNOWN_CA" in e_str
                )
                if is_cert_error:
                    self.tls_cert_error_occurred = True
                    self._last_cert_error_time = time.monotonic()
                # TC_A_06_CS: TLS-protocol-version errors are logged for
                # diagnostics but no longer get a special fast-retry path.
                # OCTT's TLSv1.1 phase is fixed ~65s; the spec-mandated
                # exponential backoff (attempt 2 at wait_min*2 ≈ 180s with
                # wait_min=90) naturally lands inside OCTT's subsequent
                # TLSv1.2 phase. The previous one-shot wait=0 (2398810)
                # made attempt 2 fall inside the TLSv1.1 window and miss
                # Phase 2 entirely once OCTT widened it.
                is_tls_protocol_error = (
                    isinstance(e, ssl.SSLError)
                    and not is_cert_error
                    and (
                        "PROTOCOL_VERSION" in e_str.upper()
                        or "WRONG_SSL_VERSION" in e_str.upper()
                        or "UNSUPPORTED_PROTOCOL" in e_str.upper()
                        # OCTT's TLSv1.1-only listener (TC_A_06_CS step 2)
                        # responds with SSLV3_ALERT_HANDSHAKE_FAILURE when
                        # our ClientHello caps maximum_version at TLS 1.2 —
                        # different alert from TLSV1_ALERT_PROTOCOL_VERSION
                        # but the same semantic ("server rejects offered
                        # TLS version"). Catch it too so the recovery
                        # connection still emits InvalidTLSVersion per
                        # OCPP 2.0.1 step 14/16.
                        or "HANDSHAKE_FAILURE" in e_str.upper()
                    )
                )
                if is_tls_protocol_error:
                    self.tls_protocol_error_occurred = True
                logger.warning(
                    f"Connection error (cert_error={is_cert_error}, "
                    f"tls_protocol_error={is_tls_protocol_error}, "
                    f"type={type(e).__name__}): {e_str[:300]}"
                )
                # Profile-mismatch diagnostic: HTTP 401 means CSMS rejected
                # at HTTP layer despite TLS succeeding — usually a Basic
                # Auth header / security profile mismatch. Surface what
                # the client presented so the cause is obvious in
                # journalctl ``security_profile=`` greps without needing
                # to cross-reference with operational_profile.json.
                if (
                    isinstance(e, websockets.exceptions.InvalidStatus)
                    and "401" in e_str
                ):
                    logger.warning(
                        f"HTTP 401: CSMS rejected at HTTP layer. "
                        f"Client presented {self.describe_ws_kwargs()}. "
                        f"Check operational security_profile vs CSMS "
                        f"session profile (mismatch: Basic Auth missing "
                        f"or unexpected, or credentials wrong)."
                    )
                # TC_C_16_CS: drop the dead socket reference so concurrent
                # call() paths take the offline-queue branch instead of
                # trying to send on a half-closed ws and losing the event.
                self.ws = None
                self._cleanup_pending_calls()
                if not self._is_running:
                    break
                self._consecutive_failures += 1
                # TC_B_46_CS: give the controller a chance to swap to the
                # next priority slot after its per-slot failure budget is
                # exhausted. The handler may call update_connection(), which
                # resets _consecutive_failures and attempt for the new slot.
                if self._connection_failure_handler is not None:
                    try:
                        self._connection_failure_handler(self._consecutive_failures)
                    except Exception as ex:
                        logger.warning(f"connection_failure_handler raised: {ex}")
                    if self._consecutive_failures == 0:
                        attempt = 0
                wait_min, random_range, repeat_times = self._retry_config()
                _branch = "?"
                if self._skip_next_reconnect_wait:
                    # TC_A_06_CS / TC_A_11_CS: CS-initiated disconnect (Reset
                    # or post-cert-renewal swap) → reconnect immediately.
                    # Only set by code paths that own the close.
                    self._skip_next_reconnect_wait = False
                    wait_time = 0
                    _branch = "skip_next"
                elif is_tls_protocol_error and not self._tls_protocol_retry_done:
                    # TC_A_06_CS: one-shot retry at exactly wait_min after a
                    # TLS-protocol-version rejection. Bypasses the doubled
                    # exponential (step=1 → wait_min*2 = 180s) so attempt 2
                    # lands at ~T+90s — past OCTT's 65s TLSv1.1 Phase 1
                    # but inside the narrow Phase 2 (TLSv1.2) window.
                    # Resets on every successful connect so a genuinely
                    # persistent TLS-version misconfig still ends up in
                    # exponential backoff after this one-shot.
                    #
                    # Also reset the attempt counter so a failure on the
                    # very next attempt (e.g. Phase 2 listener still
                    # warming up after Phase 1 stop → ConnectionResetError)
                    # restarts exponential from step=0 (wait_min, not
                    # wait_min*4=360s). Without this reset, attempt 3 lands
                    # at ~T+450s — well past OCTT's ~T+319s timeout —
                    # forcing the test into "did not make a connection
                    # attempt" FAIL even when Phase 2 becomes ready a few
                    # seconds later. With the reset, attempt 3 lands at
                    # ~T+185s (wait_min*2^0 = 90s) and attempt 4 at
                    # ~T+370s, giving two chances inside the window.
                    self._tls_protocol_retry_done = True
                    wait_time = wait_min
                    attempt = -1  # incremented to 0 at end of except block
                    _branch = "tls_proto_one_shot"
                elif is_cert_error and not self._cert_error_retry_done:
                    # TC_A_05_CS first observed cert error: one-shot wait=0
                    # so the next handshake lands with the restored valid
                    # cert. Also (re)arm the fast-retry burst window — if
                    # more cert errors follow they fall into the extension
                    # branch below instead of the exponential else.
                    self._cert_error_retry_done = True
                    wait_time = 0
                    self._fast_retry_until = max(
                        self._fast_retry_until, time.monotonic() + 10.0
                    )
                    _branch = "cert_err_one_shot"
                elif (
                    is_clean_drop
                    and attempt == 0
                    and self._should_fast_retry_on_clean_drop()
                ):
                    # TC_A_05_CS round 1: CSMS swaps its server cert to an
                    # invalid one and cleanly closes the WS, then expects
                    # the CS to attempt a connect inside the bad-cert
                    # phase. Honouring wait_min here (=90s) misses the
                    # window entirely → cert error never observed → no
                    # SecurityEventNotification → test fails.
                    #
                    # _should_fast_retry_on_clean_drop narrows this to the
                    # cert-recovery pattern (drop <10s after a fresh
                    # connect). TC_B_51 / TC_B_52 close after long prep
                    # phases, predicate returns False, falls through to
                    # the spec-compliant else branch.
                    wait_time = 0
                    self._fast_retry_until = time.monotonic() + 10.0
                    _branch = "clean_drop_fast"
                elif is_cert_error and time.monotonic() < self._fast_retry_until:
                    # TC_A_05_CS bad-cert phase observed in the wild has
                    # varied 3s..~3min between OCTT runs. While cert errors
                    # keep arriving inside the burst window, extend the
                    # window so we don't drop to exponential half-way
                    # through and miss OCTT's revert (board log showed
                    # attempt 7 → wait 5760s after the original 10s window
                    # expired). Each cert error refreshes the window by
                    # 10s; once the bad cert lifts the next retry connects
                    # cleanly and resets all state.
                    wait_time = 1
                    self._fast_retry_until = time.monotonic() + 10.0
                    _branch = "cert_err_burst"
                elif time.monotonic() < self._fast_retry_until:
                    # Non-cert failures within the burst window (OSError
                    # while OCTT is still binding the bad-cert listener,
                    # etc.). Stay at 1s retries; do not extend so genuine
                    # network problems don't get stuck in fast-retry
                    # forever.
                    wait_time = 1
                    _branch = "in_burst_window"
                elif self._tls_protocol_retry_done:
                    # TC_A_06_CS Phase 2 robustness: once we've had a TLS
                    # protocol-version rejection and haven't successfully
                    # connected yet, OCTT's Phase 2 listener may RST
                    # several times before stabilising — observed window
                    # is 65s..~319s, and listener readiness within that
                    # window is non-deterministic. Suppress exponential
                    # growth and keep retrying at wait_min so multiple
                    # attempts land inside the window. The flag resets on
                    # the next successful connect, so a genuinely
                    # persistent TLS misconfig still escapes to
                    # exponential after a fresh failure round (the next
                    # time tls_protocol_retry_done is re-armed).
                    wait_time = wait_min
                    _branch = "tls_proto_stable"
                else:
                    # OCPP 2.0.1: every unsuccessful retry doubles the wait
                    # up to RetryBackOffRepeatTimes. Applies uniformly to
                    # ConnectionClosed, network errors, and HTTP rejections
                    # (TC_B_57_CS verifies the doubling on repeated reject).
                    if self._fast_retry_until > 0.0:
                        # Just exited a cert-recovery burst (TC_A_05): the
                        # attempt counter accumulated during 1s retries has
                        # no exponential meaning — every retry waited the
                        # same 1s. Reset so this branch starts from
                        # wait_min*2^0 instead of wait_min*2^N (observed
                        # N=95 in one TC_A_05 run, which would land at
                        # ~92160s ≈ 25h on the next failure).
                        attempt = 0
                        self._fast_retry_until = 0.0
                    step = min(attempt, repeat_times)
                    wait_time = (
                        wait_min * (2 ** step)
                        + (random.randint(0, random_range) if random_range > 0 else 0)
                    )
                # _branch tags the wait-selection branch so journal trails
                # can show why a particular wait_time was picked. Useful
                # when wait=0 fires unexpectedly (TC_B_52 observed this).
                _ce = (time.monotonic() - self._last_cert_error_time
                       if self._last_cert_error_time > 0 else -1)
                _ca = (time.monotonic() - self._last_connect_time
                       if self._last_connect_time > 0 else -1)
                _w = self._fast_retry_until - time.monotonic()
                logger.info(
                    f"Reconnecting in {wait_time}s (attempt {attempt + 1}, "
                    f"branch={_branch}, clean_drop={is_clean_drop}, "
                    f"cert_err={is_cert_error}, conn_age={_ca:.1f}s, "
                    f"cert_age={_ce:.1f}s, frt_left={_w:.1f}s)..."
                )
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                attempt += 1

    async def _drain_offline_queue(self) -> None:
        """재연결 후 오프라인 큐에 저장된 메세지를 순차 재전송한다.

        TC_E_16_CS: replay hooks run AFTER every message in the queue has
        been delivered. Firing a deauth stop_transaction mid-drain would
        interleave a fresh Ended event (seqNo n+1) between still-queued
        Updated events (seqNo < n), producing an out-of-order stream. By
        collecting (action, payload, response) triples and running the hook
        only after the drain loop terminates, the spec-required ordering
        (all queued-offline events first, then the fresh Ended on Invalid)
        is preserved.
        """
        if self.offline_queue.is_empty():
            return
        entries = await self.offline_queue.drain()
        logger.info(f"[OfflineQueue] Replaying {len(entries)} queued messages")
        replay_results: List[tuple] = []
        for entry in entries:
            # TC_B_52_CS / OCPP 2.0.1 §B12.FR.04: on reconnect the CS reports
            # only the LATEST status per connector via the post-connect
            # _send_availability_status_notification. Replaying queued
            # StatusNotifications here would produce duplicates that OCTT
            # rejects ("already received a StatusNotification on this EVSE,
            # connector"). TransactionEvent and other queued events still
            # replay normally to preserve the offline event history.
            if entry.get("action") == "StatusNotification":
                logger.info(
                    "[OfflineQueue] Skipping queued StatusNotification "
                    f"(latest state already sent on reconnect): "
                    f"{entry.get('payload', {})}"
                )
                await self.offline_queue.ack_in_flight(entry)
                continue
            try:
                response = await self.call(entry["action"], entry["payload"])
                logger.info(f"[OfflineQueue] Replayed: {entry['action']}")
                replay_results.append((entry["action"], entry["payload"], response or {}))
            except Exception as e:
                logger.error(f"[OfflineQueue] Failed to replay {entry['action']}: {e}")
            finally:
                # TC_E_29_CS: drop from the in-flight shadow as soon as this
                # entry leaves the CS — messagesInQueue stays true until the
                # full drain loop ends, but individual ACKs shrink the set
                # so that peek() only reports what's actually pending.
                await self.offline_queue.ack_in_flight(entry)
        # Safety net: anything left in-flight (never ACKed) is dropped so
        # subsequent peeks don't leak stale state.
        await self.offline_queue.clear_in_flight()
        if self._replay_response_hook is not None:
            for action, payload, response in replay_results:
                try:
                    await self._replay_response_hook(action, payload, response)
                except Exception as hook_err:
                    logger.error(
                        f"[OfflineQueue] Replay hook for {action} failed: {hook_err}"
                    )

    async def _listen(self) -> None:
        try:
            async for message in self.ws:
                await self._handle_message(message)
        except ConnectionClosed:
            logger.warning("Listen task stopped due to ConnectionClosed")
            raise

    async def _handle_message(self, message: str) -> None:
        try:
            msg_list = json.loads(message)
        except json.JSONDecodeError as e:
            logger.error(f"[ProtocolError] Failed to decode JSON: {e}")
            return

        try:
            msg_type = msg_list[0]
            msg_id   = msg_list[1]
        except (IndexError, TypeError) as e:
            logger.error(f"[ProtocolError] Malformed message frame: {e}")
            return

        logger.info(f"WS RECV: msgId={msg_id} type={msg_type} raw={message[:200]}")

        if msg_type == OCPPConfig.MESSAGE_TYPE_RESULT:
            payload = msg_list[2]
            action  = self._pending_actions.get(msg_id)
            if action:
                try:
                    self._validate_payload(f"{action}Response.json", payload)
                    self._resolve_pending_call(msg_id, payload)
                except ValueError as ve:
                    error_code = (
                        RPCErrorCodes.TYPE_CONSTRAINT_VIOLATION
                        if RPCErrorCodes.TYPE_CONSTRAINT_VIOLATION in str(ve)
                        else RPCErrorCodes.FORMAT_VIOLATION
                    )
                    self._resolve_pending_call_error(msg_id, error_code, str(ve), {})
            else:
                logger.warning(f"Received RESULT for unknown msgId={msg_id}")

        elif msg_type == OCPPConfig.MESSAGE_TYPE_ERROR:
            err_code    = msg_list[2]
            err_desc    = msg_list[3]
            err_details = msg_list[4] if len(msg_list) > 4 else {}
            logger.warning(f"RPC Error received: msgId={msg_id} code={err_code} desc={err_desc}")
            self._resolve_pending_call_error(msg_id, err_code, err_desc, err_details)

        elif msg_type == OCPPConfig.MESSAGE_TYPE_CALL:
            action  = msg_list[2]
            payload = msg_list[3]
            logger.info(f"WS CALL: msgId={msg_id} action={action}")
            try:
                self._validate_payload(f"{action}Request.json", payload)
            except ValueError as ve:
                error_code = (
                    RPCErrorCodes.TYPE_CONSTRAINT_VIOLATION
                    if RPCErrorCodes.TYPE_CONSTRAINT_VIOLATION in str(ve)
                    else RPCErrorCodes.FORMAT_VIOLATION
                )
                await self._send_error(msg_id, error_code, str(ve))
                return

            if self._message_gate is not None:
                gate_result = self._message_gate(action)
                if gate_result is not None:
                    err_code, err_desc = gate_result
                    await self._send_error(msg_id, err_code, err_desc)
                    return

            if action in self._action_handlers:
                try:
                    resp_payload = await self._action_handlers[action](payload)
                    self._validate_payload(f"{action}Response.json", resp_payload)
                    await self._send_result(msg_id, resp_payload)
                except ValueError as ve:
                    await self._send_error(
                        msg_id, RPCErrorCodes.INTERNAL_ERROR,
                        f"Generated response is invalid: {ve}"
                    )
                except Exception as e:
                    logger.error(f"Handler exception for {action}: {e}", exc_info=True)
                    await self._send_error(msg_id, RPCErrorCodes.INTERNAL_ERROR, str(e))
            else:
                logger.warning(f"Action '{action}' has no registered handler")
                await self._send_error(
                    msg_id, RPCErrorCodes.NOT_SUPPORTED,
                    f"Action '{action}' is not supported"
                )
        else:
            logger.warning(f"[ProtocolError] Unknown message type: {msg_type}")

    def _resolve_pending_call(self, msg_id: str, payload: Dict) -> None:
        if msg_id in self._pending_calls and not self._pending_calls[msg_id].done():
            self._pending_calls[msg_id].set_result(payload)

    def _resolve_pending_call_error(
        self, msg_id: str, err_code: str, err_desc: str, err_details: Dict
    ) -> None:
        if msg_id in self._pending_calls and not self._pending_calls[msg_id].done():
            self._pending_calls[msg_id].set_exception(
                WaitQueueError(f"[{err_code}] {err_desc}")
            )

    def _cleanup_pending_calls(self) -> None:
        for future in self._pending_calls.values():
            if not future.done():
                future.set_exception(ConnectionError("Connection lost."))
        self._pending_calls.clear()
        self._pending_actions.clear()

    async def disconnect(self) -> None:
        self._is_running = False
        if self.ws:
            await self.ws.close()
        if self._listen_task:
            self._listen_task.cancel()

    async def call(
        self,
        action: str,
        payload: Dict[str, Any],
        timeout: float = 60.0,
        allow_offline: bool = False,
    ) -> Dict[str, Any]:
        """CSMS에 CALL 메세지를 전송하고 CALLRESULT를 반환한다.

        allow_offline=True 지정 시, 연결 불가 상태에서 메세지를 오프라인 큐에
        저장하고 빈 dict를 반환한다. [OCPP 2.0.1 Part 2 - 4.1.1]
        """
        if not self.ws:
            if allow_offline:
                # OCPP 2.0.1 TransactionEventRequest has an `offline` flag that
                # must be true when the event was generated while the CS
                # couldn't reach the CSMS (TC_C_16_CS Tool validation Step 3).
                # Stamp it here so the controller side doesn't need to know
                # whether it was online at emit time.
                if action == "TransactionEvent":
                    payload = {**payload, "offline": True}
                await self.offline_queue.enqueue(action, payload)
                return {}
            raise ConnectionError("Not connected to CSMS")

        try:
            self._validate_payload(f"{action}Request.json", payload)
        except ValueError as e:
            raise WaitQueueError(f"Client payload validation failed: {e}")

        # TC_E_41/E_42/E_50/E_51_CS: TransactionEvent gets its own §E13 retry
        # schedule (MessageAttempts + MessageAttemptInterval + MessageTimeout).
        # The CS resends on the same connection without closing the ws — OCTT
        # validates that the same message is sent the configured number of
        # times, spaced by (MessageAttemptInterval * n) + MessageTimeout.
        is_tx_event = (action == "TransactionEvent")
        if is_tx_event:
            tx_attempts, tx_interval, tx_timeout = self._tx_retry_config()
            # Override the per-attempt timeout so OCTT's expected wait schedule
            # (sum-to-next-transmission = MessageTimeout + interval*n) matches.
            timeout = float(tx_timeout)
        else:
            tx_attempts, tx_interval = (1, 0)
        attempt_idx = 0
        while True:
            async with self._call_lock:
                msg_id   = str(uuid.uuid4())
                call_msg = [OCPPConfig.MESSAGE_TYPE_CALL, msg_id, action, payload]

                future = asyncio.get_event_loop().create_future()
                self._pending_calls[msg_id]  = future
                self._pending_actions[msg_id] = action

                try:
                    raw_msg = json.dumps(call_msg)
                    logger.info(f"WS SEND: msgId={msg_id} action={action} payload={raw_msg[:200]}")
                    try:
                        await self.ws.send(raw_msg)
                    except (ConnectionClosed, OSError) as send_err:
                        # TC_C_16_CS: the listener may not have set self.ws=None
                        # yet when a close frame races with our send. Treat this
                        # as offline and queue if the caller permitted it.
                        logger.warning(
                            f"WS send failed ({type(send_err).__name__}); "
                            f"ws appeared alive but is closed"
                        )
                        self.ws = None
                        self._pending_calls.pop(msg_id, None)
                        self._pending_actions.pop(msg_id, None)
                        if allow_offline:
                            if action == "TransactionEvent":
                                payload = {**payload, "offline": True}
                            await self.offline_queue.enqueue(action, payload)
                            return {}
                        raise ConnectionError(f"Not connected to CSMS: {send_err}")
                    return await asyncio.wait_for(future, timeout=timeout)
                except (asyncio.TimeoutError, WaitQueueError) as call_err:
                    # TC_E_50_CS: OCTT can answer a TransactionEvent with a
                    # CALLERROR instead of silence — that surfaces here as
                    # WaitQueueError (the future raised). Both outcomes count
                    # as a failed attempt and must trigger the §E13 retry.
                    is_timeout = isinstance(call_err, asyncio.TimeoutError)
                    attempt_idx += 1
                    if is_tx_event and attempt_idx < tx_attempts:
                        # §E13: retry on the same connection after the
                        # interval * retry-count. Release the lock so the
                        # listener can still process incoming RECVs, then
                        # reacquire for the next attempt.
                        self._pending_calls.pop(msg_id, None)
                        self._pending_actions.pop(msg_id, None)
                        wait = tx_interval * attempt_idx
                        reason = "no response" if is_timeout else f"CALLERROR ({call_err})"
                        logger.warning(
                            f"TransactionEvent {reason} (attempt {attempt_idx}/"
                            f"{tx_attempts}) — retrying in {wait}s"
                        )
                        break_to_retry = True
                    else:
                        break_to_retry = False
                        if is_tx_event:
                            logger.warning(
                                f"TransactionEvent exhausted {tx_attempts} attempts —"
                                f" giving up without closing ws (§E13)"
                            )
                            raise WaitQueueError(
                                f"TransactionEvent retry limit reached"
                            )
                        # Non-TransactionEvent failure propagates as-is for
                        # WaitQueueError, or §4.1 ws-close for TimeoutError.
                        if is_timeout:
                            logger.warning(
                                f"CALL timeout waiting for response to {action} "
                                f"(msgId={msg_id}) — closing WS to keep §4.1"
                            )
                            try:
                                if self.ws:
                                    await self.ws.close(
                                        code=1000, reason="call-timeout"
                                    )
                            except Exception:
                                pass
                            raise WaitQueueError(
                                f"Timeout waiting for response to {action}"
                            )
                        raise
                finally:
                    self._pending_calls.pop(msg_id, None)
                    self._pending_actions.pop(msg_id, None)
            # Fell out of async-with (lock released). Wait the retry interval
            # outside the lock so other traffic can flow if needed.
            if break_to_retry:
                await asyncio.sleep(wait)
                continue
            return {}  # unreachable under normal flow

    async def _send_result(self, msg_id: str, payload: Dict) -> None:
        if not self.ws:
            return
        raw = json.dumps([OCPPConfig.MESSAGE_TYPE_RESULT, msg_id, payload])
        logger.info(f"WS SEND RESULT: msgId={msg_id}")
        await self.ws.send(raw)

    async def _send_error(
        self,
        msg_id: str,
        error_code: str,
        error_description: str,
        error_details: Optional[Dict] = None,
    ) -> None:
        if not self.ws:
            return
        raw = json.dumps([
            OCPPConfig.MESSAGE_TYPE_ERROR,
            msg_id,
            error_code,
            error_description,
            error_details or {},
        ])
        logger.warning(f"WS SEND ERROR: msgId={msg_id} code={error_code} desc={error_description}")
        await self.ws.send(raw)
