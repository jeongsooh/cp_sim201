# Timing Matrix

OCTT timing-sensitive 테스트의 요구사항·OCTT 파라미터·영향 코드 경로를 한눈에 보기 위한 매트릭스. **timing 관련 코드를 변경하기 전에 이 문서를 먼저 검토해 충돌하는 행을 식별한다.**

## 업데이트 프로토콜

1. retry/reconnect/debounce/heartbeat 등 timing 영향이 있는 코드를 변경할 때, 변경 전에 이 문서의 모든 행을 읽고 영향받는 테스트를 commit message에 명시한다.
2. 영향받을 가능성이 있는 테스트는 deploy 전 commit 본문에 사전 영향 분석 표로 기재한다.
3. 테스트 결과(PASS/FAIL/INCONC)가 바뀌면 곧바로 Status 칼럼을 업데이트하고 관련 commit hash를 기록한다.
4. 새 timing 테스트 디버깅 시 이 매트릭스에 행을 추가한다.

## 영향받는 핵심 코드 경로

| 경로 | 책임 |
|------|------|
| `ocpp_client.py:connect()` except 블록 | 연결 실패 시 wait/skip 결정 |
| `ocpp_client.py:_skip_next_reconnect_wait` | CS-initiated close 후 즉시 재연결 |
| `ocpp_client.py:_tls_protocol_retry_done` | TLS protocol error 후 one-shot 즉시 재시도 |
| `controller.py:handle_reset_request` | Reset 후 ws.close + skip flag 무장 |
| `controller.py:_execute_reset` | Reset 실행 (active profile 적용 후 ws.close) |
| `controller.py:handle_certificate_signed` | cert 저장 + Profile≥2면 reconnect 트리거 |
| `controller.py:_reconnect_with_new_client_cert` | cert 갱신 후 SSL ctx rebuild + ws.close |
| `controller.py:handle_rfid_scan` | RFID debounce + stop-scan guard |
| `controller.py:_heartbeat_loop` | HeartbeatInterval 정기 송신 |
| `controller.py:_drain_offline_queue` | 오프라인 큐 재전송 |

---

## A. Connection retry / reconnect

| Test | OCTT params 설정 | OCTT 윈도우 | 기대 CS 동작 | 영향 코드 | Status / Commit |
|------|------------------|-------------|---------------|-----------|------------------|
| **TC_A_05_CS** | wait_min=90 | 동적 (CS reconnect 측정 후 결정. 빠르면 ~2.7s, 90s면 ~3분) | bad cert 후 spec wait_min 준수, 재연결 후 InvalidCsmsCertificate SecurityEvent | ocpp_client.py cert_error 분기 | **알려진 한계** (948a8d9): wait_min=90이면 dynamic window가 2.7s밖에 안 될 때 못 맞춤 |
| **TC_A_06_CS** | wait_min=90, NetworkProfileConnectionAttempts=1 | **고정 ~65s** (TLSv1.1 listener up→close) | attempt 1 즉시(skip flag) + attempt 2도 빠르게 (TLS one-shot fast retry) | handle_reset_request line 1227, _execute_reset line 1280, ocpp_client `is_tls_protocol_error` + `_tls_protocol_retry_done` | **PASS** (2398810) |
| **TC_A_11_CS** | (default) | 관대 — "wait some time, force-drop if needed" | 새 client cert로 reconnect (SSL ctx rebuild) | handle_certificate_signed (Profile 3 분기) → _reconnect_with_new_client_cert | **PASS** |
| **TC_A_19_CS** | (default, prep에 RenewChargingStationCertificate) | ~64s self-disconnect + ~64s reconnect | CertificateSigned 후 WS bounce (Profile 2 포함, SSL rebuild는 Profile 3에만) | handle_certificate_signed (>=2 분기), _reconnect_with_new_client_cert | **PASS** (77bd58f) |
| **TC_B_51_CS** | wait_min=64, OfflineThreshold=62 | OfflineThreshold(62s) 이상 offline 유지 | 첫 attempt까지 wait_min 준수 (>=62s) → SecurityEventNotification(OfflineThreshold) 트리거 | ocpp_client.connect default 분기 (exponential) | **PASS** |
| **TC_B_57_CS** | wait_min=90 | 거듭된 rejection 사이 doubling 검증 | attempt 1 wait_min, attempt 2 wait_min*2(=180s) | ocpp_client.connect exponential (`5f7ce0b`에서 transient cap 제거) | **PASS** (5f7ce0b) |
| **TC_E_43_CS** | wait_min=90, OfflineThreshold=210, NetworkProfileConnectionAttempts=3 | OCTT 측 timeout ≈ user_unplug+64s | 오프라인 인증(LocalAuthList → AuthCache → OfflineTxForUnknownIdEnabled) + 이벤트 큐잉 + 재연결 시 drain | handle_rfid_scan offline 분기 (b74f9e1), offline_queue, _drain_offline_queue | **PASS-conditional** — user 액션이 빠르면 timing 충돌 (memory: feedback_octt_dynamic_timing) |
| **TC_B_50_CS** | (default) | reset reconnect | handle_reset_request 안에서 active profile 미리 적용 (race 회피) | handle_reset_request._apply_active_network_profile | **PASS** (8018943) |
| **TC_B_22_CS** | (default) | reset reconnect | active tx면 ImmediateReset stop → ws.close → reconnect | _execute_reset (tx stop 분기) | **PASS** |
| **TC_B_21_CS** | (default) | OnIdle defer | tx ends + cable unplugged 모두 충족 시 reset 실행 | _try_execute_deferred_reset | **PASS** |
| **TC_G_21_CS** | (default) | reboot persists Inoperative | reboot 후 admin_state.json에서 Inoperative 복원 | persistence.load_admin_state | **PASS-conditional** — reboot 늦으면 heartbeat-vs-reboot race로 OCTT FAIL. 권장: prompt 후 60s 안에 reboot |

## B. RFID / transaction state debounce

| Test | Trigger | 기대 CS 동작 | 영향 코드 | Status / Commit |
|------|---------|---------------|-----------|------------------|
| **TC_E_05_CS** | RFID 하드웨어 더블 emit (~0.6s) | 같은 UID 2초 이내 재스캔 무시 | handle_rfid_scan `_last_rfid_uid` / `_last_rfid_scan_at` | **PASS** (5ade17d) |
| **TC_N_30_CS** | tx 시작 후 cable plug 전 사용자 더블탭 (~6s) | `_state_c_active=True` 이전엔 same-idToken 재스캔 무시 | handle_rfid_scan stop-scan guard | **PASS** (f95dea8) |

## C. Heartbeat / 정기 메시지

| Test | OCTT params | 기대 CS 동작 | 영향 코드 | Status |
|------|-------------|---------------|-----------|--------|
| **TC_G_01/02/...** (heartbeat 정기) | HeartbeatInterval | OCPP §G02: 매 interval마다 무조건 송신 | _heartbeat_loop | **PASS** — 8bb932f의 "다른 메시지 활동 시 skip" 최적화는 spec과 어긋나 52c9135에서 revert |

---

## D. 알려진 충돌 / Trade-offs

이 섹션은 timing fix가 서로 모순될 때 어떤 우선순위로 해결됐는지 기록한다. 새 fix가 이 표의 trade-off를 다시 깨면 다른 해결책 모색.

| 충돌 영역 | 영향 받는 테스트 | 현재 해결 방향 | 결정 근거 |
|-----------|------------------|----------------|-----------|
| post-Reset 첫 attempt 즉시 vs spec wait_min | TC_A_06_CS (즉시 필요) vs TC_B_51_CS (spec 필요) | `_skip_next_reconnect_wait`: Reset 경로에서만 즉시, 그 외는 spec | TC_A_06은 OCTT 65s 윈도우, TC_B_51은 OfflineThreshold 검증으로 둘 다 spec 양립 |
| TLS error 후 즉시 재시도 vs exponential doubling | TC_A_06_CS (즉시 필요) vs TC_B_57_CS (doubling 필요) | TLS protocol version error에 한해 one-shot 즉시; 다른 error는 exponential | 다른 error 타입이라 분기로 양립 |
| transient HTTP rejection cap vs uniform exponential | TC_E_16_CS (짧은 wait 필요) vs TC_B_57_CS (doubling 검증) | uniform exponential (cap 제거) | TC_E_16은 attempt=0이면 wait_min*2^0=wait_min과 동일해 양립 (5f7ce0b commit message 참고) |
| heartbeat skip-on-activity vs unconditional | TC_G_21_CS-fixable (skip이 race 줄임) vs heartbeat tests (spec 요구) | unconditional (spec 우선) | 사용자가 reboot 빨리 하면 race 자연 해결 |
| BootNotification.serialNumber vs cert CN | TC_A_07_CS | station_config.json `serial_number = station_id` (cert CN과 동일) | PICS에 serial 필드 없어 자유로이 정렬 가능 (f87e6a0 commit) |

---

## E. 메모리 / 외부 문서 참조

- `feedback_octt_dynamic_timing.md` — OCTT 동적 timing 윈도우 측정 패턴 (TC_A_05_CS 디버깅 시 발견)
- `OCPP-2.0.1_part6-test_cases_cs.txt` — 본 매트릭스의 OCTT 시나리오 출처
- `feedback_deploy_then_push.md` — 배포 직후 push 강제 (timing fix 회귀 추적용)

---

## F. 향후 개선

- [ ] OCTT 161개 테스트 모두 PASS 시점에 git tag (`v1.0-all-pass` 등) — 회귀 발생 시 `git diff <tag> HEAD` 로 회귀 가능 영역 즉시 식별
- [ ] TIMING_MATRIX와 commit 영향 분석 표를 자동 cross-check 하는 스크립트 (선택사항)
- [ ] OCTT 전체 회귀 자동 실행 (현 OCTT 도구는 manual; 가능 시 CLI 스크립팅)
