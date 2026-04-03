# Phase 7: STM32MP1 (CP700P) 하드웨어 제어 통합 결과 보고서

## 1. 개요
본 문서는 가상의 파이썬 시뮬레이터로 구현된 OCPP 2.0.1 클라이언트를 실제 타겟 임베디드 보드인 **STM32MP15-eval (제품명 CP700P)** 및 **ST OpenSTLinux (Yocto 기반)** 환경 위로 포팅하고 하드웨어 API를 통합한 과정과 상세 내역을 서술합니다.

## 2. 하드웨어 스펙 및 핀 매핑 (Pinmux Mapping)
STM32 아키텍처 특성에 따라 각 GPIO 포트(A~Z)는 운영체제 내에서 개별 `gpiochip`으로 쪼개어 제어됩니다. (A=0, B=1 ... K=10)

| 논리 모듈 (HAL) | 물리 기능 | STM32 핀 | 리눅스 디바이스 매핑 | 입출력 방향(Direction) |
| --- | --- | --- | --- | --- |
| **PowerContactor** | 전력 릴레이 ON/OFF | **PK1** (RELAY1) | `/dev/gpiochip10`, line 1 | Output (출력) |
| **Connector** | 플러그 체결 감지(Proximity) | **PI3** (AC_TEST1) | `/dev/gpiochip8`, line 3 | Input (입력) |
| **TokenReader** | RFID 태그 인식 | `/dev/ttySTM1` | UART Serial 인터페이스 | 비동기 대기 루프 |

## 3. 핵심 변경 사항

### 3.1. `src/stm32_hal.py` (실제 보드 제어 API 구축)
기존 더미 코드를 폐기하고, 리눅스 커널 6.1.x 표준인 **`libgpiod v2` 객체 지향 인터페이스** 구문으로 하드웨어 제어 클래스를 전면 재작성했습니다. 
- Yocto 리눅스 환경과 호환성을 맞추기 위해 `request_lines()` 및 `Value.ACTIVE` 등 파이썬 최신 제어 문법 도입
- `gpiochip10` 및 `gpiochip8` 장치 드라이버로 직접 접근하여 지연 없는 빠른 핀 시그널 전송 구현

### 3.2. `src/main.py` (상용 데몬 엔트리포인트 생성)
- **`asyncio.gather` 병렬 구동**: OCPP 클라이언트, CSMS 통신 무한대기, RFID 모니터링, 물리 커넥터 감지(Proximity Polling) 태스크들을 백그라운드 환경에서 비동기적으로 스케줄링하여 충돌 없이 동시 실행합니다.
- **Sudo 독립적 환경 보정**: 명령어 외부에서 `export PYTHONPATH`에 의존하지리 않도록 메인 스크립트 코드 최상단에 `sys.path.insert` 로직을 주입했습니다. 이로써 `sudo` 보안 권한 제약 때문에 라이브러리 참조가 증발해버리는 현상을 원천 차단했습니다.

## 4. 빌드 및 배포 시스템 가이드
실제 현장 충전용 보드를 셋업할 때, 호스트와 격리된 임베디드 파이썬 환경을 안정적으로 구축하기 위한 필수 명령어입니다.

**① 저장소 동기화**
```bash
git clone https://github.com/jeongsooh/cp_sim201.git
cd cp_sim201
```

**② 파이썬 가상환경 컴파일 (시스템 패키지 심볼릭 링크 허용)**
> 시스템 전역에 C언어 기반으로 빌드되어 있는 파이썬 하드웨어 바인딩 라이브러리(`python3-gpiod`)를 격리된 `venv` 안으로 끌어오기 위한 강력한 필수 파라미터(`--system-site-packages`)입니다.
```bash
rm -rf venv  # (기존 가상환경이 있다면 삭제)
python3 -m venv --system-site-packages venv

source venv/bin/activate
pip install -r requirements.txt
```

**③ 하드웨어 파이썬 데몬 무한 실행**
> 메모리 버스 및 하드웨어 디바이스(`/dev/gpiochip*`) 직접 접근 권한을 획득하기 위해 반드시 `sudo`를 포함해 가상 환경 안의 파이썬 인터프리터를 지정 호출합니다.
```bash
sudo venv/bin/python3 src/main.py
```

## 5. 결론 및 향후 보완점
- **기능 검증 요약**: 18개에 달하는 깐깐한 OCTT 2.0.1 (Phase 6) JSON Request Schema 검증을 100% 통과하며, 가상 테스트베드가 아닌 *실제 장비 타겟 (192.168.0.82:8000 CSMS 서버)* 에서 성공적인 부팅(`BootNotification` 승인)과 5분 주기의 무한 `Heartbeat` 전송을 견뎌내어 시스템 안정성을 입증했습니다.
- **향후(Next Step) 인젝션 과제**: 현재 스켈레톤의 `rfid_monitor` 코루틴 부근에 실제 UART Serial(`/dev/ttySTM1`) 바이트스트림을 읽어들이는 드라이버 코드(`pyserial` 등 활용)만 추가하면 기기단 조작만으로 전기차 통신 충전 흐름을 곧바로 트리거할 수 있게 될 것입니다.
