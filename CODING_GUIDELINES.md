# OCPP 2.0.1 AC Client - Coding Guidelines

본 가이드라인은 OCPP 2.0.1 (2024-09 Errata 반영) 기반 AC 충전기 소프트웨어 개발 시 일관성을 유지하기 위한 원칙을 정의합니다. 모든 개발 및 AI 어시스턴트 코드 작성 시 이 지침을 최우선으로 준수해야 합니다.

## 1. 아키텍처 및 계층 분리 원칙 (Architecture & Separation of Concerns)
- **HAL(Hardware Abstraction Layer)의 철저한 분리**: 하드웨어 제어 논리(GPIO, Serial 등 물리적 통신)는 `ConnectorHAL`, `TokenReaderHAL`, `PowerContactorHAL` 등의 인터페이스 내에서만 처리합니다. 핵심 비즈니스 로직(OCPP 메시지 생성, 전송 등)에 하드웨어 코드가 섞이지 않도록 합니다.
- **3-Tier 모델 준수**: 모든 컴포넌트와 변수는 `Station (0)`, `EVSE (1~N)`, `Connector (1~N)`의 계층적 구조에 맞게 매핑되어야 합니다.

## 2. OCPP 2.0.1 규격 및 컴플라이언스 (Standard Compliance)
- **2024-09 Errata 엄수**: 트랜잭션 종료 시 `StoppedReason` 명시, 중단 사유 분리 등 최신 에라타 규격을 코딩에 반드시 반영합니다.
- **동기적 RPC 및 메시지 큐 (Message Queueing)**: 비동기(Async) 소켓 통신을 하되, 특정 CALL 메시지를 전송한 후 CALLRESULT나 CALLERROR를 받기 전까지 대기하는 **Wait Queue (동기화 큐)** 로직을 반드시 구현합니다.
- **오프라인 대응 보장 (Offline Handling)**: 네트워크 단절 시나리오를 대비하여 `TransactionEvent` 등 중요 메시지는 큐(비휘발성 메모리 또는 로컬 DB)에 저장하고 복구 시 누락 없이 순차 전송해야 합니다.
- **상태 영속성 (Persistence)**: `Mutability` 속성이 `ReadWrite`인 장치 모델(Device Model) 변수의 변경점은 재부팅 시에도 유지될 수 있게 영구 저장소에 기록합니다.

## 3. 코드 작성 규칙 (Coding Conventions)
- **언어 및 타이핑**: Python 3.10 이상을 기준으로 하며, 모든 함수와 클래스에 명시적인 타입 힌팅(Type Hinting)을 적용합니다. (예: `def send_message(payload: dict) -> bool:`)
- **비동기 프로그래밍 (Asyncio)**: WebSocket 통신 및 장치 입출력에는 가급적 `async/await` 패턴을 사용하여 블로킹(Blocking)을 최소화합니다.
- **주석 및 문서화**: 클래스 및 메서드 작성 시, 해당하는 OCPP 2.0.1 규격서 파트나 요구사항 ID(예: `[Part 2 - 3.1.1]`)를 주석 또는 Docstring에 남겨 추적성을 확보합니다.

## 4. 에러 처리 및 로깅 (Error Handling & Logging)
- **로깅 포맷**: 실서버 및 OCTT(테스트 툴) 디버깅을 위해 로깅 포맷을 표준화합니다. (시간, 레벨, 모듈명, MessageId, 페이로드 등 포함)
- **OCPP 에러 래핑 (Error Wrapping)**: 내부 예외(Exception) 발생 시, 규격에 맞는 RPC 통신 에러(예: `FormatViolation`, `TypeConstraintViolation`, `ProtocolError`)로 적절히 변환하여 응답하도록 예외 처리기를 구축합니다.

## 5. 테스트 주도 구현 (Testing)
- 컴포넌트나 모듈 구현 완료 시, OCTT Part 6 테스트 케이스(예: `TC_B_01_CS`, `TC_E_01_CS`) 시나리오 단위별로 통합/단위 테스트를 작성하여 검증합니다.

## 6. 문서의 작성
- 항상 코딩을 할 때는 코딩할 내용을 작성해서 outputs 폴더에 md 파일로 저장한다.
- 구현 플랜도 output_docs 폴더에 md 파일로 저장한다. 구현이 끝나면 결과에 대해서도 별도의 md 파일로 저장한다.

## 7. 코딩 과정에 관한 요청 사항
- 코딩 과정에 현재의 workspace 내에서 파일을 작성하거나 저장하거나, 또는 삭제할 경우 별도의 확인 질문없이 진행한다.
- 코딩이 완료되면 반드시 테스트를 진행하고 타인이 테스트를 진행할 수 있는 방법을 포함해서 결과를 md 파일로 저장한다.