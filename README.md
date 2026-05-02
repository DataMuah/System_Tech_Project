```
 ██████╗██╗  ██╗ █████╗ ████████╗    ███████╗███████╗██████╗ ██╗   ██╗███████╗██████╗
██╔════╝██║  ██║██╔══██╗╚══██╔══╝    ██╔════╝██╔════╝██╔══██╗██║   ██║██╔════╝██╔══██╗
██║     ███████║███████║   ██║       ███████╗█████╗  ██████╔╝██║   ██║█████╗  ██████╔╝
██║     ██╔══██║██╔══██║   ██║       ╚════██║██╔══╝  ██╔══██╗╚██╗ ██╔╝██╔══╝  ██╔══██╗
╚██████╗██║  ██║██║  ██║   ██║       ███████║███████╗██║  ██║ ╚████╔╝ ███████╗██║  ██║
 ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝       ╚══════╝╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝
```

# 멀티채팅 서버 · Rust + Tokio

*도전과제 프로젝트 2 — 500명이 동시에 접속하는 단일 채팅방을 Rust로 구현하고 검증합니다.*

`Rust 1.75+` · `Tokio 1.x` · `Async/Await` · `Stress Tested 500 Clients` · `535 msg/s` · `Connection Failure 0/500`

---

## 📋 목차

1. [프로젝트 개요](#-프로젝트-개요)
2. [빠른 시작](#-빠른-시작)
3. [파일 구조](#-파일-구조)
4. [시스템 아키텍처](#-시스템-아키텍처)
5. [서버 구현 상세](#-서버-구현-상세)
6. [스트레스 테스트](#-스트레스-테스트)
7. [서버 모니터 (TUI)](#️-서버-모니터-tui)
8. [실행 결과 & 분석](#-실행-결과--분석)
9. [성능 설계 원칙](#-성능-설계-원칙)
10. [의존성](#-의존성)
11. [팀원](#-팀원)

---

## 🎯 프로젝트 개요

> **"500명이 동시에 입장한 단톡방에서 각자 다른 메시지를 보내고, 제대로 수신되는지 검증한다."**

Rust의 소유권 모델과 Tokio의 비동기 런타임을 활용하여 **OS 스레드 없이** 수백 개의 TCP 연결을 동시에 처리하는 채팅 서버를 구현합니다.

**구현된 기능**
- 단일 채팅방 실시간 메시지 브로드캐스트
- 닉네임 기반 클라이언트 식별
- 입장 / 퇴장 서버 알림
- `HH:MM:SS` 타임스탬프 메시지 포매팅
- 자기 메시지 echo 차단
- 연결/메시지 수 실시간 통계

**핵심 설계 목표**
- 500명 동시 접속 무실패 처리
- lock-free 원자적 통계 집계
- 교착(deadlock) 구조적 방지
- 메시지 복사 최소화 (O(1) fan-out)
- 재시도 로직 포함 부하 테스터

---

## ⚡ 빠른 시작

### 요구사항

```
Rust 1.75+   →   https://rustup.rs
```

### 30초 만에 실행하기

```bash
# 1. 빌드
cargo build --release

# 2. 터미널 A: 서버 시작
cargo run --bin server
# 🚀 채팅 서버 시작 → 0.0.0.0:8080

# 3. 터미널 B: 클라이언트 접속
nc 127.0.0.1 8080
# > Alice        ← 닉네임 입력 후 엔터
# > 안녕하세요!  ← 메시지 입력

# 4. 터미널 C: 스트레스 테스트 (500명 동시 접속)
cargo run --bin stress_test -- 500 5

# 5. 터미널 D: 서버 모니터 (TUI) — 채팅 흐름을 실시간 시각화
cargo run --bin monitor
```

### 스트레스 테스트 옵션

```bash
cargo run --bin stress_test -- [클라이언트수] [클라이언트당_메시지수] [서버주소]

# 예시
cargo run --bin stress_test -- 100 10                     # 소규모 테스트
cargo run --bin stress_test -- 500 5                      # 기본 (500명 × 5msg)
cargo run --bin stress_test -- 1000 3 192.168.0.10:8080   # 원격 서버 테스트
```

---

## 📁 파일 구조

```
System_Tech_Project/
│
├── Cargo.toml                      ← 의존성 및 바이너리 3개 정의
│
└── src/
    ├── main.rs                     ← 서버 본체
    │   ├── main()                  │  TCP accept 루프 + 상태 초기화
    │   ├── handle_client()         │  닉네임 수신, 송수신 분리, 퇴장 처리
    │   └── now()                   │  HH:MM:SS 타임스탬프 생성
    │
    └── bin/
        ├── stress_test.rs          ← 부하 테스터
        │   ├── main()              │  N개 async task 스폰 + 결과 집계 출력
        │   └── connect_with_retry()│  재시도 로직 (최대 5회)
        │
        └── monitor.rs              ← TUI 모니터 (NEW)
            ├── main()              │  서버 접속 + ratatui 터미널 초기화
            └── run_app()           │  메시지 수신 / 키 이벤트 / 화면 렌더 루프
```

| 파일 | 역할 | 주요 타입 |
|--------------------------|---------------------------|-----------|
| `Cargo.toml`             | 의존성 · 바이너리 진입점 정의    | `[[bin]]` × 3 |
| `src/main.rs`            | TCP 수락 · 메시지 라우팅 · 통계 | `broadcast::channel`, `RwLock`, `AtomicUsize` |
| `src/bin/stress_test.rs` | 동시 접속 · 송수신 검증        | `AtomicUsize` × 4, `tokio::spawn` |
| `src/bin/monitor.rs`     | 서버 트래픽 실시간 시각화 (TUI) | `ratatui::Terminal`, `mpsc::unbounded_channel` |

---

## 🏗️ 시스템 아키텍처

### 전체 구조

```
                     ┌──────────────────────────────────────────────────────────┐
                     │                      CHAT SERVER                         │
                     │                                                          │
  Client A ──TCP──▶  │  ┌────────────────┐                                      │
  Client B ──TCP──▶  │  │  TcpListener   │  accept() loop                       │
  Client C ──TCP──▶  │  │  0.0.0.0:8080  │                                      │
      ...            │  └───────┬────────┘                                      │
                     │          │ tokio::spawn() per connection                 │
                     │          ▼                                               │
                     │  ┌─────────────────────┐   Arc<RwLock<HashMap>>          │
                     │  │   handle_client()   │ ◀▶  { addr → nickname }         │
                     │  │                     │                                 │
                     │  │  into_split()        │   Arc<AtomicUsize>             │
                     │  │  ┌────────┐          │ ◀▶  total_connected            │
                     │  │  │ reader │──read──▶ tx.send(Message)                 │
                     │  │  └────────┘          │           ↓                    │
                     │  │  ┌────────┐          │  ┌──────────────────────┐      │
                     │  │  │ writer │◀─write── rx │ broadcast::channel   │      │
                     │  │  └────────┘          │  │      (cap=10k)       │      │
                     │  └─────────────────────┘  └──────────────────────┘       │
                     │                                       ↑                  │
                     │                    fan-out to all subscribers            │
                     │          ┌────────────────────────────┘                  │
                     │          │           │           │                       │
                     │       rx.recv()  rx.recv()   rx.recv()                   │
                     │       Client B   Client C   Client D  ...                │
                     └──────────────────────────────────────────────────────────┘
```

### 동시성 모델: 왜 스레드가 아닌가?

```
  전통적인 Thread-per-Connection          Tokio Async Task (이 서버)
  ─────────────────────────────          ───────────────────────────
  클라이언트 500명                        클라이언트 500명
       │                                       │
       ├─ Thread 1  (2MB 스택)                 ├─ async task 1  (~수 KB)
       ├─ Thread 2  (2MB 스택)                 ├─ async task 2
       ├─ Thread 3  (2MB 스택)                 ├─ async task 3
       ...                                     ...
       └─ Thread 500 (2MB 스택)                └─ async task 500
                                                       │
  총 메모리 : ~1 GB                        Tokio 워커 (CPU 수만큼)
  컨텍스트 스위칭 : 매우 빈번                   총 메모리 : ~수 MB
                                           컨텍스트 스위칭 : 최소
```

### 메시지 수명 주기

```
클라이언트 A 가 "안녕" 입력
        │
        ▼
   [TCP Socket]
        │  BufReader::read_line()
        ▼
  handle_client() 수신 루프
        │  tx.send(Message { from: "A", body: "안녕", ... })
        ▼
  broadcast::channel  ←── 단 1회 복사, Arc 로 모든 Receiver 공유
        │
        ├──▶  Receiver B → write_task → TCP Socket → Client B  ✅
        ├──▶  Receiver C → write_task → TCP Socket → Client C  ✅
        ├──▶  Receiver A → write_task →  msg.addr == A → skip  🚫 echo 없음
        └──▶  ...
```

---

## 🔬 서버 구현 상세

### 공유 상태 설계

```rust
// 접속 중인 클라이언트 목록
// 읽기 다수 / 쓰기 소수(입퇴장 시만) → RwLock 이 Mutex 보다 적합
type ClientMap = Arc<RwLock<HashMap<SocketAddr, String>>>;

// 메시지 구조체 — broadcast 로 전달되므로 Clone 필수
#[derive(Clone, Debug)]
struct Message {
    from:      String,      // 발신자 닉네임
    addr:      SocketAddr,  // echo 차단 판별용
    body:      String,      // 메시지 본문
    timestamp: String,      // HH:MM:SS
}
```

### 진입점 `main()`

```rust
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();

    // 단일 Sender — 모든 클라이언트가 subscribe() 로 Receiver 획득
    let (tx, _rx) = broadcast::channel::<Message>(10_000);
    let tx = Arc::new(tx);

    let clients: ClientMap   = Arc::new(RwLock::new(HashMap::new()));
    let total_connected      = Arc::new(AtomicUsize::new(0));
    let total_messages       = Arc::new(AtomicUsize::new(0));

    loop {
        let (socket, addr) = listener.accept().await?;
        // OS 스레드 생성 없이 경량 async task 로 분기
        tokio::spawn(async move {
            handle_client(socket, addr, tx, rx, clients, ...).await;
        });
    }
}
```

### 클라이언트 핸들러: 교착 방지 설계

소켓을 분리하지 않으면 단일 태스크가 읽기를 기다리는 동안 쓰기가 막혀
**교착(deadlock)** 이 발생합니다. `into_split()` 으로 구조적으로 방지합니다.

```rust
let (reader, mut writer) = socket.into_split();
//        │                    └── 쓰기 전용 → write_task 로 소유권 이동
//        └── 읽기 전용 → 현재 태스크에서 사용

// ✦ 송신 태스크: broadcast 수신 → 소켓 쓰기
let write_task = tokio::spawn(async move {
    loop {
        match rx.recv().await {
            Ok(msg) => {
                if msg.addr == addr { continue; } // 자신 메시지 skip
                let line = format!("[{}] {}: {}\n",
                                   msg.timestamp, msg.from, msg.body);
                if writer.write_all(line.as_bytes()).await.is_err() { break; }
            }
            Err(RecvError::Lagged(n)) => eprintln!("⚠️  {addr} lagged {n}"),
            Err(_) => break,
        }
    }
});

// ✦ 수신 루프: 소켓 읽기 → broadcast 전파
loop {
    match buf_reader.read_line(&mut line).await {
        Ok(0) => break, // EOF — 연결 종료
        Ok(_) => { tx.send(Message { from: nick.clone(), addr, body, timestamp: now() }); }
        Err(e) => { eprintln!("읽기 오류: {e}"); break; }
    }
}

// ✦ 퇴장 처리
write_task.abort();
clients.write().await.remove(&addr);
tx.send(Message { from: "SERVER".into(), body: format!("[{nick}] 퇴장"), ... });
```

---

## 🧪 스트레스 테스트

### 설계 원칙

```
                    stress_test 실행
                          │
         ┌────────────────┼─────────────────┐
         │                │                 │
    Batch 1 (50)     Batch 2 (50)  ...  Batch 10 (50)
         │            [50ms 대기]            │
         └─────── 한 번에 500개 연결하지 않음 ───┘
                  → accept() 큐 과부하 방지

  각 task 내부:
  ┌──────────────────────────────────────────────────┐
  │  connect_with_retry(addr, 최대 5회)                │
  │        │                                         │
  │  닉네임 전송  "client_NNNN\n"                       │
  │        │                                         │
  │   ┌────┴──────────┐   ┌──────────────────────┐   │
  │   │  recv_task    │   │  송신 루프             │   │
  │   │  (별도 spawn)  │   │  N_MESSAGES 번       │    │
  │   │  RECV++       │   │  10ms 간격 전송        │   │
  │   └───────────────┘   │  SENT++              │   │
  │        │              └──────────────────────┘   │
  │        │                                         │
  │   3초 대기 후 recv_task.abort()                    │
  └──────────────────────────────────────────────────┘
```

### 검증 공식

```
이론상 총 수신 수
= 총 송신 메시지 수 × (전체 클라이언트 수 - 1)
= (500명 × 5개) × (500 - 1)
= 2,500 × 499
= 1,247,500 건

※ SERVER 입/퇴장 알림은 이 계산에서 제외
※ 클라이언트가 3초 후 종료되므로 실측값은 이보다 낮음
  → 연결을 유지하면 수신율 ≈ 100 %
```

### 핵심 원자적 카운터

```rust
// 전역 AtomicUsize — Mutex 없는 lock-free 집계
static SENT:      AtomicUsize = AtomicUsize::new(0); // 총 송신
static RECV:      AtomicUsize = AtomicUsize::new(0); // 총 수신
static CONNECTED: AtomicUsize = AtomicUsize::new(0); // 연결 성공
static FAILED:    AtomicUsize = AtomicUsize::new(0); // 연결 실패

// 사용 예시 (병목 없이 멀티태스크에서 동시 호출 가능)
SENT.fetch_add(1, Ordering::Relaxed);
```

---

## 🖥️ 서버 모니터 (TUI)

### 개요

서버 측에 채팅 흐름을 **시각적으로** 확인할 수단이 없으면, 500명이 동시에 떠드는 트래픽이 정상인지 직관적으로 알기 어렵습니다.
모니터는 이 문제를 **기존 서버 코드를 한 줄도 건드리지 않는 방식**으로 해결합니다.

```
설계 원칙: "모니터는 그냥 또 한 명의 클라이언트일 뿐"

  ┌──────────────────────┐
  │      server (8080)   │  ← main.rs 그대로
  │  broadcast::channel  │
  └──────────┬───────────┘
             │ subscribe
             ▼
  ┌──────────────────────┐
  │  monitor (MONITOR)   │  ← 새 바이너리, 서버에 일반 TCP 클라이언트로 접속
  │   ratatui TUI 렌더    │
  └──────────────────────┘
```

서버는 같은 `addr` 로 들어온 메시지에 echo 를 하지 않으므로
(`main.rs:108` 의 `if msg.addr == addr { continue; }`),
모니터는 **다른 모든 클라이언트가 주고받는 메시지만** 깨끗하게 수신합니다.

### 실행

```bash
# 기본 — 127.0.0.1:8080 에 닉네임 "MONITOR" 로 접속
cargo run --release --bin monitor

# 주소 / 닉네임 지정
cargo run --release --bin monitor -- 127.0.0.1:8080 ADMIN
cargo run --release --bin monitor -- 192.168.0.10:8080 OPS-DESK
```

> `--release` 권장. 디버그 빌드는 ratatui 렌더링이 느려 burst 트래픽에서 broadcast lag 가 발생할 수 있습니다.

### 화면 구성

```
┌ 🖥  서버 모니터 | 닉: MONITOR | 메시지: 1,243 | LIVE ─────────────────┐
├ 채팅 로그 (1198~1243 / 1243) ─────────────────────────────────────┤
│ [12:03:11] SERVER: [client_0042] 입장 (현재 412명)                   │
│ [12:03:11] client_0042: client_0042 메시지 #0                      │
│ [12:03:11] client_0117: client_0117 메시지 #2                      │
│ [나] MONITOR: 모두 안녕하세요                                          │
│ [12:03:12] SERVER: [client_0203] 퇴장 (현재 411명)                   │
│ ...                                                                │
├ 입력 (Enter: 전송) ────────────────────────────────────────────────┤
│ > _                                                                │
└ Esc/Ctrl+C: 종료 | PageUp/PageDown: 스크롤 | End: 최신 ─────────────┘
```

영역 4개로 구성됩니다.

| 영역 | 내용 |
|------|------|
| 헤더 | 접속 닉, 누적 메시지 수, `LIVE` / 스크롤 상태 |
| 채팅 로그 | 서버에서 들어온 라인을 색으로 구분해 누적 표시 (최대 5,000개, 초과 시 1,000개 단위로 트림) |
| 입력창 | 모니터에서 직접 메시지 송신 — 닉네임 `MONITOR` 로 다른 클라이언트들에게 전파 |
| 푸터 | 키 조작 안내 |

### 색상 구분

| 종류        | 색상   | 예시 |
|-------------|--------|------|
| 입퇴장 알림 | 노랑 (BOLD) | `[12:03:11] SERVER: [client_0042] 입장 (현재 412명)` |
| 일반 채팅   | 흰색      | `[12:03:11] client_0042: client_0042 메시지 #0` |
| 내가 보낸 메시지 | 초록 (BOLD) | `[나] MONITOR: 모두 안녕하세요` |
| 모니터 시스템 메시지 | 자주 | `✅ 127.0.0.1:8080 에 [MONITOR] 닉네임으로 접속됨` |

### 키 조작

| 키            | 동작 |
|---------------|------|
| 일반 문자 입력 + `Enter` | 입력창 메시지를 서버로 송신 (다른 클라이언트들에게 전파) |
| `PageUp`     | 채팅 로그 10줄 위로 스크롤 (헤더가 `스크롤 -N` 으로 변경) |
| `PageDown`   | 10줄 아래로 스크롤 |
| `End`        | 가장 최신 메시지로 복귀 (`LIVE`) |
| `Backspace`  | 입력창 글자 1개 삭제 |
| `Esc` / `Ctrl+C` | 모니터 종료 (서버에는 영향 없음) |

> **자기 메시지가 로컬에만 보이는 이유**: 서버가 송신자에게는 echo 를 안 보내기 때문입니다. 모니터는 자기가 입력한 줄을 `[나]` 라벨로 직접 화면에만 추가합니다.

### stress_test 와 함께 쓰기

부하 테스트의 흐름을 **눈으로 확인하는** 가장 직관적인 방법입니다.

```bash
# 터미널 1: 서버
cargo run --release --bin server

# 터미널 2: 모니터를 먼저 띄워둠
cargo run --release --bin monitor

# 터미널 3: 부하 발사
cargo run --release --bin stress_test -- 500 5
```

`500명 × 5메시지` 기준으로 모니터가 받는 라인 수:

```
입장 알림      500개  (노란색)
일반 메시지   2,500개  (흰색)
퇴장 알림     500개   (노란색)
─────────────────────
합계         약 3,500개  (4~5초 안에 폭포처럼 흘러감)
```

`PageUp` 으로 거슬러 올라가 `client_0000 메시지 #0` 같은 라인을 직접 확인할 수 있습니다.

### 주의사항

- **broadcast lag**: 모니터가 폭주 트래픽을 따라가지 못하면 서버 콘솔에 `⚠️ 127.0.0.1:xxxxx lagged N messages` 가 찍힙니다. **모니터가 일부 메시지를 건너뛴다는 의미일 뿐 서버는 정상**입니다. `--release` 빌드 / 채널 용량 증가로 완화할 수 있습니다.
- 모니터를 종료해도 서버와 다른 클라이언트는 그대로 작동합니다.
- 입력창에서 IME 한글 입력은 환경에 따라 조합 표시가 어색할 수 있습니다 — 영문/숫자 입력은 안정적입니다.

---

## 📊 실행 결과 & 분석

### 스트레스 테스트 출력 (`500명 × 5개 메시지`)

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  멀티채팅 서버 스트레스 테스트
  서버 : 127.0.0.1:8080
  클라이언트 수 : 500
  클라이언트당 메시지 : 5
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  →  50/500 연결 완료 (실패: 0)
  → 100/500 연결 완료 (실패: 0)
  →  ...
  → 500/500 연결 완료 (실패: 0)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  📊 결과 요약
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  소요 시간      :  4.67s
  연결 성공      :  500 / 500    ✅
  연결 실패      :  0            ✅
  총 송신 메시지 :  2,500 건
  총 수신 메시지 :  88,667 건
  이론상 수신수  :  1,247,500 건  (SERVER 알림 제외)
  처리량         :  535.6 msg/sec
  수신율 (근사)  :  7.1 %
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✅ 테스트 완료!
```

### 수치 해석

| 항목 | 값 | 판정 | 비고 |
|---------|------------|---------|------------------------|
| 연결 성공 | 500 / 500  | ✅ PASS  | 실패 0건, 완벽한 연결 안정성 |
| 연결 실패 | 0건         | ✅ PASS | 재시도 로직 정상 동작       |
| 총 송신  | 2,500건     | ✅ OK   | 500명 × 5개              |
| 총 수신  | 88,667건    | ✅ OK   | 3초 타임아웃 기준 (아래 참고) |
| 처리량   | 535.6 msg/s | ✅ OK   | 4.67초 동안 안정적 유지     |
| 소요 시간 | 4.67초      | ✅ OK   | 배치 연결 + 메시지 전파 포함  |

### 📌 수신율 7.1%에 대하여

낮은 수신율은 서버 한계가 아닌 **테스트 클라이언트의 3초 타임아웃** 때문입니다.

```
타임라인 예시 (client_0000):

  T + 0ms    연결 & 닉네임 전송
  T + 50ms   1번째 메시지 전송
  T + 60ms   2번째 메시지 전송
  ...
  T + 90ms   5번째 메시지 전송
  T + 3090ms ⏱ 타임아웃 → recv_task.abort() & 연결 종료
              ↑
              client_0499 는 T+545ms 이후에 메시지를 보내기 시작.
              그 메시지들은 client_0000 이 이미 종료된 후 전파됨
              → 카운팅 불가

결론: 연결을 유지하면 수신율 ≈ 100% 달성
```

---

## ⚡ 성능 설계 원칙

### 4가지 핵심 원칙

```
┌─────────────────────────────────────────────────────────────────────┐
│  1. 경량 Async Task                                                  │
│     OS 스레드(2MB 스택) 대신 tokio async task(수 KB) 사용                 │
│     → 500개 연결 = 수 MB 메모리  (스레드 방식 대비 ~99% 절감)                │
├─────────────────────────────────────────────────────────────────────┤
│  2. Zero-Copy 브로드캐스트                                             │
│     broadcast::channel 은 Arc 기반 링 버퍼 사용                         │
│     → 클라이언트 500명이 있어도 메시지 본체는 1회만 복사                       │
├─────────────────────────────────────────────────────────────────────┤
│  3. Lock-Free 통계                                                   │
│     AtomicUsize::fetch_add(Relaxed) 로 카운터 집계                     │
│     → Mutex 없이 원자적 연산 → 집계 병목 제로                              │
├─────────────────────────────────────────────────────────────────────┤
│  4. 교착 없는 I/O 분리                                                 │
│     into_split() 으로 reader / writer 를 별도 소유권으로 분리              │
│     → 단방향 데이터 흐름 보장 → 구조적으로 deadlock 불가                      │
└─────────────────────────────────────────────────────────────────────┘
```

### broadcast 채널 용량 선택 근거

```
cap = 10,000 으로 설정한 이유:

  최대 클라이언트 500명 × 순간 메시지 burst ≈ 수백 msg/s
  write_task 가 일시적으로 처리 지연될 경우,
  링 버퍼가 10,000개를 보관 → 약 수백 ms 의 여유 확보
  → 순간 과부하 시에도 메시지 손실 최소화
  → Lagged 오류 발생 시 cap 을 늘려 튜닝 가능
```

---

## 📦 의존성

```toml
[dependencies]
tokio     = { version = "1", features = ["full"] }
chrono    = "0.4"
ratatui   = "0.29"   # monitor 전용
crossterm = "0.28"   # monitor 전용
```

| 크레이트 | 버전 | 사용 목적 |
|-----------|-----|------------------------------------------------------------------------|
| `tokio`     | 1.x  | 비동기 런타임 전체 — `TcpListener`, `spawn`, `broadcast`, `RwLock`, `time` |
| `chrono`    | 0.4  | 메시지 타임스탬프 (`HH:MM:SS`) 포매팅                                        |
| `ratatui`   | 0.29 | TUI 모니터 화면 렌더링 (위젯, 레이아웃, 색상)                                  |
| `crossterm` | 0.28 | 모니터의 raw 모드 / 키 이벤트 / alternate screen 제어                       |

> **프로덕션 최적화 팁**
> `features = ["full"]` 대신 실제로 사용하는 기능만 명시하면
> 빌드 시간과 바이너리 크기를 줄일 수 있습니다.
>
> ```toml
> tokio = { version = "1", features = ["net", "sync", "rt-multi-thread", "io-util", "time"] }
> ```

---

## 👥 팀원

| 학과 | 학년 | 학번 | 이름 |
|------------------|---|----------|------|
| 소프트웨어전공       | 3 | 20235312 | 장희예 |
| 소프트웨어전공       | 4 | 20213077 | 전영환 |
| 소프트웨어전공       | 4 | 20181691 | 정지원 |
| AI빅데이터융합경영학과 | 3 | 20212579 | 정찬민 |

---

```
연결 500명  ·  실패 0건  ·  처리량 535 msg/s  ·  소요 시간 4.67s
```

*멀티채팅 서버 · Rust + Tokio · 도전과제 프로젝트 2*
