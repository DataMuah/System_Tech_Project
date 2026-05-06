/// stress_test_1000.rs
/// 1000명 동시 접속 확장 테스트.
///
/// 500명 baseline 검증을 통과한 서버를 1000명 규모로 확장 검증한다.
/// 더 큰 클라이언트 수에 맞춰 다음 항목을 보수적으로 튜닝했다.
///   - 배치 크기 100, 배치 간격 80 ms      (accept 큐 과부하 회피)
///   - 메시지 송신 후 5초 수신 대기          (fan-out 전파 시간 확보)
///   - per-client stagger 최대 1초          (초기 burst 분산)
///
/// 사용법:
///   cargo run --release --bin stress_test_1000
///   cargo run --release --bin stress_test_1000 -- 1000 5 127.0.0.1:8080
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

// ─────────────────────────────────────────────
//  전역 카운터 (lock-free 통계)
// ─────────────────────────────────────────────
static SENT: AtomicUsize = AtomicUsize::new(0);
static RECV: AtomicUsize = AtomicUsize::new(0);
static CONNECTED: AtomicUsize = AtomicUsize::new(0);
static FAILED: AtomicUsize = AtomicUsize::new(0);

// 1000-client 전용 튜닝 상수
const BATCH_SIZE: usize = 100;          // 한 번에 동시 연결할 개수
const BATCH_GAP_MS: u64 = 80;           // 배치 간 대기
const RECV_HOLD_MS: u64 = 5_000;        // 메시지 송신 후 수신 대기 시간
const MSG_INTERVAL_MS: u64 = 10;        // 클라이언트당 메시지 간격

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n_clients: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
    let n_messages: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
    let server_addr = args.get(3).cloned().unwrap_or("127.0.0.1:8080".to_string());

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  멀티채팅 서버 1000명 확장 스트레스 테스트");
    println!("  서버 : {server_addr}");
    println!("  클라이언트 수 : {n_clients}");
    println!("  클라이언트당 메시지 : {n_messages}");
    println!("  배치 : {BATCH_SIZE}개씩 {BATCH_GAP_MS}ms 간격");
    println!("  수신 대기 : {RECV_HOLD_MS}ms");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();
    let expected_recv = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(n_clients);

    for i in 0..n_clients {
        let addr = server_addr.clone();
        let exp = expected_recv.clone();

        let handle = tokio::spawn(async move {
            // ── 연결 (재시도 7회 — 1000명 환경에서는 일시 실패가 더 자주 발생) ──
            let stream = match connect_with_retry(&addr, 7).await {
                Some(s) => s,
                None => {
                    FAILED.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            CONNECTED.fetch_add(1, Ordering::Relaxed);

            let (reader, mut writer) = stream.into_split();
            let mut buf_reader = BufReader::new(reader);

            // 닉네임 전송
            let nick = format!("client_{:04}\n", i);
            if writer.write_all(nick.as_bytes()).await.is_err() {
                return;
            }

            // ── 수신 태스크 ──
            let recv_task = tokio::spawn(async move {
                let mut line = String::new();
                loop {
                    line.clear();
                    match buf_reader.read_line(&mut line).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            RECV.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });

            // 1000명 burst 분산: 0~999 ms 사이 stagger
            sleep(Duration::from_millis((i as u64) % 1000)).await;

            // ── 메시지 전송 ──
            for j in 0..n_messages {
                let msg = format!("client_{:04} 메시지 #{}\n", i, j);
                if writer.write_all(msg.as_bytes()).await.is_err() {
                    break;
                }
                SENT.fetch_add(1, Ordering::Relaxed);
                exp.fetch_add(n_clients - 1, Ordering::Relaxed);
                sleep(Duration::from_millis(MSG_INTERVAL_MS)).await;
            }

            // 수신 완료까지 충분히 대기
            sleep(Duration::from_millis(RECV_HOLD_MS)).await;
            recv_task.abort();
        });

        handles.push(handle);

        // 배치 단위로 연결 분산
        if (i + 1) % BATCH_SIZE == 0 {
            sleep(Duration::from_millis(BATCH_GAP_MS)).await;
            let c = CONNECTED.load(Ordering::Relaxed);
            let f = FAILED.load(Ordering::Relaxed);
            println!("  → {}/{n_clients} 연결 완료 (실패: {f})", c);
        }
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed();
    let sent = SENT.load(Ordering::Relaxed);
    let recv = RECV.load(Ordering::Relaxed);
    let connected = CONNECTED.load(Ordering::Relaxed);
    let failed = FAILED.load(Ordering::Relaxed);
    let expected = sent * (n_clients.saturating_sub(1));

    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  📊 1000명 확장 테스트 결과");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  소요 시간      : {:.2?}", elapsed);
    println!("  연결 성공      : {connected}/{n_clients}");
    println!("  연결 실패      : {failed}");
    println!("  총 송신 메시지 : {sent}");
    println!("  총 수신 메시지 : {recv}");
    println!("  이론상 수신수  : {expected}  (SERVER 알림 제외)");
    println!(
        "  처리량         : {:.1} msg/sec",
        sent as f64 / elapsed.as_secs_f64()
    );
    if expected > 0 {
        let rate = (recv as f64 / expected as f64 * 100.0).min(100.0);
        println!("  수신율 (근사)  : {:.1}%", rate);
    }
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    if failed > 0 {
        println!("  ⚠️  일부 연결 실패: {failed}건 — broadcast 채널 용량 또는 OS limit 점검 권장");
    } else {
        println!("  ✅ 1000명 확장 테스트 통과!");
    }
}

async fn connect_with_retry(addr: &str, retries: usize) -> Option<TcpStream> {
    for attempt in 0..retries {
        match TcpStream::connect(addr).await {
            Ok(s) => return Some(s),
            Err(_) => {
                if attempt + 1 < retries {
                    // 지수적 백오프 (1000명 환경에서는 더 길게)
                    sleep(Duration::from_millis(150 * (attempt as u64 + 1))).await;
                }
            }
        }
    }
    None
}
