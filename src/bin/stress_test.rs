/// stress_test.rs
/// 500명 클라이언트를 동시에 띄워 서버를 부하 테스트합니다.
///
/// 사용법:
///   cargo run --bin stress_test -- [클라이언트수] [메시지수]
///   cargo run --bin stress_test -- 500 10
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

// ─────────────────────────────────────────────
//  전역 카운터
// ─────────────────────────────────────────────
static SENT: AtomicUsize = AtomicUsize::new(0);
static RECV: AtomicUsize = AtomicUsize::new(0);
static CONNECTED: AtomicUsize = AtomicUsize::new(0);
static FAILED: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n_clients: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(500);
    let n_messages: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
    let server_addr = args.get(3).cloned().unwrap_or("127.0.0.1:8080".to_string());

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  멀티채팅 서버 스트레스 테스트");
    println!("  서버 : {server_addr}");
    println!("  클라이언트 수 : {n_clients}");
    println!("  클라이언트당 메시지 : {n_messages}");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();

    // ── 검증용 Arc 카운터 ──
    let expected_recv = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(n_clients);

    for i in 0..n_clients {
        let addr = server_addr.clone();
        let exp = expected_recv.clone();

        let handle = tokio::spawn(async move {
            // 연결 재시도 (서버 준비 대기)
            let stream = match connect_with_retry(&addr, 5).await {
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

            // 약간의 지연 후 메시지 전송 (모두 동시에 쏘면 커넥션 과부하)
            sleep(Duration::from_millis((i % 100) as u64 * 5)).await;

            // ── 메시지 전송 ──
            for j in 0..n_messages {
                let msg = format!("client_{:04} 메시지 #{}\n", i, j);
                if writer.write_all(msg.as_bytes()).await.is_err() {
                    break;
                }
                SENT.fetch_add(1, Ordering::Relaxed);
                // 다른 클라이언트들이 이 메시지를 받을 것으로 기대
                exp.fetch_add(n_clients - 1, Ordering::Relaxed);

                // 약간의 간격
                sleep(Duration::from_millis(10)).await;
            }

            // 수신 완료 대기 (최대 3초)
            sleep(Duration::from_millis(3000)).await;
            recv_task.abort();
        });

        handles.push(handle);

        // 한번에 너무 많이 연결하지 않도록 배치 처리
        if (i + 1) % 50 == 0 {
            sleep(Duration::from_millis(50)).await;
            let c = CONNECTED.load(Ordering::Relaxed);
            let f = FAILED.load(Ordering::Relaxed);
            println!("  → {}/{n_clients} 연결 완료 (실패: {f})", c);
        }
    }

    // 모든 태스크 완료 대기
    for h in handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed();
    let sent = SENT.load(Ordering::Relaxed);
    let recv = RECV.load(Ordering::Relaxed);
    let connected = CONNECTED.load(Ordering::Relaxed);
    let failed = FAILED.load(Ordering::Relaxed);
    // 이론상 수신 수 = sent * (n_clients - 1) 이지만 SERVER 알림도 포함되므로 근사치
    let expected = sent * (n_clients.saturating_sub(1));

    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  📊 결과 요약");
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
        println!("  ⚠️  일부 연결 실패. 서버가 실행 중인지 확인하세요.");
    } else {
        println!("  ✅ 테스트 완료!");
    }
}

async fn connect_with_retry(addr: &str, retries: usize) -> Option<TcpStream> {
    for attempt in 0..retries {
        match TcpStream::connect(addr).await {
            Ok(s) => return Some(s),
            Err(_) => {
                if attempt + 1 < retries {
                    sleep(Duration::from_millis(200 * (attempt as u64 + 1))).await;
                }
            }
        }
    }
    None
}
