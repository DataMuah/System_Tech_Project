use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};

// ─────────────────────────────────────────────
//  공유 상태
// ─────────────────────────────────────────────
type ClientMap = Arc<RwLock<HashMap<SocketAddr, String>>>;

#[derive(Clone, Debug)]
struct Message {
    from: String,
    addr: SocketAddr,
    body: String,
    timestamp: String,
}

// ─────────────────────────────────────────────
//  서버 진입점
// ─────────────────────────────────────────────
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:8080").await.expect("포트 바인딩 실패");
    println!("🚀 채팅 서버 시작 → 0.0.0.0:8080");

    // broadcast: 모든 클라이언트에게 메시지 전송 (용량 10_000)
    let (tx, _rx) = broadcast::channel::<Message>(10_000);
    let tx = Arc::new(tx);

    // 접속 중인 클라이언트 목록: addr → 닉네임
    let clients: ClientMap = Arc::new(RwLock::new(HashMap::new()));

    // 통계
    let total_connected = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total_messages = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    loop {
        let (socket, addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("accept 오류: {e}");
                continue;
            }
        };

        let tx = tx.clone();
        let rx = tx.subscribe();
        let clients = clients.clone();
        let connected = total_connected.clone();
        let msg_count = total_messages.clone();

        tokio::spawn(async move {
            handle_client(socket, addr, tx, rx, clients, connected, msg_count).await;
        });
    }
}

// ─────────────────────────────────────────────
//  클라이언트 핸들러
// ─────────────────────────────────────────────
async fn handle_client(
    socket: TcpStream,
    addr: SocketAddr,
    tx: Arc<broadcast::Sender<Message>>,
    mut rx: broadcast::Receiver<Message>,
    clients: ClientMap,
    connected: Arc<std::sync::atomic::AtomicUsize>,
    msg_count: Arc<std::sync::atomic::AtomicUsize>,
) {
    let (reader, mut writer) = socket.into_split();
    let mut buf_reader = BufReader::new(reader);

    // ── 닉네임 수신 ──────────────────────────
    let mut nick = String::new();
    if buf_reader.read_line(&mut nick).await.unwrap_or(0) == 0 {
        return;
    }
    let nick = nick.trim().to_string();
    if nick.is_empty() {
        return;
    }

    // 클라이언트 등록
    {
        let mut map = clients.write().await;
        map.insert(addr, nick.clone());
    }
    let count = connected.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

    let join_msg = format!("[{nick}] 입장 (현재 {}명)", count);
    println!("{join_msg}");
    let _ = tx.send(Message {
        from: "SERVER".into(),
        addr,
        body: join_msg,
        timestamp: now(),
    });

    // ── 송신 태스크 (broadcast → 이 클라이언트 소켓) ──
    let write_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    // 자신이 보낸 메시지는 echo 제외
                    if msg.addr == addr {
                        continue;
                    }
                    let line = format!("[{}] {}: {}\n", msg.timestamp, msg.from, msg.body);
                    if writer.write_all(line.as_bytes()).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("⚠️  {addr} lagged {n} messages");
                }
                Err(_) => break,
            }
        }
    });

    // ── 수신 루프 (소켓 → broadcast) ──────────
    let mut line = String::new();
    loop {
        line.clear();
        match buf_reader.read_line(&mut line).await {
            Ok(0) => break, // 연결 종료
            Ok(_) => {
                let body = line.trim().to_string();
                if body.is_empty() {
                    continue;
                }
                msg_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = tx.send(Message {
                    from: nick.clone(),
                    addr,
                    body,
                    timestamp: now(),
                });
            }
            Err(e) => {
                eprintln!("읽기 오류 {addr}: {e}");
                break;
            }
        }
    }

    // ── 퇴장 처리 ─────────────────────────────
    write_task.abort();
    {
        let mut map = clients.write().await;
        map.remove(&addr);
    }
    let count = connected.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
    let leave_msg = format!("[{nick}] 퇴장 (현재 {}명)", count);
    println!("{leave_msg}");
    let _ = tx.send(Message {
        from: "SERVER".into(),
        addr,
        body: leave_msg,
        timestamp: now(),
    });
}

fn now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let h = (secs % 86400) / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}
