/// monitor.rs
/// 서버를 건드리지 않고 "일반 클라이언트"로 접속해서
/// 채팅 트래픽을 실시간으로 시각화하는 TUI 모니터.
///
/// 사용법:
///   cargo run --bin monitor
///   cargo run --bin monitor -- 127.0.0.1:8080 MONITOR
///
/// 키 조작:
///   - 입력 후 Enter : 관리자 메시지 전송 (닉=MONITOR)
///   - PageUp/PageDown : 채팅 로그 스크롤
///   - End          : 가장 최근 메시지로 이동
///   - Esc / Ctrl+C : 종료
use std::error::Error;
use std::io;
use std::time::Duration;

use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};

use ratatui::backend::{Backend, CrosstermBackend};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};
use ratatui::Terminal;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

// ─────────────────────────────────────────────
//  상수
// ─────────────────────────────────────────────
const MAX_MESSAGES: usize = 5_000;
const TRIM_CHUNK: usize = 1_000;
const TICK_MS: u64 = 50;

// ─────────────────────────────────────────────
//  메시지 종류 (색상 구분용)
// ─────────────────────────────────────────────
#[derive(Clone, Debug)]
enum Kind {
    Server, // SERVER 알림 (입장/퇴장)
    Chat,   // 일반 사용자 채팅
    SelfMe, // 내가 보낸 관리자 메시지
    Sys,    // 모니터 자체 시스템 메시지 (연결/오류 등)
}

#[derive(Clone, Debug)]
struct Msg {
    kind: Kind,
    text: String,
}

// ─────────────────────────────────────────────
//  진입점
// ─────────────────────────────────────────────
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let nick = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "MONITOR".to_string());

    // 1) 서버 접속
    let stream = match TcpStream::connect(&server_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("❌ 서버 접속 실패 ({server_addr}): {e}");
            return Ok(());
        }
    };
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);

    // 2) 닉네임 송신
    writer
        .write_all(format!("{}\n", nick).as_bytes())
        .await?;

    // 3) UI ↔ Net 채널
    let (tx_ui, rx_ui) = mpsc::unbounded_channel::<Msg>();
    let (tx_send, mut rx_send) = mpsc::unbounded_channel::<String>();

    let _ = tx_ui.send(Msg {
        kind: Kind::Sys,
        text: format!("✅ {server_addr} 에 [{}] 닉네임으로 접속됨", nick),
    });

    // 4) 수신 태스크: 서버 라인 → UI 채널
    let tx_ui_recv = tx_ui.clone();
    tokio::spawn(async move {
        let mut line = String::new();
        loop {
            line.clear();
            match buf_reader.read_line(&mut line).await {
                Ok(0) => {
                    let _ = tx_ui_recv.send(Msg {
                        kind: Kind::Sys,
                        text: "⚠️  서버 연결이 종료되었습니다.".to_string(),
                    });
                    break;
                }
                Ok(_) => {
                    let s = line.trim_end_matches(['\r', '\n']).to_string();
                    if s.is_empty() {
                        continue;
                    }
                    let kind = if s.contains("SERVER") {
                        Kind::Server
                    } else {
                        Kind::Chat
                    };
                    let _ = tx_ui_recv.send(Msg { kind, text: s });
                }
                Err(e) => {
                    let _ = tx_ui_recv.send(Msg {
                        kind: Kind::Sys,
                        text: format!("⚠️  읽기 오류: {e}"),
                    });
                    break;
                }
            }
        }
    });

    // 5) 송신 태스크: UI 입력 → 서버
    tokio::spawn(async move {
        while let Some(msg) = rx_send.recv().await {
            let line = format!("{}\n", msg);
            if writer.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    // 6) 터미널 초기화
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // 7) 앱 실행
    let result = run_app(&mut terminal, rx_ui, tx_send, &nick).await;

    // 8) 정리
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(e) = result {
        eprintln!("오류: {e}");
    }
    Ok(())
}

// ─────────────────────────────────────────────
//  메인 이벤트 루프
// ─────────────────────────────────────────────
async fn run_app<B: Backend>(
    terminal: &mut Terminal<B>,
    mut rx_ui: mpsc::UnboundedReceiver<Msg>,
    tx_send: mpsc::UnboundedSender<String>,
    nick: &str,
) -> Result<(), Box<dyn Error>> {
    let mut messages: Vec<Msg> = Vec::new();
    let mut input = String::new();
    // 스크롤: 끝에서부터 몇 개를 거슬러 올라갔는지 (0 = 가장 최근 고정)
    let mut scroll_back: usize = 0;

    loop {
        // ── 1) 새 메시지 수집 ──
        let mut new_arrived = false;
        while let Ok(m) = rx_ui.try_recv() {
            messages.push(m);
            new_arrived = true;
            if messages.len() > MAX_MESSAGES {
                messages.drain(0..TRIM_CHUNK);
                if scroll_back > messages.len() {
                    scroll_back = 0;
                }
            }
        }
        // 새 메시지가 오는 동안 스크롤이 0이면 자동 추적, 아니면 그대로 (스크롤 잠금)
        if new_arrived && scroll_back > 0 {
            // 잠금 상태에서 새 메시지가 오면 보고 있던 위치 유지: 그냥 둠
        }

        // ── 2) 그리기 ──
        terminal.draw(|f| {
            let area = f.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1), // 헤더
                    Constraint::Min(3),    // 채팅 로그
                    Constraint::Length(3), // 입력창
                    Constraint::Length(1), // 푸터
                ])
                .split(area);

            // ── 헤더 ──
            let header_text = format!(
                " 🖥  서버 모니터  |  닉: {}  |  메시지: {}  |  {}",
                nick,
                messages.len(),
                if scroll_back == 0 {
                    "LIVE".to_string()
                } else {
                    format!("스크롤 -{}", scroll_back)
                }
            );
            let header = Paragraph::new(header_text).style(
                Style::default()
                    .fg(Color::White)
                    .bg(Color::Blue)
                    .add_modifier(Modifier::BOLD),
            );
            f.render_widget(header, chunks[0]);

            // ── 채팅 로그 ──
            let log_area = chunks[1];
            let inner_h = log_area.height.saturating_sub(2) as usize; // borders
            let total = messages.len();
            let end = total.saturating_sub(scroll_back);
            let start = end.saturating_sub(inner_h);

            let items: Vec<ListItem> = messages[start..end]
                .iter()
                .map(|m| {
                    let style = match m.kind {
                        Kind::Server => Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                        Kind::SelfMe => Style::default()
                            .fg(Color::Green)
                            .add_modifier(Modifier::BOLD),
                        Kind::Sys => Style::default().fg(Color::Magenta),
                        Kind::Chat => Style::default().fg(Color::White),
                    };
                    ListItem::new(Line::from(Span::styled(m.text.clone(), style)))
                })
                .collect();

            let title = format!(
                " 채팅 로그 ({}~{} / {}) ",
                start + 1,
                end,
                total
            );
            let list = List::new(items).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .border_style(Style::default().fg(Color::DarkGray)),
            );
            f.render_widget(list, log_area);

            // ── 입력창 ──
            let input_area = chunks[2];
            let input_box = Paragraph::new(input.as_str())
                .style(Style::default().fg(Color::Cyan))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" 입력 (Enter: 전송) ")
                        .border_style(Style::default().fg(Color::Cyan)),
                );
            f.render_widget(input_box, input_area);

            // 커서 위치 (한글은 폭 2로 근사)
            let visual_w: u16 = input
                .chars()
                .map(|c| if c.is_ascii() { 1u16 } else { 2u16 })
                .sum();
            let cx = input_area.x + 1 + visual_w.min(input_area.width.saturating_sub(2));
            let cy = input_area.y + 1;
            f.set_cursor_position((cx, cy));

            // ── 푸터 ──
            let footer = Paragraph::new(
                " Esc/Ctrl+C: 종료  |  PageUp/PageDown: 스크롤  |  End: 최신 ",
            )
            .style(Style::default().fg(Color::DarkGray));
            f.render_widget(footer, chunks[3]);
        })?;

        // ── 3) 키 이벤트 ──
        if event::poll(Duration::from_millis(TICK_MS))? {
            if let Event::Key(key) = event::read()? {
                // Windows에서는 Press/Release가 둘 다 옴 - Press만 처리 (Linux도 안전)
                if key.kind != crossterm::event::KeyEventKind::Press {
                    continue;
                }

                match key.code {
                    KeyCode::Esc => break,
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                    KeyCode::Enter => {
                        let msg = std::mem::take(&mut input);
                        let msg = msg.trim().to_string();
                        if !msg.is_empty() {
                            // 로컬 표시 (서버는 자기 자신에게는 echo 안 함)
                            messages.push(Msg {
                                kind: Kind::SelfMe,
                                text: format!("[나] {}: {}", nick, &msg),
                            });
                            let _ = tx_send.send(msg);
                            // 새 메시지 보냈으니 LIVE 모드로 복귀
                            scroll_back = 0;
                        }
                    }
                    KeyCode::Backspace => {
                        input.pop();
                    }
                    KeyCode::PageUp => {
                        scroll_back = (scroll_back + 10).min(messages.len());
                    }
                    KeyCode::PageDown => {
                        scroll_back = scroll_back.saturating_sub(10);
                    }
                    KeyCode::End => {
                        scroll_back = 0;
                    }
                    KeyCode::Char(c) => {
                        input.push(c);
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}
