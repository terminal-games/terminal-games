use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime},
};

use http_body_util::{BodyExt, Empty};
use hyper::{Request, Version, body::Bytes};
use hyper_util::rt::TokioIo;
use ratatui::{
    Terminal,
    style::{Color, Stylize},
    widgets::Paragraph,
};
use tachyonfx::{Interpolation, Motion, fx};
use terminal_games_sdk::{
    app,
    network::Conn,
    peer::{self, PeerId},
    terminal::{TerminalGamesBackend, TerminalReader},
    terminput,
};

fn format_time(time: SystemTime) -> String {
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let total_seconds = duration.as_secs();
    let seconds = total_seconds % 60;
    let minutes = (total_seconds / 60) % 60;
    let hours = (total_seconds / 3600) % 24;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

#[derive(Clone)]
// An Executor that uses the tokio runtime.
pub struct TokioExecutor;

// Implement the `hyper::rt::Executor` trait for `TokioExecutor` so that it can be used to spawn
// tasks in the hyper runtime.
// An Executor allows us to manage execution of tasks which can help us improve the efficiency and
// scalability of the server.
impl<F> hyper::rt::Executor<F> for TokioExecutor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn(fut);
    }
}

#[derive(Clone)]
struct PeerMessage {
    from: PeerId,
    message: String,
    #[allow(dead_code)]
    time: SystemTime,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let mut terminal = Terminal::new(TerminalGamesBackend::new(std::io::stdout()))?;
    terminal.clear()?;
    std::io::stdout().write(b"\x1b[?1003h")?;

    let conn_done = Arc::new(AtomicBool::new(false));

    let peer_id = peer::current_id();
    let target_peer_id = std::env::var("APP_ARGS")
        .ok()
        .and_then(|s| s.parse::<PeerId>().ok());

    let (parts, body) = {
        let url = "https://example.com".parse::<hyper::Uri>().unwrap();

        let host = url.host().expect("uri has no host");
        let port = url.port_u16().unwrap_or(443);

        let address = format!("{}:{}", host, port);
        let stream = Conn::dial(&address, true)?;
        let io = TokioIo::new(stream);
        let (mut sender, conn) =
            match hyper::client::conn::http2::handshake(TokioExecutor, io).await {
                Ok(result) => result,
                Err(e) => {
                    eprintln!("HTTP/2 handshake failed: {:?}", e);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        format!("HTTP/2 handshake failed: {}", e),
                    ));
                }
            };
        let conn_done_clone = conn_done.clone();
        tokio::task::spawn(async move {
            if let Err(err) = conn.await {
                println!("Connection failed: {:?}", err);
            }
            conn_done_clone.store(true, Ordering::SeqCst);
        });

        let req = Request::builder()
            .version(Version::HTTP_2)
            .uri(url)
            .body(Empty::<Bytes>::new())
            .unwrap();

        let res = match sender.send_request(req).await {
            Ok(res) => res,
            Err(e) => {
                println!("Failed to send request: {:?}", e);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    format!("Failed to send request: {}", e),
                ));
            }
        };
        let (parts, body) = res.into_parts();
        let body = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                println!("Failed to collect body: {:?}", e);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("Failed to collect body: {}", e),
                ));
            }
        };
        (parts, body)
    };

    let mut terminal_reader = TerminalReader {};
    let mut peer_messages = peer::MessageReader::new();
    let mut messages = vec![];
    let mut peers_list = Vec::<PeerId>::new();
    let mut last_peer_update = Instant::now();

    let mut effects: tachyonfx::EffectManager<()> = tachyonfx::EffectManager::default();

    let c = Color::Green;
    let timer = (1000, Interpolation::QuadInOut);
    let fx = fx::repeating(fx::ping_pong(fx::sweep_in(
        Motion::LeftToRight,
        10,
        0,
        c,
        timer,
    )));
    effects.add_effect(fx);

    let start = Instant::now();
    let mut frame_counter = 1;
    let mut last_event = None;
    let mut last_frame = Instant::now();
    'outer: loop {
        let elapsed = last_frame.elapsed();
        last_frame = Instant::now();

        let mut event_counter = 0;
        for event in &mut terminal_reader {
            event_counter += 1;
            if let Some(key_event) = event.as_key() {
                match key_event {
                    terminput::key!(terminput::KeyCode::Char('q')) => break 'outer,
                    terminput::key!(terminput::KeyCode::Char('n')) => {
                        app::change_app("kitchen-sink")?
                    }
                    terminput::key!(terminput::KeyCode::Char('f')) => {
                        let size = terminal.size().unwrap();
                        std::io::stdout()
                            .write(format!("\x1b[{};2HA", size.height + 1).as_bytes())?;
                    }
                    terminput::key!(terminput::KeyCode::Char('p')) => {
                        if let Some(target_peer_id) = target_peer_id {
                            let message = format!(
                                "Hello from {} at {}",
                                peer_id.to_string(),
                                format_time(SystemTime::now())
                            );
                            let _ = target_peer_id.send(message.as_bytes());
                        }
                    }
                    _ => {}
                }
            }
            last_event = Some(event);
        }

        if app::next_app_ready() {
            break;
        }

        if app::graceful_shutdown_poll() {
            break;
        }

        for msg in &mut peer_messages {
            let message_str = String::from_utf8_lossy(&msg.data).to_string();
            let peer_msg = PeerMessage {
                from: msg.from,
                message: message_str.clone(),
                time: SystemTime::now(),
            };

            messages.push(peer_msg);
            if messages.len() > 10 {
                messages.remove(0);
            }

            if message_str != "pong" {
                let _ = msg.from.send(b"pong".as_slice());
            }
        }

        if last_peer_update.elapsed().as_secs() >= 1 {
            if let Ok(peers) = peer::list() {
                peers_list = peers;
                peers_list.sort();
            }
            last_peer_update = Instant::now();
        }

        let peer_messages_text = {
            if messages.is_empty() {
                "No messages yet".to_string()
            } else {
                messages
                    .iter()
                    .map(|msg| {
                        format!(
                            "[{}] From {}: {}",
                            format_time(msg.time),
                            msg.from.to_string(),
                            msg.message
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        };

        let peers_list_text = {
            if peers_list.is_empty() {
                "No peers connected".to_string()
            } else {
                peers_list
                    .iter()
                    .map(|p| {
                        let is_current = *p == peer_id;
                        let marker = if is_current { "→ " } else { "  " };
                        format!("{}{}", marker, p.to_string())
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        };

        terminal.draw(|frame| {
            let area = frame.area();
            frame.render_widget(
                Paragraph::new(format!(
                    "Hello World!\ncounter={}\nlast_event={:#?}\nparts={:#?}\nbody={:#?}\nconn_done={:#?}\nfps={}\nevent_counter={}\n{}\n\nPeer ID: {}\nPress 'p' to send a message to peer {}\n\nConnected Peers ({}):\n{}\n\nRecent Messages:\n{}",
                    frame_counter,
                    last_event,
                    parts,
                    body,
                    conn_done,
                    frame_counter as f64 / start.elapsed().as_secs_f64(),
                    event_counter,
                    "hello there".red().on_red(),
                    peer_id,
                    target_peer_id.as_ref()
                        .map(|v| format!("Some({})", v))
                        .unwrap_or_else(|| "None".into()),
                    peers_list.len(),
                    peers_list_text,
                    peer_messages_text,
                )),
                area,
            );

            effects.process_effects(elapsed.into(), frame.buffer_mut(), area);
        })?;
        frame_counter += 1;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    std::io::stdout().write(b"\x1b[?1003l")?;
    Ok(())
}
