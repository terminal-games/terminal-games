use std::{
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
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
    audio::{OggVorbisResource, Resource, mixer},
    network::Conn,
    peer::{self, PeerId},
    terminal::{TerminalGamesBackend, TerminalReader},
    terminput,
};

const SONG_DATA: &[u8] = include_bytes!("../../kitchen-sink/Mesmerizing Galaxy Loop.ogg");

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

fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let seconds = total_seconds % 60;
    let minutes = total_seconds / 60;
    format!("{}:{:02}", minutes, seconds)
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

    let song_resource =
        Arc::new(OggVorbisResource::new(SONG_DATA).expect("Failed to decode OGG Vorbis audio"));
    let song = song_resource.new_instance();
    song.set_loop(true);
    song.set_volume(0.8);
    song.play();
    let mut audio_volume = 0.8f32;

    let peer_id = peer::current_id();
    let target_peer_id = std::env::var("APP_ARGS")
        .ok()
        .and_then(|s| s.parse::<PeerId>().ok());

    let http_response: Arc<Mutex<Option<Result<(hyper::http::response::Parts, Bytes), String>>>> =
        Arc::new(Mutex::new(None));
    let http_request_pending = Arc::new(AtomicBool::new(false));

    let mut terminal_reader = TerminalReader {};
    let mut peer_messages = peer::MessageReader::new();
    let mut messages = vec![];
    let mut peers_list = Vec::<PeerId>::new();
    let mut last_peer_update = Instant::now();

    let mut effects: tachyonfx::EffectManager<()> = tachyonfx::EffectManager::default();

    let c = Color::Green;
    let timer = (2000, Interpolation::QuadInOut);
    let fx = fx::sweep_in(Motion::LeftToRight, 10, 0, c, timer);
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
                    terminput::key!(terminput::KeyCode::Char(' ')) => {
                        if song.is_playing() {
                            song.pause();
                        } else {
                            song.play();
                        }
                    }
                    terminput::key!(terminput::KeyCode::Char('+'))
                    | terminput::key!(terminput::KeyCode::Char('=')) => {
                        audio_volume = (audio_volume + 0.1).min(2.0);
                        song.set_volume(audio_volume);
                    }
                    terminput::key!(terminput::KeyCode::Char('-'))
                    | terminput::key!(terminput::KeyCode::Char('_')) => {
                        audio_volume = (audio_volume - 0.1).max(0.0);
                        song.set_volume(audio_volume);
                    }
                    terminput::key!(terminput::KeyCode::Char('m')) => {
                        if audio_volume > 0.0 {
                            audio_volume = 0.0;
                        } else {
                            audio_volume = 0.8;
                        }
                        song.set_volume(audio_volume);
                    }
                    terminput::key!(terminput::KeyCode::Char('r')) => {
                        if !http_request_pending.load(Ordering::SeqCst) {
                            http_request_pending.store(true, Ordering::SeqCst);
                            let http_response_clone = http_response.clone();
                            let http_request_pending_clone = http_request_pending.clone();
                            let conn_done_clone = conn_done.clone();
                            tokio::spawn(async move {
                                let result = async {
                                    let url = "https://example.com".parse::<hyper::Uri>().unwrap();
                                    let host = url.host().expect("uri has no host");
                                    let port = url.port_u16().unwrap_or(443);
                                    let address = format!("{}:{}", host, port);

                                    let dial_result = Conn::dial_async(&address, true)
                                        .await
                                        .map_err(|e| e.to_string())?;
                                    let io = TokioIo::new(dial_result.conn);

                                    let (mut sender, conn) =
                                        hyper::client::conn::http2::handshake(TokioExecutor, io)
                                            .await
                                            .map_err(|e| format!("HTTP/2 handshake failed: {}", e))?;

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

                                    let res = sender
                                        .send_request(req)
                                        .await
                                        .map_err(|e| format!("Failed to send request: {}", e))?;

                                    let (parts, body) = res.into_parts();
                                    let body = body
                                        .collect()
                                        .await
                                        .map_err(|e| format!("Failed to collect body: {}", e))?
                                        .to_bytes();

                                    Ok::<_, String>((parts, body))
                                }
                                .await;

                                *http_response_clone.lock().unwrap() = Some(result);
                                http_request_pending_clone.store(false, Ordering::SeqCst);
                            });
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

        let net_info_str = app::network_info()
            .map(|n| {
                let rtt = if n.latency_ms >= 0 {
                    format!("{}ms", n.latency_ms)
                } else {
                    "N/A".into()
                };
                let throttled = n
                    .last_throttled
                    .map(format_time)
                    .unwrap_or_else(|| "never".into());
                format!(
                    "↑{:.0} ↓{:.0} B/s RTT {} throttled: {}",
                    n.bytes_per_sec_in, n.bytes_per_sec_out, rtt, throttled
                )
            })
            .unwrap_or_else(|_| "N/A".into());

        let audio_status = {
            let play_state = if song.is_playing() {
                "Playing"
            } else {
                "Paused"
            };
            let current = format_duration(song.current_time());
            let total = format_duration(song.duration());
            format!(
                "{} | Volume: {:.0}% | Position: {} / {}",
                play_state,
                audio_volume * 100.0,
                current,
                total
            )
        };

        let http_status = {
            let response = http_response.lock().unwrap();
            if http_request_pending.load(Ordering::SeqCst) {
                "HTTP: Loading...".to_string()
            } else {
                match response.as_ref() {
                    None => "HTTP: Press 'r' to make request".to_string(),
                    Some(Ok((parts, body))) => {
                        format!("HTTP: {} | Body: {} bytes\n{:#?}", parts.status, body.len(), parts)
                    }
                    Some(Err(e)) => format!("HTTP Error: {}", e),
                }
            }
        };

        terminal.draw(|frame| {
            let area = frame.area();
            frame.render_widget(
                Paragraph::new(format!(
                    "Hello World!\ncounter={}\nlast_event={:#?}\n{}\nconn_done={:#?}\nfps={}\nevent_counter={}\n{}\n\nAudio: {}\n  [Space]=Play/Pause  [+/-]=Volume  [M]=Mute\n\nNetwork: {}\n\nPeer ID: {}\nPress 'p' to send a message to peer {}\n\nConnected Peers ({}):\n{}\n\nRecent Messages:\n{}",
                    frame_counter,
                    last_event,
                    http_status,
                    conn_done,
                    frame_counter as f64 / start.elapsed().as_secs_f64(),
                    event_counter,
                    "hello there".red().on_red(),
                    audio_status,
                    net_info_str,
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

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        mixer().tick();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        mixer().tick();
    }
    std::io::stdout().write(b"\x1b[?1003l")?;
    Ok(())
}
