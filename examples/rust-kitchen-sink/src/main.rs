use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use http_body_util::{BodyExt, Empty};
use hyper::{Request, Version, body::Bytes};
use hyper_util::rt::TokioIo;
use ratatui::{
    Terminal,
    style::{Color, Stylize},
    symbols::border,
    text::{Line, Span, Text},
    widgets::{Block, Paragraph, Widget},
};
use tachyonfx::{Interpolation, Motion, fx};
use terminal_games_sdk::{
    app,
    network::Conn,
    terminal::{TerminalGamesBackend, TerminalReader},
    terminput,
};

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let mut terminal = Terminal::new(TerminalGamesBackend::new(std::io::stdout()))?;
    terminal.clear()?;
    std::io::stdout().write(b"\x1b[?1003h")?;

    let conn_done = Arc::new(AtomicBool::new(false));

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

    let mut effects: tachyonfx::EffectManager<()> = tachyonfx::EffectManager::default();

    // let bg = Color::from_u32(0x282c34);
    // let fx = fx::fade_from_fg(bg, (1000, tachyonfx::Interpolation::QuadOut));
    // let c = Color::from_u32(0x282c34);
    let c = Color::Green;
    let timer = (1000, Interpolation::QuadInOut);
    let fx = fx::sweep_in(Motion::LeftToRight, 10, 5, c, timer);
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
                    _ => {}
                }
            }
            last_event = Some(event);
        }

        if app::next_app_ready() {
            break;
        }

        terminal.draw(|frame| {
            let area = frame.area();
            // frame.render_widget(
            //     Paragraph::new(format!(
            //         "Hello World!\ncounter={}\nlast_event={:#?}\nparts={:#?}\nbody={:#?}\nconn_done={:#?}\nfps={}\nevent_counter={}\n{}",
            //         frame_counter,
            //         last_event,
            //         parts,
            //         body,
            //         conn_done,
            //         frame_counter as f64 / start.elapsed().as_secs_f64(),
            //         event_counter,
            //         "hello there".red().on_red(),
            //     )),
            //     area,
            // );
            let title = Line::from(" Counter App Tutorial ".bold());
            let instructions = Line::from(vec![
                " Decrement ".into(),
                "<Left>".blue().bold(),
                " Increment ".into(),
                "<Right>".blue().bold(),
                " Quit ".into(),
                "<Q> ".blue().bold(),
            ]);
            let block = Block::bordered()
                .title(title.centered())
                .title_bottom(instructions.centered())
                .border_set(border::THICK);

            let counter_text = Text::from(vec![Line::from(vec![
                "Value: ".into(),
                1.to_string().yellow(),
            ])]);

            Paragraph::new(counter_text)
                .centered()
                .block(block)
                .render(area, frame.buffer_mut());
            effects.process_effects(elapsed.into(), frame.buffer_mut(), area);
        })?;
        frame_counter += 1;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    std::io::stdout().write(b"\x1b[?1003l")?;
    Ok(())
}
