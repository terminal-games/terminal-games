use std::{io::Write, time::Instant};

use ratatui::{Terminal, widgets::Paragraph};
use terminal_games_sdk::{
    app,
    terminal::{TerminalGamesBackend, TerminalReader},
    terminput,
};

terminal_games_sdk::embed_manifest!();

fn main() -> std::io::Result<()> {
    let mut terminal = Terminal::new(TerminalGamesBackend::new(std::io::stdout()))?;
    terminal.clear()?;
    std::io::stdout().write(b"\x1b[?1003h")?;

    let mut terminal_reader = TerminalReader {};

    let start = Instant::now();
    let mut frame_counter = 1;
    let mut last_event = None;
    let mut fps_window_start = Instant::now();
    let mut fps_window_frames: u32 = 0;
    let mut recent_fps = 0.0f64;
    'outer: loop {
        if app::graceful_shutdown_poll()? {
            break;
        }

        let mut event_counter = 0;
        for event in &mut terminal_reader {
            let event = event?;
            event_counter += 1;
            if let Some(key_event) = event.as_key() {
                match key_event {
                    terminput::key!(terminput::KeyCode::Char('q')) => break 'outer,
                    _ => {}
                }
            }
            last_event = Some(event);
        }

        fps_window_frames += 1;
        let fps_window_elapsed = fps_window_start.elapsed();
        if fps_window_elapsed >= std::time::Duration::from_millis(250) {
            recent_fps = fps_window_frames as f64 / fps_window_elapsed.as_secs_f64();
            fps_window_frames = 0;
            fps_window_start = Instant::now();
        }

        terminal.draw(|frame| {
            let area = frame.area();
            frame.render_widget(
                Paragraph::new(format!(
                    "Hello World!\ncounter={}\nlast_event={:#?}\nrecent_fps_250ms={:.1}\nevent_counter={}\nuptime_secs={:.2}\n",
                    frame_counter,
                    last_event,
                    recent_fps,
                    event_counter,
                    start.elapsed().as_secs_f64(),
                )),
                area,
            );
        })?;
        frame_counter += 1;
    }
    std::io::stdout().write(b"\x1b[?1003l")?;
    Ok(())
}
