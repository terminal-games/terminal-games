use std::{io::Write, time::Instant};

use ratatui::{Terminal, widgets::Paragraph};
use terminal_games_sdk::{
    app,
    terminal::{TerminalGamesBackend, TerminalReader},
    terminput,
};

#[used]
static TERMINAL_GAMES_MANIFEST: &[u8] = include_bytes!("../terminal-games.json");

fn main() -> std::io::Result<()> {
    let mut terminal = Terminal::new(TerminalGamesBackend::new(std::io::stdout()))?;
    terminal.clear()?;
    std::io::stdout().write(b"\x1b[?1003h")?;

    let mut terminal_reader = TerminalReader {};

    let start = Instant::now();
    let mut frame_counter = 1;
    let mut last_event = None;
    'outer: loop {
        if app::graceful_shutdown_poll() {
            break;
        }

        let mut event_counter = 0;
        for event in &mut terminal_reader {
            event_counter += 1;
            if let Some(key_event) = event.as_key() {
                match key_event {
                    terminput::key!(terminput::KeyCode::Char('q')) => break 'outer,
                    _ => {}
                }
            }
            last_event = Some(event);
        }

        terminal.draw(|frame| {
            let area = frame.area();
            frame.render_widget(
                Paragraph::new(format!(
                    "Hello World!\ncounter={}\nlast_event={:#?}\nfps={}\nevent_counter={}\n",
                    frame_counter,
                    last_event,
                    frame_counter as f64 / start.elapsed().as_secs_f64(),
                    event_counter,
                )),
                area,
            );
        })?;
        frame_counter += 1;
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    std::io::stdout().write(b"\x1b[?1003l")?;
    Ok(())
}
