// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{collections::VecDeque, io::Read, thread, time::Instant};

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use reqwest::header::{AUTHORIZATION, HeaderValue};
use terminal_games::control::{SessionSummary, SpyControlMessage};
use terminal_games::palette::{self, Color as PaletteColor};
use terminal_games::terminal_profile::{TerminalColorMode, TerminalProfile};
use terminput::{
    Event as TerminputEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton,
    MouseEvent, MouseEventKind, ScrollDirection,
};
use tokio::io::AsyncWriteExt;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use unicode_width::UnicodeWidthStr;

use super::super::{AdminSessionSpyArgs, load_api, parse_session_ref};
use crate::config::format_duration;

pub(super) async fn session_spy(args: AdminSessionSpyArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let (region, local_id) = parse_session_ref(&args.session_id)?;
    let base_url = api.region_url(&region).await?;
    let session = api
        .session_summary(&region, local_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("unknown session '{}'", args.session_id))?;
    let show_input = !args.hide_input;
    let mut status_bar = SpyStatusBar::new(session, args.rw, show_input);
    let mut input_overlay = SpyInputOverlay::default();
    let ws_url = websocket_url(
        &base_url,
        &format!("/control/admin/session/spy/{local_id}"),
        Some(&format!("rw={}&show_input=true", args.rw)),
    )?;

    let mut request = ws_url.into_client_request()?;
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", api.profile.password))?,
    );
    let (ws_stream, _) = tokio_tungstenite::connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();

    let mut stdout = tokio::io::stdout();
    crossterm::terminal::enable_raw_mode()?;
    crossterm::execute!(
        std::io::stdout(),
        crossterm::terminal::EnterAlternateScreen,
        crossterm::cursor::Hide
    )?;

    let result = async {
        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(16);
        thread::spawn(move || {
            let stdin = std::io::stdin();
            let mut buf = [0u8; 4096];
            loop {
                match stdin.lock().read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(read_len) => {
                        if stdin_tx.blocking_send(buf[..read_len].to_vec()).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        let mut vt: Option<avt::Vt> = None;
        let mut mouse_modes = std::collections::BTreeSet::new();
        let mut local_mouse_capture = false;
        let mut local_size = crossterm::terminal::size()?;
        let mut exit_message: Option<String> = None;
        let mut resize_tick = tokio::time::interval(std::time::Duration::from_millis(100));
        let mut status_tick = tokio::time::interval(std::time::Duration::from_secs(1));
        let mut overlay_tick = tokio::time::interval(std::time::Duration::from_millis(120));
        loop {
            tokio::select! {
                message = read.next() => {
                    let Some(message) = message else {
                        if exit_message.is_none() {
                            exit_message = Some("Spy connection closed.".to_string());
                        }
                        break;
                    };
                    match message {
                        Ok(message) => match message {
                        Message::Binary(data) => {
                            let vt = vt.as_mut().ok_or_else(|| anyhow::anyhow!("missing spy init frame"))?;
                            vt.feed_str(&String::from_utf8_lossy(&data));
                            if args.rw {
                                sync_mouse_capture(
                                    scan_mouse_modes(&String::from_utf8_lossy(&data), &mut mouse_modes),
                                    &mut local_mouse_capture,
                                )?;
                            }
                            render_spy_view(vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                        }
                        Message::Text(text) => {
                            match serde_json::from_str::<SpyControlMessage>(&text)? {
                                SpyControlMessage::Init { cols, rows, dump } => {
                                    let mut next_vt = avt::Vt::new((cols as usize).max(1), (rows as usize).max(1));
                                    next_vt.feed_str(&dump);
                                    if args.rw {
                                        mouse_modes.clear();
                                        sync_mouse_capture(
                                            scan_mouse_modes(&dump, &mut mouse_modes),
                                            &mut local_mouse_capture,
                                        )?;
                                    }
                                    render_spy_view(&next_vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                                    vt = Some(next_vt);
                                }
                                SpyControlMessage::Resize { cols, rows } => {
                                    let vt = vt.as_mut().ok_or_else(|| anyhow::anyhow!("missing spy init frame"))?;
                                    vt.resize((cols as usize).max(1), (rows as usize).max(1));
                                    render_spy_view(vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                                }
                                SpyControlMessage::Metadata { username } => {
                                    status_bar.set_username(username);
                                    render_spy_view_if_ready(
                                        vt.as_ref(),
                                        local_size,
                                        &status_bar,
                                        &input_overlay,
                                        &mut stdout,
                                    )
                                    .await?;
                                }
                                SpyControlMessage::Input { data } => {
                                    input_overlay.ingest(&data);
                                    if status_bar.show_input_overlay() {
                                        render_spy_view_if_ready(
                                            vt.as_ref(),
                                            local_size,
                                            &status_bar,
                                            &input_overlay,
                                            &mut stdout,
                                        )
                                        .await?;
                                    }
                                }
                                SpyControlMessage::Closed { reason_slug: _, message } => {
                                    exit_message = Some(format!("Spy ended: {message}"));
                                    break;
                                }
                            }
                        }
                        Message::Close(frame) => {
                            exit_message = Some(match frame {
                                Some(frame) if !frame.reason.is_empty() => {
                                    format!("Spy ended: {}", frame.reason)
                                }
                                _ => "Spy connection closed.".to_string(),
                            });
                            break;
                        }
                        _ => {}
                        },
                        Err(error) => {
                            exit_message = Some(format!(
                                "Spy connection error: {}",
                                format_websocket_error(&error)
                            ));
                            break;
                        }
                    }
                }
                data = stdin_rx.recv() => {
                    let Some(data) = data else { break };
                    let Some(forwarded) = apply_spy_local_shortcuts(
                        &data,
                        &mut status_bar,
                        vt.as_ref(),
                        local_size,
                        &input_overlay,
                        &mut stdout,
                    ).await? else {
                        break;
                    };
                    if args.rw && !forwarded.is_empty() {
                        write.send(Message::Binary(forwarded.into())).await?;
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    break;
                }
                _ = resize_tick.tick() => {
                    let next_size = crossterm::terminal::size()?;
                    if next_size != local_size {
                        local_size = next_size;
                        render_spy_view_if_ready(
                            vt.as_ref(),
                            local_size,
                            &status_bar,
                            &input_overlay,
                            &mut stdout,
                        )
                        .await?;
                    }
                }
                _ = status_tick.tick() => {
                    render_spy_view_if_ready(
                        vt.as_ref(),
                        local_size,
                        &status_bar,
                        &input_overlay,
                        &mut stdout,
                    )
                    .await?;
                }
                _ = overlay_tick.tick() => {
                    if input_overlay.tick() && status_bar.show_input_overlay() {
                        render_spy_view_if_ready(
                            vt.as_ref(),
                            local_size,
                            &status_bar,
                            &input_overlay,
                            &mut stdout,
                        )
                        .await?;
                    }
                }
            }
        }
        Result::<Option<String>>::Ok(exit_message)
    }
    .await;

    let _ = crossterm::execute!(
        std::io::stdout(),
        crossterm::event::DisableMouseCapture,
        crossterm::cursor::Show,
        crossterm::terminal::LeaveAlternateScreen
    );
    let _ = crossterm::terminal::disable_raw_mode();
    match result {
        Ok(Some(message)) => {
            eprintln!("{message}");
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(error) => Err(error),
    }
}

fn websocket_url(base_url: &str, path: &str, query: Option<&str>) -> Result<String> {
    let mut url = reqwest::Url::parse(base_url)?;
    url.set_scheme(match url.scheme() {
        "https" => "wss",
        _ => "ws",
    })
    .map_err(|_| anyhow::anyhow!("invalid websocket scheme"))?;
    url.set_path(path);
    url.set_query(query);
    Ok(url.to_string())
}

async fn render_spy_view(
    vt: &avt::Vt,
    local_size: (u16, u16),
    status_bar: &SpyStatusBar,
    input_overlay: &SpyInputOverlay,
    stdout: &mut tokio::io::Stdout,
) -> Result<()> {
    let width = local_size.0 as usize;
    let height = local_size.1 as usize;
    let content_height = height.saturating_sub(1);
    let app_size = vt.size();
    let mut frame = String::from("\x1b[H");
    if content_height > 0 {
        render_spy_app_area(&mut frame, vt, width, content_height, status_bar);
    }

    if height > 0 {
        frame.push_str(&format!("\x1b[{};1H", height));
        frame.push_str(&status_bar.render(width, vt.size()));
    }
    if status_bar.show_input_overlay() {
        render_spy_input_overlay(
            &mut frame,
            input_overlay,
            status_bar,
            width,
            content_height,
            app_size,
        );
    }

    let cursor = vt.cursor();
    if cursor.visible && cursor.row < content_height && cursor.col < width {
        frame.push_str(&format!(
            "\x1b[{};{}H\x1b[?25h",
            cursor.row + 1,
            cursor.col + 1
        ));
    } else {
        frame.push_str("\x1b[?25l");
    }
    stdout.write_all(frame.as_bytes()).await?;
    stdout.flush().await?;
    Ok(())
}

fn render_spy_app_area(
    frame: &mut String,
    vt: &avt::Vt,
    width: usize,
    content_height: usize,
    status_bar: &SpyStatusBar,
) {
    let (fg, bg) = status_bar.spy_margin_colors();
    let fill = format!(
        "\x1b[{};{}m/\x1b[0m",
        palette::render_color_code(fg, false),
        palette::render_color_code(bg, true),
    );
    let lines = vt.view().collect::<Vec<_>>();
    for row in 0..content_height {
        let (line, shown) = lines
            .get(row)
            .map(|line| spy_line_ansi(line, width))
            .unwrap_or_default();
        frame.push_str(&format!(
            "\x1b[{};1H{}{}\x1b[0m\x1b[K",
            row + 1,
            line,
            fill.repeat(width.saturating_sub(shown))
        ));
    }
}

fn spy_line_ansi(line: &avt::Line, width: usize) -> (String, usize) {
    let mut out = String::new();
    let mut shown = 0usize;
    for cells in line.chunks(|left, right| left.pen() != right.pen()) {
        if shown >= width {
            break;
        }
        let pen = cells[0].pen();
        out.push_str("\x1b[0");
        if let Some(color) = pen.foreground() {
            match color {
                avt::Color::Indexed(index) if index < 8 => {
                    out.push_str(&format!(";{}", 30 + index))
                }
                avt::Color::Indexed(index) if index < 16 => {
                    out.push_str(&format!(";{}", 82 + index))
                }
                avt::Color::Indexed(index) => out.push_str(&format!(";38;5;{index}")),
                avt::Color::RGB(rgb) => {
                    out.push_str(&format!(";38;2;{};{};{}", rgb.r, rgb.g, rgb.b))
                }
            }
        }
        if let Some(color) = pen.background() {
            match color {
                avt::Color::Indexed(index) if index < 8 => {
                    out.push_str(&format!(";{}", 40 + index))
                }
                avt::Color::Indexed(index) if index < 16 => {
                    out.push_str(&format!(";{}", 92 + index))
                }
                avt::Color::Indexed(index) => out.push_str(&format!(";48;5;{index}")),
                avt::Color::RGB(rgb) => {
                    out.push_str(&format!(";48;2;{};{};{}", rgb.r, rgb.g, rgb.b))
                }
            }
        }
        if pen.is_bold() {
            out.push_str(";1");
        } else if pen.is_faint() {
            out.push_str(";2");
        }
        if pen.is_italic() {
            out.push_str(";3");
        }
        if pen.is_underline() {
            out.push_str(";4");
        }
        if pen.is_blink() {
            out.push_str(";5");
        }
        if pen.is_inverse() {
            out.push_str(";7");
        }
        if pen.is_strikethrough() {
            out.push_str(";9");
        }
        out.push('m');
        for cell in cells {
            let cell_width = cell.width().max(1);
            if cell.width() == 0 || shown + cell_width > width {
                continue;
            }
            out.push(cell.char());
            shown += cell_width;
        }
    }
    (out, shown)
}

async fn render_spy_view_if_ready(
    vt: Option<&avt::Vt>,
    local_size: (u16, u16),
    status_bar: &SpyStatusBar,
    input_overlay: &SpyInputOverlay,
    stdout: &mut tokio::io::Stdout,
) -> Result<()> {
    if let Some(vt) = vt {
        render_spy_view(vt, local_size, status_bar, input_overlay, stdout).await?;
    }
    Ok(())
}

fn render_spy_input_overlay(
    frame: &mut String,
    input_overlay: &SpyInputOverlay,
    status_bar: &SpyStatusBar,
    width: usize,
    content_height: usize,
    app_size: (usize, usize),
) {
    if content_height == 0 {
        return;
    }
    if let Some((col, row)) = input_overlay.mouse_position()
        && col < width
        && col < app_size.0
        && row < content_height
        && row < app_size.1
    {
        frame.push_str(&format!(
            "\x1b[{};{}H\x1b[38;2;255;0;0m•\x1b[0m",
            row + 1,
            col + 1
        ));
    }
    let items = input_overlay.visible_items(status_bar.profile);
    if items.is_empty() || width == 0 {
        return;
    }
    let total_width =
        items.iter().map(|item| item.width).sum::<usize>() + items.len().saturating_sub(1);
    let start_col = width.saturating_sub(total_width) / 2 + 1;
    let overlay_row = content_height.saturating_sub(1).max(1);
    frame.push_str(&format!("\x1b[{};{}H", overlay_row, start_col));
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            frame.push(' ');
        }
        frame.push_str(&item.ansi);
    }
    frame.push_str("\x1b[0m");
}

fn scan_mouse_modes(text: &str, mouse_modes: &mut std::collections::BTreeSet<u16>) -> bool {
    let bytes = text.as_bytes();
    let mut idx = 0usize;
    while idx + 3 < bytes.len() {
        if bytes[idx] != 0x1b || bytes[idx + 1] != b'[' || bytes[idx + 2] != b'?' {
            idx += 1;
            continue;
        }
        let mut end = idx + 3;
        while end < bytes.len() && !matches!(bytes[end], b'h' | b'l') {
            end += 1;
        }
        if end >= bytes.len() {
            break;
        }
        let Ok(params) = std::str::from_utf8(&bytes[idx + 3..end]) else {
            idx = end + 1;
            continue;
        };
        let enable = bytes[end] == b'h';
        for value in params
            .split(';')
            .filter_map(|value| value.parse::<u16>().ok())
        {
            if !matches!(value, 1000 | 1002 | 1003 | 1005 | 1006 | 1015) {
                continue;
            }
            if enable {
                mouse_modes.insert(value);
            } else {
                mouse_modes.remove(&value);
            }
        }
        idx = end + 1;
    }
    !mouse_modes.is_empty()
}

fn sync_mouse_capture(enabled: bool, local_mouse_capture: &mut bool) -> Result<()> {
    if enabled == *local_mouse_capture {
        return Ok(());
    }
    if enabled {
        crossterm::execute!(std::io::stdout(), crossterm::event::EnableMouseCapture)?;
    } else {
        crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture)?;
    }
    *local_mouse_capture = enabled;
    Ok(())
}

fn format_websocket_error(error: &tokio_tungstenite::tungstenite::Error) -> String {
    match error {
        tokio_tungstenite::tungstenite::Error::Protocol(
            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => "connection reset without a closing handshake".to_string(),
        tokio_tungstenite::tungstenite::Error::ConnectionClosed => "connection closed".to_string(),
        _ => error.to_string(),
    }
}

async fn apply_spy_local_shortcuts(
    data: &[u8],
    status_bar: &mut SpyStatusBar,
    vt: Option<&avt::Vt>,
    local_size: (u16, u16),
    input_overlay: &SpyInputOverlay,
    stdout: &mut tokio::io::Stdout,
) -> Result<Option<Vec<u8>>> {
    let mut forwarded = data.to_vec();
    let mut toggled = false;
    if let Ok(Some(TerminputEvent::Key(key))) = TerminputEvent::parse_from(data)
        && matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
        && key.modifiers.contains(KeyModifiers::CTRL)
    {
        match key.code {
            KeyCode::Char('c') | KeyCode::Char('C') => return Ok(None),
            KeyCode::Char('t') | KeyCode::Char('T') => {
                status_bar.toggle_input_overlay();
                toggled = true;
                forwarded.clear();
            }
            _ => {}
        }
    } else {
        match data {
            [0x03] => return Ok(None),
            [0x14] => {
                status_bar.toggle_input_overlay();
                toggled = true;
                forwarded.clear();
            }
            _ => {}
        }
    }
    if toggled && let Some(vt) = vt {
        render_spy_view(vt, local_size, status_bar, input_overlay, stdout).await?;
    }
    Ok(Some(forwarded))
}

struct SpyStatusBar {
    session: SessionSummary,
    read_write: bool,
    show_input_overlay: bool,
    attached_at: std::time::Instant,
    profile: TerminalProfile,
}

impl SpyStatusBar {
    fn new(session: SessionSummary, read_write: bool, show_input_overlay: bool) -> Self {
        Self {
            session,
            read_write,
            show_input_overlay,
            attached_at: std::time::Instant::now(),
            profile: TerminalProfile::from_term(
                std::env::var("TERM").ok().as_deref(),
                std::env::var("COLORTERM").ok().as_deref(),
            ),
        }
    }

    fn render(&self, width: usize, app_size: (usize, usize)) -> String {
        if width == 0 {
            return String::new();
        }
        let theme = SpyTheme::new(self.profile);
        let duration = self.session.duration_seconds + self.attached_at.elapsed().as_secs();
        let label = format!(" spy {} ", self.session.session_id);
        let label_width = visible_width(&label);
        if width <= label_width {
            return format!(
                "\x1b[{};{}m{}\x1b[K\x1b[0m",
                palette::render_color_code(theme.on_pink, false),
                palette::render_color_code(theme.pink, true),
                label.chars().take(width).collect::<String>(),
            );
        }
        let gap = " ";
        let remaining_width = width - label_width;
        let input_hint_value = if self.show_input_overlay {
            "hide input"
        } else {
            "show input"
        };
        let hint_plain = format!("ctrl+t: {input_hint_value} ctrl+c: exit");
        let hint = StatusField {
            width: visible_width(&hint_plain),
            ansi: format!(
                "\x1b[{}mctrl+t:\x1b[{}m {} \x1b[{}mctrl+c:\x1b[{}m exit",
                palette::render_color_code(theme.muted, false),
                palette::render_color_code(theme.text, false),
                input_hint_value,
                palette::render_color_code(theme.muted, false),
                palette::render_color_code(theme.text, false),
            ),
        };
        let hint_total_width = hint.width + 1;
        let reserve_hint = remaining_width > hint_total_width + 24;
        let left_budget = if reserve_hint {
            remaining_width.saturating_sub(hint_total_width)
        } else {
            remaining_width
        };
        let fields = [
            status_field(
                theme.muted,
                theme.text,
                "mode",
                if self.read_write { "rw" } else { "readonly" },
            ),
            status_field(
                theme.muted,
                theme.text,
                "uid",
                &self
                    .session
                    .user_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ),
            status_field(
                theme.muted,
                theme.text,
                "transport",
                &self.session.transport,
            ),
            status_field(
                theme.muted,
                theme.text,
                "duration",
                &format_duration(duration),
            ),
            status_field(
                theme.muted,
                theme.text,
                "size",
                &format!("{}x{}", app_size.0, app_size.1),
            ),
            status_field(theme.muted, theme.text, "user", &self.session.username),
            status_field(theme.muted, theme.text, "region", &self.session.region_id),
            status_field(theme.muted, theme.text, "ip", &self.session.ip_address),
        ];
        let mut left = String::new();
        let mut used = 0usize;
        let leading_padding = " ";
        if left_budget > 0 {
            left.push_str(leading_padding);
            used += visible_width(leading_padding);
        }
        for (index, field) in fields.into_iter().enumerate() {
            let gap_width = if index == 0 { 0 } else { visible_width(gap) };
            if used + gap_width + field.width > left_budget {
                break;
            }
            if index > 0 {
                left.push_str(gap);
                used += gap_width;
            }
            left.push_str(&field.ansi);
            used += field.width;
        }
        let mut right = String::new();
        let padding = if reserve_hint {
            let middle = remaining_width.saturating_sub(used + hint_total_width);
            right.push_str(&hint.ansi);
            right.push(' ');
            " ".repeat(middle)
        } else {
            " ".repeat(remaining_width.saturating_sub(used))
        };
        format!(
            "\x1b[{};{}m{}\x1b[{};{}m{}\x1b[K\x1b[0m",
            palette::render_color_code(theme.on_pink, false),
            palette::render_color_code(theme.pink, true),
            label,
            palette::render_color_code(theme.text, false),
            palette::render_color_code(theme.surface, true),
            format_args!("{left}{padding}{right}"),
        )
    }

    fn set_username(&mut self, username: String) {
        self.session.username = username;
    }

    fn toggle_input_overlay(&mut self) {
        self.show_input_overlay = !self.show_input_overlay;
    }

    fn show_input_overlay(&self) -> bool {
        self.show_input_overlay
    }

    fn spy_margin_colors(&self) -> (PaletteColor, PaletteColor) {
        let theme = SpyTheme::new(self.profile);
        (theme.muted, theme.surface)
    }
}

#[derive(Clone, Copy)]
struct SpyTheme {
    pink: PaletteColor,
    on_pink: PaletteColor,
    surface: PaletteColor,
    text: PaletteColor,
    muted: PaletteColor,
}

impl SpyTheme {
    fn new(profile: TerminalProfile) -> Self {
        Self {
            pink: fixed_theme_color(profile, ThemeColor::new((0xec, 0x48, 0x99), 205, 13)),
            on_pink: fixed_theme_color(profile, ThemeColor::new((0xff, 0xf7, 0xfb), 255, 15)),
            surface: theme_color(
                profile,
                ThemeColor::new((0x26, 0x26, 0x26), 235, 0),
                ThemeColor::new((0xf5, 0xf5, 0xf5), 255, 7),
            ),
            text: theme_color(
                profile,
                ThemeColor::new((0xf5, 0xf5, 0xf5), 255, 15),
                ThemeColor::new((0x26, 0x26, 0x26), 235, 0),
            ),
            muted: theme_color(
                profile,
                ThemeColor::new((0xa3, 0xa3, 0xa3), 248, 8),
                ThemeColor::new((0x73, 0x73, 0x73), 243, 8),
            ),
        }
    }
}

#[derive(Clone, Copy)]
struct ThemeColor {
    true_color: (u8, u8, u8),
    color256: u8,
    color16: u8,
}

impl ThemeColor {
    const fn new(true_color: (u8, u8, u8), color256: u8, color16: u8) -> Self {
        Self {
            true_color,
            color256,
            color16,
        }
    }
}

#[derive(Default)]
struct SpyInputOverlay {
    items: VecDeque<SpyInputItem>,
    mouse: Option<SpyMousePointer>,
}

struct SpyInputItem {
    label: String,
    created_at: Instant,
}

struct SpyMousePointer {
    col: usize,
    row: usize,
    seen_at: Instant,
}

struct RenderedOverlayItem {
    ansi: String,
    width: usize,
}

impl SpyInputOverlay {
    fn ingest(&mut self, data: &[u8]) {
        let Ok(Some(event)) = TerminputEvent::parse_from(data) else {
            return;
        };
        let now = Instant::now();
        match event {
            TerminputEvent::Key(key)
                if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
            {
                self.push_item(format_key_event(key), now);
            }
            TerminputEvent::Mouse(mouse) => {
                self.mouse = Some(SpyMousePointer {
                    col: mouse.column as usize,
                    row: mouse.row as usize,
                    seen_at: now,
                });
                if let Some(label) = format_mouse_event(mouse) {
                    self.push_item(label, now);
                }
            }
            TerminputEvent::Paste(text) => {
                self.push_item(format!("paste {}", text.chars().count()), now);
            }
            _ => {}
        }
    }

    fn tick(&mut self) -> bool {
        let before_len = self.items.len();
        let had_mouse = self.mouse.is_some();
        let now = Instant::now();
        self.items.retain(|item| {
            now.duration_since(item.created_at) <= std::time::Duration::from_secs(4)
        });
        if self.mouse.as_ref().is_some_and(|mouse| {
            now.duration_since(mouse.seen_at) > std::time::Duration::from_secs(2)
        }) {
            self.mouse = None;
        }
        before_len != self.items.len() || had_mouse != self.mouse.is_some()
    }

    fn visible_items(&self, profile: TerminalProfile) -> Vec<RenderedOverlayItem> {
        let now = Instant::now();
        self.items
            .iter()
            .filter_map(|item| {
                let age = now.duration_since(item.created_at);
                let step = (age.as_millis() / 900) as usize;
                let color = overlay_fade_color(profile, step)?;
                Some(RenderedOverlayItem {
                    width: visible_width(&item.label),
                    ansi: format!(
                        "\x1b[{}m{}\x1b[0m",
                        palette::render_color_code(color, false),
                        item.label
                    ),
                })
            })
            .collect()
    }

    fn mouse_position(&self) -> Option<(usize, usize)> {
        self.mouse.as_ref().map(|mouse| (mouse.col, mouse.row))
    }

    fn push_item(&mut self, label: String, created_at: Instant) {
        self.items.push_back(SpyInputItem { label, created_at });
        while self.items.len() > 6 {
            self.items.pop_front();
        }
    }
}

fn overlay_fade_color(profile: TerminalProfile, step: usize) -> Option<PaletteColor> {
    let step = step.min(4);
    const DARK: [ThemeColor; 4] = [
        ThemeColor::new((0xf5, 0xf5, 0xf5), 255, 15),
        ThemeColor::new((0xd4, 0xd4, 0xd4), 252, 7),
        ThemeColor::new((0xa3, 0xa3, 0xa3), 248, 8),
        ThemeColor::new((0x73, 0x73, 0x73), 243, 8),
    ];
    const LIGHT: [ThemeColor; 4] = [
        ThemeColor::new((0x26, 0x26, 0x26), 235, 0),
        ThemeColor::new((0x52, 0x52, 0x52), 240, 7),
        ThemeColor::new((0x73, 0x73, 0x73), 243, 8),
        ThemeColor::new((0xa3, 0xa3, 0xa3), 248, 8),
    ];
    Some(theme_color(
        profile,
        DARK.get(step).copied()?,
        LIGHT.get(step).copied()?,
    ))
}

fn fixed_theme_color(profile: TerminalProfile, color: ThemeColor) -> PaletteColor {
    theme_color(profile, color, color)
}

fn theme_color(profile: TerminalProfile, dark: ThemeColor, light: ThemeColor) -> PaletteColor {
    let color = if profile.has_dark_background() {
        dark
    } else {
        light
    };
    match profile.color_mode {
        TerminalColorMode::TrueColor => {
            PaletteColor::Rgb(color.true_color.0, color.true_color.1, color.true_color.2)
        }
        TerminalColorMode::Color256 => PaletteColor::Fixed(color.color256),
        TerminalColorMode::Color16 => PaletteColor::Basic(color.color16),
    }
}

fn format_key_event(event: KeyEvent) -> String {
    let mut parts = Vec::new();
    if event.modifiers.contains(KeyModifiers::CTRL) {
        parts.push("ctrl".to_string());
    }
    if event.modifiers.contains(KeyModifiers::ALT) {
        parts.push("alt".to_string());
    }
    if event.modifiers.contains(KeyModifiers::SHIFT) {
        parts.push("shift".to_string());
    }
    if event.modifiers.contains(KeyModifiers::SUPER) {
        parts.push("super".to_string());
    }
    parts.push(match event.code {
        KeyCode::Backspace => "backspace".to_string(),
        KeyCode::Enter => "enter".to_string(),
        KeyCode::Left => "left".to_string(),
        KeyCode::Right => "right".to_string(),
        KeyCode::Up => "up".to_string(),
        KeyCode::Down => "down".to_string(),
        KeyCode::Home => "home".to_string(),
        KeyCode::End => "end".to_string(),
        KeyCode::PageUp => "pageup".to_string(),
        KeyCode::PageDown => "pagedown".to_string(),
        KeyCode::Tab => "tab".to_string(),
        KeyCode::Delete => "delete".to_string(),
        KeyCode::Insert => "insert".to_string(),
        KeyCode::F(n) => format!("f{n}"),
        KeyCode::Char(ch) => ch.to_string(),
        KeyCode::Esc => "esc".to_string(),
        KeyCode::CapsLock => "caps".to_string(),
        KeyCode::ScrollLock => "scroll".to_string(),
        KeyCode::NumLock => "numlock".to_string(),
        KeyCode::PrintScreen => "print".to_string(),
        KeyCode::Pause => "pause".to_string(),
        KeyCode::Menu => "menu".to_string(),
        KeyCode::KeypadBegin => "kpbegin".to_string(),
        KeyCode::Media(_) => "media".to_string(),
        KeyCode::Modifier(_, _) => "modifier".to_string(),
    });
    parts.join("+")
}

fn format_mouse_event(event: MouseEvent) -> Option<String> {
    Some(match event.kind {
        MouseEventKind::Down(button) => format!("{} click", mouse_button_label(button)),
        MouseEventKind::Up(button) => format!("{} release", mouse_button_label(button)),
        MouseEventKind::Drag(button) => format!("{} drag", mouse_button_label(button)),
        MouseEventKind::Moved => return None,
        MouseEventKind::Scroll(direction) => format!("wheel {}", scroll_direction_label(direction)),
    })
}

fn mouse_button_label(button: MouseButton) -> &'static str {
    match button {
        MouseButton::Left => "left",
        MouseButton::Right => "right",
        MouseButton::Middle => "middle",
        MouseButton::Unknown => "mouse",
    }
}

fn scroll_direction_label(direction: ScrollDirection) -> &'static str {
    match direction {
        ScrollDirection::Up => "up",
        ScrollDirection::Down => "down",
        ScrollDirection::Left => "left",
        ScrollDirection::Right => "right",
    }
}

struct StatusField {
    ansi: String,
    width: usize,
}

fn status_field(muted: PaletteColor, text: PaletteColor, label: &str, value: &str) -> StatusField {
    let plain = format!("{label}: {value}");
    StatusField {
        width: visible_width(&plain),
        ansi: format!(
            "\x1b[{}m{}:\x1b[{}m {}",
            palette::render_color_code(muted, false),
            label,
            palette::render_color_code(text, false),
            value,
        ),
    }
}

fn visible_width(text: &str) -> usize {
    UnicodeWidthStr::width(text)
}
