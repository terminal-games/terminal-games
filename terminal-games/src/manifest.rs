// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use avt::{Color, Pen};
use serde::{Deserialize, Serialize};
use unicode_width::UnicodeWidthChar;
use wasmparser::{Parser, Payload};

pub const MANIFEST_VERSION: u32 = 1;
const MANIFEST_MARKER: &[u8] = br#""terminal_games_manifest_version""#;
const MAX_SHORTNAME_LEN: usize = 32;
const MAX_AUTHOR_LEN: usize = 256;
const MAX_VERSION_LEN: usize = 128;
const MAX_LOCALIZED_VALUE_LEN: usize = 16 * 1024;
const MAX_SCREENSHOT_CONTENT_LEN: usize = 128 * 1024;
const MAX_SCREENSHOTS_PER_LOCALE: usize = 8;
const SCREENSHOT_COLS: usize = 80;
const SCREENSHOT_ROWS: usize = 24;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GameManifest {
    pub terminal_games_manifest_version: u32,
    pub shortname: String,
    #[serde(default)]
    pub details: GameDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GameDetails {
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub name: BTreeMap<String, String>,
    #[serde(default)]
    pub description: BTreeMap<String, String>,
    #[serde(default)]
    pub details: BTreeMap<String, String>,
    #[serde(default)]
    pub screenshots: BTreeMap<String, Vec<Screenshot>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Screenshot {
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub caption: String,
}

impl GameManifest {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.terminal_games_manifest_version == MANIFEST_VERSION,
            "terminal_games_manifest_version must be {}",
            MANIFEST_VERSION
        );
        validate_shortname(&self.shortname)?;
        validate_author(&self.details.author)?;
        validate_version(&self.details.version)?;
        validate_localized_map("details.name", &self.details.name)?;
        validate_localized_map("details.description", &self.details.description)?;
        validate_localized_map("details.details", &self.details.details)?;
        for (locale, screenshots) in &self.details.screenshots {
            validate_locale(locale)?;
            anyhow::ensure!(
                screenshots.len() <= MAX_SCREENSHOTS_PER_LOCALE,
                "details.screenshots.{locale} exceeds {MAX_SCREENSHOTS_PER_LOCALE} entries"
            );
            for (index, screenshot) in screenshots.iter().enumerate() {
                anyhow::ensure!(
                    screenshot.content.len() <= MAX_SCREENSHOT_CONTENT_LEN,
                    "details.screenshots.{locale}[{index}].content exceeds {} bytes",
                    MAX_SCREENSHOT_CONTENT_LEN
                );
                anyhow::ensure!(
                    screenshot.caption.len() <= MAX_LOCALIZED_VALUE_LEN,
                    "details.screenshots.{locale}[{index}].caption exceeds {} bytes",
                    MAX_LOCALIZED_VALUE_LEN
                );
            }
        }
        Ok(())
    }

    pub fn sanitized(&self) -> anyhow::Result<Self> {
        self.validate()?;
        let mut out = self.clone();
        out.details.author = sanitize_wrapped_text(&out.details.author);
        out.details.version = sanitize_wrapped_text(&out.details.version);
        for value in out.details.name.values_mut() {
            *value = sanitize_wrapped_text(value);
        }
        for value in out.details.description.values_mut() {
            *value = sanitize_wrapped_text(value);
        }
        for value in out.details.details.values_mut() {
            *value = sanitize_wrapped_text(value);
        }
        for screenshots in out.details.screenshots.values_mut() {
            for screenshot in screenshots {
                screenshot.content = sanitize_screenshot(&screenshot.content);
                screenshot.caption = sanitize_wrapped_text(&screenshot.caption);
            }
        }
        Ok(out)
    }
}

pub fn extract_manifest_from_wasm(bytes: &[u8]) -> anyhow::Result<Option<GameManifest>> {
    for payload in Parser::new(0).parse_all(bytes) {
        match payload? {
            Payload::CustomSection(section) => {
                if let Some(manifest) = extract_manifest_from_blob(section.data()) {
                    return Ok(Some(manifest));
                }
            }
            Payload::DataSection(reader) => {
                for data in reader {
                    let data = data?;
                    if let Some(manifest) = extract_manifest_from_blob(data.data) {
                        return Ok(Some(manifest));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(None)
}

fn extract_manifest_from_blob(bytes: &[u8]) -> Option<GameManifest> {
    let mut marker_search_from = 0usize;
    while let Some(marker_pos) = find_subslice(bytes, MANIFEST_MARKER, marker_search_from) {
        marker_search_from = marker_pos + MANIFEST_MARKER.len();
        let scan_start = marker_pos.saturating_sub(128 * 1024);
        for start in (scan_start..=marker_pos).rev() {
            if bytes[start] != b'{' {
                continue;
            }
            let mut stream = serde_json::Deserializer::from_slice(&bytes[start..])
                .into_iter::<GameManifest>();
            let Some(Ok(manifest)) = stream.next() else {
                continue;
            };
            if start + stream.byte_offset() <= marker_pos {
                continue;
            }
            if manifest.terminal_games_manifest_version != MANIFEST_VERSION {
                continue;
            }
            if manifest.shortname.trim().is_empty() {
                continue;
            }
            return Some(manifest);
        }
    }
    None
}

fn find_subslice(haystack: &[u8], needle: &[u8], start_at: usize) -> Option<usize> {
    if needle.is_empty() || start_at >= haystack.len() {
        return None;
    }
    haystack[start_at..]
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|offset| start_at + offset)
}

pub fn validate_shortname(shortname: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !shortname.is_empty() && shortname.len() <= MAX_SHORTNAME_LEN,
        "shortname must be between 1 and {MAX_SHORTNAME_LEN} characters"
    );
    anyhow::ensure!(
        shortname
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'),
        "shortname must contain only lowercase ASCII letters, digits, or '-'"
    );
    anyhow::ensure!(
        shortname
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()),
        "shortname must start with a lowercase ASCII letter or digit"
    );
    anyhow::ensure!(
        shortname
            .bytes()
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()),
        "shortname must end with a lowercase ASCII letter or digit"
    );
    Ok(())
}

fn validate_author(author: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !author.trim().is_empty() && author.len() <= MAX_AUTHOR_LEN,
        "details.author must be between 1 and {MAX_AUTHOR_LEN} bytes"
    );
    Ok(())
}

fn validate_version(version: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !version.trim().is_empty() && version.len() <= MAX_VERSION_LEN,
        "details.version must be between 1 and {MAX_VERSION_LEN} bytes"
    );
    Ok(())
}

fn validate_localized_map(path: &str, values: &BTreeMap<String, String>) -> anyhow::Result<()> {
    for (locale, value) in values {
        validate_locale(locale)?;
        anyhow::ensure!(
            value.len() <= MAX_LOCALIZED_VALUE_LEN,
            "{path}.{locale} exceeds {} bytes",
            MAX_LOCALIZED_VALUE_LEN
        );
    }
    Ok(())
}

fn validate_locale(locale: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!locale.trim().is_empty(), "locale keys cannot be empty");
    anyhow::ensure!(
        locale
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "locale keys must be ASCII alphanumeric plus '-' or '_'"
    );
    Ok(())
}

fn sanitize_wrapped_text(input: &str) -> String {
    trim_trailing_empty_lines(&sanitize_ansi_text(input))
}

fn sanitize_screenshot(input: &str) -> String {
    let mut vt = avt::Vt::new(SCREENSHOT_COLS, SCREENSHOT_ROWS);
    vt.feed_str(&normalize_screenshot_lines(&sanitize_ansi_text(input)));
    render_vt_to_ansi(&vt, SCREENSHOT_COLS, SCREENSHOT_ROWS)
}

fn sanitize_ansi_text(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\x1b' => {
                let (consumed, preserved) = parse_escape_sequence(&bytes[i..]);
                if preserved {
                    out.push_str(std::str::from_utf8(&bytes[i..i + consumed]).unwrap_or_default());
                }
                i += consumed.max(1);
            }
            b'\r' => {
                out.push('\n');
                i += 1;
                if bytes.get(i) == Some(&b'\n') {
                    i += 1;
                }
            }
            b'\n' => {
                out.push('\n');
                i += 1;
            }
            b'\t' => {
                out.push_str("    ");
                i += 1;
            }
            _ => {
                let tail = &input[i..];
                let ch = tail.chars().next().unwrap_or_default();
                if is_safe_text_char(ch) {
                    out.push(ch);
                }
                i += ch.len_utf8().max(1);
            }
        }
    }
    out
}

fn parse_escape_sequence(bytes: &[u8]) -> (usize, bool) {
    if bytes.len() < 2 {
        return (bytes.len(), false);
    }
    match bytes[1] {
        b'[' => parse_csi_sequence(bytes),
        b']' => parse_terminated_escape(bytes, b'\x07'),
        b'P' | b'X' | b'^' | b'_' => parse_st_terminated_escape(bytes),
        b'@'..=b'~' => (2, false),
        _ => (1, false),
    }
}

fn parse_csi_sequence(bytes: &[u8]) -> (usize, bool) {
    let mut end = 2usize;
    while end < bytes.len() {
        let byte = bytes[end];
        if (0x40..=0x7e).contains(&byte) {
            let params = &bytes[2..end];
            let keep = byte == b'm'
                && params
                    .iter()
                    .all(|b| b.is_ascii_digit() || matches!(*b, b';' | b':'));
            return (end + 1, keep);
        }
        end += 1;
    }
    (bytes.len(), false)
}

fn parse_terminated_escape(bytes: &[u8], terminator: u8) -> (usize, bool) {
    let mut end = 2usize;
    while end < bytes.len() {
        if bytes[end] == terminator {
            return (end + 1, false);
        }
        if bytes[end] == b'\x1b' && bytes.get(end + 1) == Some(&b'\\') {
            return (end + 2, false);
        }
        end += 1;
    }
    (bytes.len(), false)
}

fn parse_st_terminated_escape(bytes: &[u8]) -> (usize, bool) {
    let mut end = 2usize;
    while end + 1 < bytes.len() {
        if bytes[end] == b'\x1b' && bytes[end + 1] == b'\\' {
            return (end + 2, false);
        }
        end += 1;
    }
    (bytes.len(), false)
}

fn is_safe_text_char(ch: char) -> bool {
    !ch.is_control() && UnicodeWidthChar::width(ch).is_some()
}

fn trim_trailing_empty_lines(text: &str) -> String {
    let lines = text.lines().collect::<Vec<_>>();
    let mut end = lines.len();
    while end > 0 && lines[end - 1].trim().is_empty() {
        end -= 1;
    }
    lines[..end].join("\n")
}

fn normalize_screenshot_lines(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + text.matches('\n').count());
    for ch in text.chars() {
        if ch == '\n' {
            out.push('\r');
        }
        out.push(ch);
    }
    out
}

fn render_vt_to_ansi(vt: &avt::Vt, cols: usize, rows: usize) -> String {
    let lines = vt.view().collect::<Vec<_>>();
    let default_pen = Pen::default();
    let mut current_pen = default_pen;
    let mut out = String::new();
    for row in 0..rows {
        let line = lines.get(row).copied();
        let mut col = 0usize;
        while col < cols {
            let Some(line) = line else {
                apply_pen(&mut out, &mut current_pen, default_pen);
                out.push(' ');
                col += 1;
                continue;
            };
            if col >= line.len() {
                apply_pen(&mut out, &mut current_pen, default_pen);
                out.push(' ');
                col += 1;
                continue;
            }
            let cell = &line[col];
            if cell.width() == 0 {
                col += 1;
                continue;
            }
            if cell.width() == 2 && col + 1 >= cols {
                apply_pen(&mut out, &mut current_pen, *cell.pen());
                out.push(' ');
                col += 1;
                continue;
            }
            apply_pen(&mut out, &mut current_pen, *cell.pen());
            out.push(cell.char());
            col += cell.width().max(1);
        }
        apply_pen(&mut out, &mut current_pen, default_pen);
        if row + 1 < rows {
            out.push('\n');
        }
    }
    out.push_str("\x1b[0m");
    out
}

fn apply_pen(out: &mut String, current_pen: &mut Pen, next_pen: Pen) {
    if *current_pen == next_pen {
        return;
    }
    out.push_str("\x1b[0");
    if let Some(color) = next_pen.foreground() {
        append_color_param(out, color, 30);
    }
    if let Some(color) = next_pen.background() {
        append_color_param(out, color, 40);
    }
    if next_pen.is_bold() {
        out.push_str(";1");
    } else if next_pen.is_faint() {
        out.push_str(";2");
    }
    if next_pen.is_italic() {
        out.push_str(";3");
    }
    if next_pen.is_underline() {
        out.push_str(";4");
    }
    if next_pen.is_blink() {
        out.push_str(";5");
    }
    if next_pen.is_inverse() {
        out.push_str(";7");
    }
    if next_pen.is_strikethrough() {
        out.push_str(";9");
    }
    out.push('m');
    *current_pen = next_pen;
}

fn append_color_param(out: &mut String, color: Color, base: u8) {
    match color {
        Color::Indexed(index) if index < 8 => {
            out.push(';');
            out.push_str(&(base + index).to_string());
        }
        Color::Indexed(index) if index < 16 => {
            out.push(';');
            out.push_str(&(base + 52 + index).to_string());
        }
        Color::Indexed(index) => {
            out.push_str(&format!(";{};5;{}", base + 8, index));
        }
        Color::RGB(rgb) => {
            out.push_str(&format!(";{};2;{};{};{}", base + 8, rgb.r, rgb.g, rgb.b));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{sanitize_screenshot, sanitize_wrapped_text};

    #[test]
    fn wrapped_text_keeps_sgr_and_drops_control_sequences() {
        let text = "hello\x1b[31m red\x1b[0m\x1b]8;;https://example.com\x1b\\bad\x1b[2J\nthere";
        let sanitized = sanitize_wrapped_text(text);
        assert!(sanitized.contains("\x1b[31m red\x1b[0m"));
        assert!(sanitized.contains("bad"));
        assert!(!sanitized.contains("\x1b]8"));
        assert!(!sanitized.contains("\x1b[2J"));
    }

    #[test]
    fn screenshots_render_to_exact_canvas_size() {
        let screenshot = sanitize_screenshot("\x1b[31mhi\x1b[0m");
        let stripped = strip_ansi_escapes::strip_str(&screenshot);
        let lines = stripped.split('\n').collect::<Vec<_>>();
        assert_eq!(lines.len(), 24);
        assert!(lines.iter().all(|line| line.chars().count() == 80));
    }

    #[test]
    fn screenshots_reset_column_on_newline() {
        let screenshot = sanitize_screenshot("abc\nxyz");
        let stripped = strip_ansi_escapes::strip_str(&screenshot);
        let lines = stripped.split('\n').collect::<Vec<_>>();
        assert!(lines[0].starts_with("abc"));
        assert!(lines[1].starts_with("xyz"));
    }
}
