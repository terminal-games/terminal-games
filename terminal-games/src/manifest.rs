// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use unicode_width::UnicodeWidthChar;
use wasmparser::{Parser, Payload};

const MANIFEST_MARKER: &[u8] = br#""terminal_games_manifest_version""#;
pub const MANIFEST_VERSION: u32 = 1;
const SCREENSHOT_COLS: usize = 80;
const SCREENSHOT_ROWS: usize = 24;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppManifest {
    pub terminal_games_manifest_version: u32,
    pub shortname: String,
    #[serde(default)]
    pub details: AppDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppDetails {
    pub author: String,
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

pub fn validate_manifest(manifest: &AppManifest) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(manifest)?;
    terminal_games_manifest::validate_manifest_json(&bytes).map_err(anyhow::Error::msg)
}

pub fn sanitize_manifest(manifest: &AppManifest) -> anyhow::Result<AppManifest> {
    validate_manifest(manifest)?;
    let mut out = manifest.clone();
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

pub fn extract_manifest_from_wasm(bytes: &[u8]) -> anyhow::Result<Option<AppManifest>> {
    for payload in Parser::new(0).parse_all(bytes) {
        match payload? {
            Payload::CustomSection(section) => {
                if let Some(manifest) = extract_manifest_from_blob(section.data())? {
                    return Ok(Some(manifest));
                }
            }
            Payload::DataSection(reader) => {
                for data in reader {
                    let data = data?;
                    if let Some(manifest) = extract_manifest_from_blob(data.data)? {
                        return Ok(Some(manifest));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(None)
}

fn extract_manifest_from_blob(bytes: &[u8]) -> anyhow::Result<Option<AppManifest>> {
    let mut marker_search_from = 0usize;
    while let Some(marker_pos) = find_subslice(bytes, MANIFEST_MARKER, marker_search_from) {
        marker_search_from = marker_pos + MANIFEST_MARKER.len();
        let scan_start = marker_pos.saturating_sub(128 * 1024);
        let mut found_json_start = false;
        for start in (scan_start..=marker_pos).rev() {
            if bytes[start] != b'{' {
                continue;
            }
            found_json_start = true;
            let Some(end) = find_json_object_end(bytes, start) else {
                continue;
            };
            if end <= marker_pos {
                continue;
            }
            let candidate = &bytes[start..end];
            if !candidate
                .windows(MANIFEST_MARKER.len())
                .any(|window| window == MANIFEST_MARKER)
            {
                continue;
            }
            terminal_games_manifest::validate_manifest_json(candidate).map_err(|error| {
                anyhow::anyhow!("invalid embedded terminal-games manifest: {error}")
            })?;
            let manifest = serde_json::from_slice::<AppManifest>(candidate)?;
            return Ok(Some(manifest));
        }
        if found_json_start {
            return Err(anyhow::anyhow!(
                "found embedded terminal-games manifest marker but failed to recover a complete JSON object"
            ));
        }
    }
    Ok(None)
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

fn find_json_object_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (offset, byte) in bytes[start..].iter().copied().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match byte {
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(start + offset + 1);
                }
            }
            _ => {}
        }
    }
    None
}

pub fn validate_shortname(shortname: &str) -> anyhow::Result<()> {
    if shortname.is_empty() || shortname.len() > 32 {
        anyhow::bail!("shortname must be between 1 and 32 characters");
    }
    if !shortname
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        anyhow::bail!("shortname must contain only lowercase ASCII letters, digits, or '-'");
    }
    if !shortname
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        anyhow::bail!("shortname must start with a lowercase ASCII letter or digit");
    }
    if !shortname
        .bytes()
        .last()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        anyhow::bail!("shortname must end with a lowercase ASCII letter or digit");
    }
    Ok(())
}

fn sanitize_wrapped_text(input: &str) -> String {
    trim_trailing_empty_lines(&sanitize_ansi_text(input, true))
}

fn sanitize_screenshot(input: &str) -> String {
    let text = sanitize_ansi_text(input, true);
    let mut rows = text
        .split('\n')
        .take(SCREENSHOT_ROWS)
        .map(|line| crop_ansi_line(line, SCREENSHOT_COLS))
        .collect::<Vec<_>>();
    while rows.len() < SCREENSHOT_ROWS {
        rows.push(" ".repeat(SCREENSHOT_COLS));
    }
    rows.join("\n")
}

fn sanitize_ansi_text(input: &str, allow_osc8: bool) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\x1b' => {
                let (consumed, preserved) = parse_escape_sequence(&bytes[i..], allow_osc8);
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

fn parse_escape_sequence(bytes: &[u8], allow_osc8: bool) -> (usize, bool) {
    if bytes.len() < 2 {
        return (bytes.len(), false);
    }
    match bytes[1] {
        b'[' => parse_csi_sequence(bytes),
        b']' => parse_osc_sequence(bytes, allow_osc8),
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

fn parse_osc_sequence(bytes: &[u8], allow_osc8: bool) -> (usize, bool) {
    let mut end = 2usize;
    while end < bytes.len() {
        if bytes[end] == b'\x07' {
            return (end + 1, allow_osc8 && is_preserved_osc8(&bytes[..end + 1]));
        }
        if bytes[end] == b'\x1b' && bytes.get(end + 1) == Some(&b'\\') {
            return (end + 2, allow_osc8 && is_preserved_osc8(&bytes[..end + 2]));
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

fn is_preserved_osc8(bytes: &[u8]) -> bool {
    bytes.starts_with(b"\x1b]8;;")
}

fn trim_trailing_empty_lines(text: &str) -> String {
    let lines = text.lines().collect::<Vec<_>>();
    let mut end = lines.len();
    while end > 0 && lines[end - 1].trim().is_empty() {
        end -= 1;
    }
    lines[..end].join("\n")
}

fn crop_ansi_line(line: &str, max_width: usize) -> String {
    let bytes = line.as_bytes();
    let mut out = String::with_capacity(line.len() + max_width);
    let mut i = 0usize;
    let mut width = 0usize;
    while i < bytes.len() && width < max_width {
        match bytes[i] {
            b'\x1b' => {
                let (consumed, preserved) = parse_escape_sequence(&bytes[i..], true);
                if preserved {
                    out.push_str(std::str::from_utf8(&bytes[i..i + consumed]).unwrap_or_default());
                }
                i += consumed.max(1);
            }
            _ => {
                let tail = &line[i..];
                let ch = tail.chars().next().unwrap_or_default();
                let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
                if width + ch_width > max_width {
                    break;
                }
                out.push(ch);
                width += ch_width;
                i += ch.len_utf8().max(1);
            }
        }
    }
    if width < max_width {
        out.push_str(&" ".repeat(max_width - width));
    }
    out.push_str("\x1b]8;;\x1b\\");
    out.push_str("\x1b[0m");
    out
}

#[cfg(test)]
mod tests {
    use super::{sanitize_screenshot, sanitize_wrapped_text};

    #[test]
    fn wrapped_text_keeps_sgr_and_drops_control_sequences() {
        let text = "hello\x1b[31m red\x1b[0m\x1b]8;;https://example.com\x1b\\bad\x1b[2J\nthere";
        let sanitized = sanitize_wrapped_text(text);
        assert!(sanitized.contains("\x1b[31m red\x1b[0m"));
        assert!(sanitized.contains("\x1b]8;;https://example.com\x1b\\"));
        assert!(sanitized.contains("bad"));
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

    #[test]
    fn screenshots_are_confined_to_24_rows() {
        let screenshot = sanitize_screenshot(&"x\n".repeat(100));
        let stripped = strip_ansi_escapes::strip_str(&screenshot);
        assert_eq!(stripped.split('\n').count(), 24);
    }

    #[test]
    fn screenshots_close_osc8_at_line_end() {
        let screenshot = sanitize_screenshot("\x1b]8;;https://example.com\x1b\\hello");
        assert!(screenshot.contains("\x1b]8;;\x1b\\\x1b[0m"));
    }
}
