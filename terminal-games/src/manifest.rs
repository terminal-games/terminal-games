// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

pub use terminal_games_manifest::{GameDetails, GameManifest, MANIFEST_VERSION, Screenshot};
use unicode_width::UnicodeWidthChar;
use wasmparser::{Parser, Payload};

const MANIFEST_MARKER: &[u8] = br#""terminal_games_manifest_version""#;
const SCREENSHOT_COLS: usize = 80;
const SCREENSHOT_ROWS: usize = 24;

pub fn validate_manifest(manifest: &GameManifest) -> anyhow::Result<()> {
    terminal_games_manifest::GameManifest::validate(manifest).map_err(anyhow::Error::msg)
}

pub fn sanitize_manifest(manifest: &GameManifest) -> anyhow::Result<GameManifest> {
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
            let mut stream =
                serde_json::Deserializer::from_slice(&bytes[start..]).into_iter::<GameManifest>();
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
    terminal_games_manifest::validate_shortname(shortname).map_err(anyhow::Error::msg)
}

fn sanitize_wrapped_text(input: &str) -> String {
    trim_trailing_empty_lines(&sanitize_ansi_text(input))
}

fn sanitize_screenshot(input: &str) -> String {
    let mut vt = avt::Vt::new(SCREENSHOT_COLS, SCREENSHOT_ROWS);
    vt.feed_str(&normalize_screenshot_lines(&sanitize_ansi_text(input)));
    vt.dump()
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
