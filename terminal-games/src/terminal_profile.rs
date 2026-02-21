// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TerminalColorMode {
    Color16 = 0,
    Color256 = 1,
    TrueColor = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalProfile {
    pub color_mode: TerminalColorMode,
    pub background_rgb: Option<(u8, u8, u8)>,
    pub dark_background: Option<bool>,
}

impl Default for TerminalProfile {
    fn default() -> Self {
        Self {
            color_mode: TerminalColorMode::Color256,
            background_rgb: None,
            dark_background: Some(true),
        }
    }
}

impl TerminalProfile {
    pub fn detect_color_mode(term: Option<&str>, colorterm: Option<&str>) -> TerminalColorMode {
        let term_raw = term.unwrap_or_default();
        let term = term_raw.to_ascii_lowercase();
        let colorterm = colorterm.unwrap_or_default().to_ascii_lowercase();

        match colorterm.as_str() {
            "24bit" | "truecolor" => {
                if term.starts_with("screen") {
                    return TerminalColorMode::Color256;
                }
                return TerminalColorMode::TrueColor;
            }
            "yes" | "true" => {
                return TerminalColorMode::Color256;
            }
            _ => {}
        }

        match term.as_str() {
            "alacritty" | "contour" | "rio" | "wezterm" | "xterm-ghostty" | "xterm-kitty" => {
                return TerminalColorMode::TrueColor;
            }
            "linux" | "xterm" => {
                return TerminalColorMode::Color16;
            }
            _ => {}
        }

        if term.contains("256color") {
            return TerminalColorMode::Color256;
        }
        if term.contains("color") || term.contains("ansi") {
            return TerminalColorMode::Color16;
        }

        TerminalColorMode::Color16
    }

    pub fn from_term(term: Option<&str>, colorterm: Option<&str>) -> Self {
        Self {
            color_mode: Self::detect_color_mode(term, colorterm),
            background_rgb: None,
            dark_background: None,
        }
    }

    pub fn with_background_rgb(mut self, rgb: (u8, u8, u8)) -> Self {
        self.background_rgb = Some(rgb);
        self.dark_background = Some(Self::is_rgb_dark(rgb));
        self
    }

    pub fn set_dark_background(&mut self, is_dark: bool) {
        self.dark_background = Some(is_dark);
    }

    pub fn has_dark_background(self) -> bool {
        if let Some(is_dark) = self.dark_background {
            return is_dark;
        }
        if let Some(rgb) = self.background_rgb {
            return Self::is_rgb_dark(rgb);
        }
        true
    }

    pub fn web_default() -> Self {
        Self {
            color_mode: TerminalColorMode::TrueColor,
            background_rgb: Some((0, 0, 0)),
            dark_background: Some(true),
        }
    }

    fn is_rgb_dark((r, g, b): (u8, u8, u8)) -> bool {
        let r = (r as f64) / 255.0;
        let g = (g as f64) / 255.0;
        let b = (b as f64) / 255.0;
        let luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b;
        luminance < 0.5
    }
}
