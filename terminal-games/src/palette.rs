// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use crate::terminal_profile::{TerminalColorMode, TerminalProfile};

#[derive(Clone, Copy)]
pub enum Color {
    Basic(u8),
    Fixed(u8),
    Rgb(u8, u8, u8),
}

/// Tailwind CSS-based color palette, adaptive to terminal color mode and
/// light/dark background.
///
/// TrueColor values use Tailwind neutral scale + user-specified primary green.
/// Fixed (256) and Basic (16) values are the closest matches.
#[derive(Clone, Copy)]
pub struct Palette {
    pub primary: Color,
    pub accent: Color,
    pub danger: Color,
    pub on_primary: Color,
    pub text: Color,
    pub text_muted: Color,
    pub text_subtle: Color,
    pub surface: Color,
    pub surface_bright: Color,
    pub line: Color,
}

pub fn render_color_code(color: Color, is_bg: bool) -> String {
    match color {
        Color::Basic(n) => {
            let base = if is_bg { 40 } else { 30 };
            if n < 8 {
                (base + n as i32).to_string()
            } else {
                let bright = if is_bg { 100 } else { 90 };
                (bright + (n as i32 - 8)).to_string()
            }
        }
        Color::Fixed(n) => {
            if is_bg {
                format!("48;5;{n}")
            } else {
                format!("38;5;{n}")
            }
        }
        Color::Rgb(r, g, b) => {
            if is_bg {
                format!("48;2;{r};{g};{b}")
            } else {
                format!("38;2;{r};{g};{b}")
            }
        }
    }
}

fn c(dark: bool, d: Color, l: Color) -> Color {
    if dark { d } else { l }
}

pub fn palette(profile: TerminalProfile) -> Palette {
    let dark = profile.has_dark_background();
    match profile.color_mode {
        //                dark                               light
        // ─────────────────────────────────────────────────────────────────────────
        // primary        #98c379                          #98c379
        // accent         blue-400  #60a5fa                 blue-600   #2563eb
        // danger         red-400   #f87171                 red-600    #dc2626
        // on_primary     neutral-900 #171717               neutral-900 #171717
        // text           neutral-100 #f5f5f5               neutral-800 #262626
        // text_muted     neutral-300 #d4d4d4               neutral-600 #525252
        // text_subtle    neutral-400 #a3a3a3               neutral-500 #737373
        // surface        neutral-800 #262626               neutral-100 #f5f5f5
        // surface_bright neutral-700 #404040               neutral-200 #e5e5e5
        // line           neutral-600 #525252               neutral-300 #d4d4d4
        TerminalColorMode::TrueColor => Palette {
            primary: Color::Rgb(0x98, 0xc3, 0x79),
            accent: c(
                dark,
                Color::Rgb(0x60, 0xa5, 0xfa),
                Color::Rgb(0x25, 0x63, 0xeb),
            ),
            danger: c(
                dark,
                Color::Rgb(0xf8, 0x71, 0x71),
                Color::Rgb(0xdc, 0x26, 0x26),
            ),
            on_primary: Color::Rgb(0x17, 0x17, 0x17),
            text: c(
                dark,
                Color::Rgb(0xf5, 0xf5, 0xf5),
                Color::Rgb(0x26, 0x26, 0x26),
            ),
            text_muted: c(
                dark,
                Color::Rgb(0xd4, 0xd4, 0xd4),
                Color::Rgb(0x52, 0x52, 0x52),
            ),
            text_subtle: c(
                dark,
                Color::Rgb(0xa3, 0xa3, 0xa3),
                Color::Rgb(0x73, 0x73, 0x73),
            ),
            surface: c(
                dark,
                Color::Rgb(0x26, 0x26, 0x26),
                Color::Rgb(0xf5, 0xf5, 0xf5),
            ),
            surface_bright: c(
                dark,
                Color::Rgb(0x40, 0x40, 0x40),
                Color::Rgb(0xe5, 0xe5, 0xe5),
            ),
            line: c(
                dark,
                Color::Rgb(0x52, 0x52, 0x52),
                Color::Rgb(0xd4, 0xd4, 0xd4),
            ),
        },
        TerminalColorMode::Color256 => Palette {
            primary: Color::Fixed(114),
            accent: c(dark, Color::Fixed(75), Color::Fixed(27)),
            danger: c(dark, Color::Fixed(203), Color::Fixed(160)),
            on_primary: Color::Fixed(234),
            text: c(dark, Color::Fixed(255), Color::Fixed(235)),
            text_muted: c(dark, Color::Fixed(252), Color::Fixed(240)),
            text_subtle: c(dark, Color::Fixed(248), Color::Fixed(243)),
            surface: c(dark, Color::Fixed(235), Color::Fixed(255)),
            surface_bright: c(dark, Color::Fixed(237), Color::Fixed(254)),
            line: c(dark, Color::Fixed(240), Color::Fixed(252)),
        },
        TerminalColorMode::Color16 => Palette {
            primary: Color::Basic(10),
            accent: c(dark, Color::Basic(12), Color::Basic(4)),
            danger: c(dark, Color::Basic(9), Color::Basic(1)),
            on_primary: Color::Basic(0),
            text: c(dark, Color::Basic(15), Color::Basic(0)),
            text_muted: c(dark, Color::Basic(7), Color::Basic(8)),
            text_subtle: c(dark, Color::Basic(7), Color::Basic(8)),
            surface: c(dark, Color::Basic(8), Color::Basic(15)),
            surface_bright: c(dark, Color::Basic(8), Color::Basic(7)),
            line: c(dark, Color::Basic(8), Color::Basic(7)),
        },
    }
}
