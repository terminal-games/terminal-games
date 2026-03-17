# `headless-terminal`

This is a hard fork of [`vt100`](https://crates.io/crates/vt100) but where there is no backing grid of cells.
It still parses terminal output and moves the cursor.
Primarily it does damage tracking on a per row basis.

It also tracks the terminal's default background color when an OSC 11 reply is
seen, so callers can inspect `parser.screen().terminal_background()` after
processing `ESC ] 11 ; ... BEL` / `ESC \` responses.
