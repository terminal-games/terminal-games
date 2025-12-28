use crate::term::BufWrite as _;
use unicode_width::UnicodeWidthChar as _;

const MODE_APPLICATION_KEYPAD: u8 = 0b0000_0001;
const MODE_APPLICATION_CURSOR: u8 = 0b0000_0010;
const MODE_HIDE_CURSOR: u8 = 0b0000_0100;
const MODE_ALTERNATE_SCREEN: u8 = 0b0000_1000;
const MODE_BRACKETED_PASTE: u8 = 0b0001_0000;

/// The xterm mouse handling mode currently in use.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub enum MouseProtocolMode {
    /// Mouse handling is disabled.
    #[default]
    None,

    /// Mouse button events should be reported on button press. Also known as
    /// X10 mouse mode.
    Press,

    /// Mouse button events should be reported on button press and release.
    /// Also known as VT200 mouse mode.
    PressRelease,

    // Highlight,
    /// Mouse button events should be reported on button press and release, as
    /// well as when the mouse moves between cells while a button is held
    /// down.
    ButtonMotion,

    /// Mouse button events should be reported on button press and release,
    /// and mouse motion events should be reported when the mouse moves
    /// between cells regardless of whether a button is held down or not.
    AnyMotion,
    // DecLocator,
}

/// The encoding to use for the enabled [`MouseProtocolMode`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub enum MouseProtocolEncoding {
    /// Default single-printable-byte encoding.
    #[default]
    Default,

    /// UTF-8-based encoding.
    Utf8,

    /// SGR-like encoding.
    Sgr,
    // Urxvt,
}

/// Represents the overall terminal state.
#[derive(Clone, Debug)]
pub struct Screen {
    grid: crate::grid::Grid,
    alternate_grid: crate::grid::Grid,

    attrs: crate::attrs::Attrs,
    saved_attrs: crate::attrs::Attrs,

    modes: u8,
    mouse_protocol_mode: MouseProtocolMode,
    mouse_protocol_encoding: MouseProtocolEncoding,
}

impl Screen {
    pub(crate) fn new(size: crate::grid::Size, _scrollback_len: usize) -> Self {
        let grid = crate::grid::Grid::new(size, 0);
        Self {
            grid,
            alternate_grid: crate::grid::Grid::new(size, 0),

            attrs: crate::attrs::Attrs::default(),
            saved_attrs: crate::attrs::Attrs::default(),

            modes: 0,
            mouse_protocol_mode: MouseProtocolMode::default(),
            mouse_protocol_encoding: MouseProtocolEncoding::default(),
        }
    }

    /// Resizes the terminal.
    pub fn set_size(&mut self, rows: u16, cols: u16) {
        self.grid.set_size(crate::grid::Size { rows, cols });
        self.alternate_grid
            .set_size(crate::grid::Size { rows, cols });
    }

    /// Returns the current size of the terminal.
    ///
    /// The return value will be (rows, cols).
    #[must_use]
    pub fn size(&self) -> (u16, u16) {
        let size = self.grid().size();
        (size.rows, size.cols)
    }

    /// Returns terminal escape sequences sufficient to set the current
    /// terminal's input modes.
    ///
    /// Supported modes are:
    /// * application keypad
    /// * application cursor
    /// * bracketed paste
    /// * xterm mouse support
    #[must_use]
    pub fn input_mode_formatted(&self) -> Vec<u8> {
        let mut contents = vec![];
        self.write_input_mode_formatted(&mut contents);
        contents
    }

    fn write_input_mode_formatted(&self, contents: &mut Vec<u8>) {
        crate::term::ApplicationKeypad::new(self.mode(MODE_APPLICATION_KEYPAD)).write_buf(contents);
        crate::term::ApplicationCursor::new(self.mode(MODE_APPLICATION_CURSOR)).write_buf(contents);
        crate::term::BracketedPaste::new(self.mode(MODE_BRACKETED_PASTE)).write_buf(contents);
        crate::term::MouseProtocolMode::new(self.mouse_protocol_mode, MouseProtocolMode::None)
            .write_buf(contents);
        crate::term::MouseProtocolEncoding::new(
            self.mouse_protocol_encoding,
            MouseProtocolEncoding::Default,
        )
        .write_buf(contents);
    }

    /// Returns terminal escape sequences sufficient to change the previous
    /// terminal's input modes to the input modes enabled in the current
    /// terminal.
    #[must_use]
    pub fn input_mode_diff(&self, prev: &Self) -> Vec<u8> {
        let mut contents = vec![];
        self.write_input_mode_diff(&mut contents, prev);
        contents
    }

    fn write_input_mode_diff(&self, contents: &mut Vec<u8>, prev: &Self) {
        if self.mode(MODE_APPLICATION_KEYPAD) != prev.mode(MODE_APPLICATION_KEYPAD) {
            crate::term::ApplicationKeypad::new(self.mode(MODE_APPLICATION_KEYPAD))
                .write_buf(contents);
        }
        if self.mode(MODE_APPLICATION_CURSOR) != prev.mode(MODE_APPLICATION_CURSOR) {
            crate::term::ApplicationCursor::new(self.mode(MODE_APPLICATION_CURSOR))
                .write_buf(contents);
        }
        if self.mode(MODE_BRACKETED_PASTE) != prev.mode(MODE_BRACKETED_PASTE) {
            crate::term::BracketedPaste::new(self.mode(MODE_BRACKETED_PASTE)).write_buf(contents);
        }
        crate::term::MouseProtocolMode::new(self.mouse_protocol_mode, prev.mouse_protocol_mode)
            .write_buf(contents);
        crate::term::MouseProtocolEncoding::new(
            self.mouse_protocol_encoding,
            prev.mouse_protocol_encoding,
        )
        .write_buf(contents);
    }

    /// Returns terminal escape sequences sufficient to set the current
    /// terminal's drawing attributes.
    ///
    /// Supported drawing attributes are:
    /// * fgcolor
    /// * bgcolor
    /// * bold
    /// * dim
    /// * italic
    /// * underline
    /// * inverse
    ///
    /// This is not typically necessary, since
    /// [`contents_formatted`](Self::contents_formatted) will leave
    /// the current active drawing attributes in the correct state, but this
    /// can be useful in the case of drawing additional things on top of a
    /// terminal output, since you will need to restore the terminal state
    /// without the terminal contents necessarily being the same.
    #[must_use]
    pub fn attributes_formatted(&self) -> Vec<u8> {
        let mut contents = vec![];
        self.write_attributes_formatted(&mut contents);
        contents
    }

    fn write_attributes_formatted(&self, contents: &mut Vec<u8>) {
        crate::term::ClearAttrs.write_buf(contents);
        self.attrs
            .write_escape_code_diff(contents, &crate::attrs::Attrs::default());
    }

    /// Returns the current cursor position of the terminal.
    ///
    /// The return value will be (row, col).
    #[must_use]
    pub fn cursor_position(&self) -> (u16, u16) {
        let pos = self.grid().pos();
        (pos.row, pos.col)
    }

    /// Returns the set of row indices that have been written to since the last
    /// call to `take_damaged_rows()`.
    ///
    /// This does not clear the damage tracking. Use `take_damaged_rows()` to
    /// get and clear the damaged rows in one operation.
    #[must_use]
    pub fn damaged_rows(&self) -> std::collections::HashSet<u16> {
        self.grid().damaged_rows().clone()
    }

    /// Returns the set of row indices that have been written to and clears the
    /// damage tracking.
    ///
    /// This is useful for incremental updates where you want to know which
    /// rows need to be redrawn.
    #[must_use]
    pub fn take_damaged_rows(&mut self) -> std::collections::HashSet<u16> {
        self.grid_mut().take_damaged_rows()
    }

    /// Returns terminal escape sequences sufficient to set the current
    /// cursor state of the terminal.
    ///
    /// This is not typically necessary, since
    /// [`contents_formatted`](Self::contents_formatted) will leave
    /// the cursor in the correct state, but this can be useful in the case of
    /// drawing additional things on top of a terminal output, since you will
    /// need to restore the terminal state without the terminal contents
    /// necessarily being the same.
    ///
    /// Note that the bytes returned by this function may alter the active
    /// drawing attributes, because it may require redrawing existing cells in
    /// order to position the cursor correctly (for instance, in the case
    /// where the cursor is past the end of a row). Therefore, you should
    /// ensure to reset the active drawing attributes if necessary after
    /// processing this data, for instance by using
    /// [`attributes_formatted`](Self::attributes_formatted).
    #[must_use]
    pub fn cursor_state_formatted(&self) -> Vec<u8> {
        let mut contents = vec![];
        self.write_cursor_state_formatted(&mut contents);
        contents
    }

    fn write_cursor_state_formatted(&self, contents: &mut Vec<u8>) {
        crate::term::HideCursor::new(self.hide_cursor()).write_buf(contents);
        self.grid()
            .write_cursor_position_formatted(contents, None, None);

        // we don't just call write_attributes_formatted here, because that
        // would still be confusing - consider the case where the user sets
        // their own unrelated drawing attributes (on a different parser
        // instance) and then calls cursor_state_formatted. just documenting
        // it and letting the user handle it on their own is more
        // straightforward.
    }

    /// Returns whether the alternate screen is currently in use.
    #[must_use]
    pub fn alternate_screen(&self) -> bool {
        self.mode(MODE_ALTERNATE_SCREEN)
    }

    /// Returns whether the terminal should be in application keypad mode.
    #[must_use]
    pub fn application_keypad(&self) -> bool {
        self.mode(MODE_APPLICATION_KEYPAD)
    }

    /// Returns whether the terminal should be in application cursor mode.
    #[must_use]
    pub fn application_cursor(&self) -> bool {
        self.mode(MODE_APPLICATION_CURSOR)
    }

    /// Returns whether the terminal should be in hide cursor mode.
    #[must_use]
    pub fn hide_cursor(&self) -> bool {
        self.mode(MODE_HIDE_CURSOR)
    }

    /// Returns whether the terminal should be in bracketed paste mode.
    #[must_use]
    pub fn bracketed_paste(&self) -> bool {
        self.mode(MODE_BRACKETED_PASTE)
    }

    /// Returns the currently active [`MouseProtocolMode`].
    #[must_use]
    pub fn mouse_protocol_mode(&self) -> MouseProtocolMode {
        self.mouse_protocol_mode
    }

    /// Returns the currently active [`MouseProtocolEncoding`].
    #[must_use]
    pub fn mouse_protocol_encoding(&self) -> MouseProtocolEncoding {
        self.mouse_protocol_encoding
    }

    /// Returns the currently active foreground color.
    #[must_use]
    pub fn fgcolor(&self) -> crate::Color {
        self.attrs.fgcolor
    }

    /// Returns the currently active background color.
    #[must_use]
    pub fn bgcolor(&self) -> crate::Color {
        self.attrs.bgcolor
    }

    /// Returns whether newly drawn text should be rendered with the bold text
    /// attribute.
    #[must_use]
    pub fn bold(&self) -> bool {
        self.attrs.bold()
    }

    /// Returns whether newly drawn text should be rendered with the dim text
    /// attribute.
    #[must_use]
    pub fn dim(&self) -> bool {
        self.attrs.dim()
    }

    /// Returns whether newly drawn text should be rendered with the italic
    /// text attribute.
    #[must_use]
    pub fn italic(&self) -> bool {
        self.attrs.italic()
    }

    /// Returns whether newly drawn text should be rendered with the
    /// underlined text attribute.
    #[must_use]
    pub fn underline(&self) -> bool {
        self.attrs.underline()
    }

    /// Returns whether newly drawn text should be rendered with the inverse
    /// text attribute.
    #[must_use]
    pub fn inverse(&self) -> bool {
        self.attrs.inverse()
    }

    pub(crate) fn grid(&self) -> &crate::grid::Grid {
        if self.mode(MODE_ALTERNATE_SCREEN) {
            &self.alternate_grid
        } else {
            &self.grid
        }
    }

    fn grid_mut(&mut self) -> &mut crate::grid::Grid {
        if self.mode(MODE_ALTERNATE_SCREEN) {
            &mut self.alternate_grid
        } else {
            &mut self.grid
        }
    }

    fn enter_alternate_grid(&mut self) {
        self.set_mode(MODE_ALTERNATE_SCREEN);
    }

    fn exit_alternate_grid(&mut self) {
        self.clear_mode(MODE_ALTERNATE_SCREEN);
    }

    fn save_cursor(&mut self) {
        self.grid_mut().save_cursor();
        self.saved_attrs = self.attrs;
    }

    fn restore_cursor(&mut self) {
        self.grid_mut().restore_cursor();
        self.attrs = self.saved_attrs;
    }

    fn set_mode(&mut self, mode: u8) {
        self.modes |= mode;
    }

    fn clear_mode(&mut self, mode: u8) {
        self.modes &= !mode;
    }

    fn mode(&self, mode: u8) -> bool {
        self.modes & mode != 0
    }

    fn set_mouse_mode(&mut self, mode: MouseProtocolMode) {
        self.mouse_protocol_mode = mode;
    }

    fn clear_mouse_mode(&mut self, mode: MouseProtocolMode) {
        if self.mouse_protocol_mode == mode {
            self.mouse_protocol_mode = MouseProtocolMode::default();
        }
    }

    fn set_mouse_encoding(&mut self, encoding: MouseProtocolEncoding) {
        self.mouse_protocol_encoding = encoding;
    }

    fn clear_mouse_encoding(&mut self, encoding: MouseProtocolEncoding) {
        if self.mouse_protocol_encoding == encoding {
            self.mouse_protocol_encoding = MouseProtocolEncoding::default();
        }
    }
}

impl Screen {
    pub(crate) fn text(&mut self, c: char) {
        // Update cursor position based on character width, but don't store the character
        let pos = self.grid().pos();
        let size = self.grid().size();

        let width = c.width();
        if width.is_none() && (u32::from(c)) < 256 {
            // don't even try to draw control characters
            return;
        }
        let width = width
            .unwrap_or(1)
            .try_into()
            // width() can only return 0, 1, or 2
            .unwrap();

        // Mark current row as damaged before any operations
        self.grid_mut().mark_row_damaged(pos.row);

        // Handle wrapping
        if pos.col > size.cols - width {
            self.grid_mut().col_wrap(width, false);
        }

        // Advance cursor position
        self.grid_mut().col_inc(1);
        if width > 1 {
            self.grid_mut().col_inc(1);
        }
    }

    // control codes

    pub(crate) fn bs(&mut self) {
        self.grid_mut().col_dec(1);
    }

    pub(crate) fn tab(&mut self) {
        self.grid_mut().col_tab();
    }

    pub(crate) fn lf(&mut self) {
        self.grid_mut().row_inc_scroll(1);
    }

    pub(crate) fn vt(&mut self) {
        self.lf();
    }

    pub(crate) fn ff(&mut self) {
        self.lf();
    }

    pub(crate) fn cr(&mut self) {
        self.grid_mut().col_set(0);
    }

    // escape codes

    // ESC 7
    pub(crate) fn decsc(&mut self) {
        self.save_cursor();
    }

    // ESC 8
    pub(crate) fn decrc(&mut self) {
        self.restore_cursor();
    }

    // ESC =
    pub(crate) fn deckpam(&mut self) {
        self.set_mode(MODE_APPLICATION_KEYPAD);
    }

    // ESC >
    pub(crate) fn deckpnm(&mut self) {
        self.clear_mode(MODE_APPLICATION_KEYPAD);
    }

    // ESC M
    pub(crate) fn ri(&mut self) {
        self.grid_mut().row_dec_scroll(1);
    }

    // ESC c
    pub(crate) fn ris(&mut self) {
        *self = Self::new(self.grid.size(), 0);
    }

    // csi codes

    // CSI @
    pub(crate) fn ich(&mut self, _count: u16) {
        // Insert characters - damages current row
        let pos = self.grid().pos();
        self.grid_mut().mark_row_damaged(pos.row);
    }

    // CSI A
    pub(crate) fn cuu(&mut self, offset: u16) {
        self.grid_mut().row_dec_clamp(offset);
    }

    // CSI B
    pub(crate) fn cud(&mut self, offset: u16) {
        self.grid_mut().row_inc_clamp(offset);
    }

    // CSI C
    pub(crate) fn cuf(&mut self, offset: u16) {
        self.grid_mut().col_inc_clamp(offset);
    }

    // CSI D
    pub(crate) fn cub(&mut self, offset: u16) {
        self.grid_mut().col_dec(offset);
    }

    // CSI E
    pub(crate) fn cnl(&mut self, offset: u16) {
        self.grid_mut().col_set(0);
        self.grid_mut().row_inc_clamp(offset);
    }

    // CSI F
    pub(crate) fn cpl(&mut self, offset: u16) {
        self.grid_mut().col_set(0);
        self.grid_mut().row_dec_clamp(offset);
    }

    // CSI G
    pub(crate) fn cha(&mut self, col: u16) {
        self.grid_mut().col_set(col - 1);
    }

    // CSI H
    pub(crate) fn cup(&mut self, (row, col): (u16, u16)) {
        self.grid_mut().set_pos(crate::grid::Pos {
            row: row - 1,
            col: col - 1,
        });
    }

    // CSI J
    pub(crate) fn ed(&mut self, mode: u16, mut unhandled: impl FnMut(&mut Self)) {
        let pos = self.grid().pos();
        let size = self.grid().size();
        match mode {
            0 => {
                // Erase from cursor to end of screen - damages current row and all below
                self.grid_mut().mark_rows_damaged(pos.row, size.rows);
            }
            1 => {
                // Erase from start of screen to cursor - damages all rows from start to current
                self.grid_mut().mark_rows_damaged(0, pos.row + 1);
            }
            2 => {
                // Erase entire screen - damages all rows
                self.grid_mut().mark_all_rows_damaged();
            }
            _ => unhandled(self),
        }
    }

    // CSI ? J
    pub(crate) fn decsed(&mut self, mode: u16, unhandled: impl FnMut(&mut Self)) {
        self.ed(mode, unhandled);
    }

    // CSI K
    pub(crate) fn el(&mut self, mode: u16, mut unhandled: impl FnMut(&mut Self)) {
        let pos = self.grid().pos();
        match mode {
            0 | 1 | 2 => {
                // Erase line - all modes damage the current row
                self.grid_mut().mark_row_damaged(pos.row);
            }
            _ => unhandled(self),
        }
    }

    // CSI ? K
    pub(crate) fn decsel(&mut self, mode: u16, unhandled: impl FnMut(&mut Self)) {
        self.el(mode, unhandled);
    }

    // CSI L
    pub(crate) fn il(&mut self, _count: u16) {
        // Insert lines - damages current row and all rows below (they shift down)
        let pos = self.grid().pos();
        let size = self.grid().size();
        self.grid_mut().mark_rows_damaged(pos.row, size.rows);
    }

    // CSI M
    pub(crate) fn dl(&mut self, _count: u16) {
        // Delete lines - damages current row and all rows below (they shift up)
        let pos = self.grid().pos();
        let size = self.grid().size();
        self.grid_mut().mark_rows_damaged(pos.row, size.rows);
    }

    // CSI P
    pub(crate) fn dch(&mut self, _count: u16) {
        // Delete characters - damages current row
        let pos = self.grid().pos();
        self.grid_mut().mark_row_damaged(pos.row);
    }

    // CSI S
    pub(crate) fn su(&mut self, _count: u16) {
        // Scroll up - damages all rows (they all shift)
        self.grid_mut().mark_all_rows_damaged();
    }

    // CSI T
    pub(crate) fn sd(&mut self, _count: u16) {
        // Scroll down - damages all rows (they all shift)
        self.grid_mut().mark_all_rows_damaged();
    }

    // CSI X
    pub(crate) fn ech(&mut self, _count: u16) {
        // Erase characters - damages current row
        let pos = self.grid().pos();
        self.grid_mut().mark_row_damaged(pos.row);
    }

    // CSI d
    pub(crate) fn vpa(&mut self, row: u16) {
        self.grid_mut().row_set(row - 1);
    }

    // CSI ? h
    pub(crate) fn decset(&mut self, params: &vte::Params, mut unhandled: impl FnMut(&mut Self)) {
        for param in params {
            match param {
                [1] => self.set_mode(MODE_APPLICATION_CURSOR),
                [6] => self.grid_mut().set_origin_mode(true),
                [9] => self.set_mouse_mode(MouseProtocolMode::Press),
                [25] => self.clear_mode(MODE_HIDE_CURSOR),
                [47] => self.enter_alternate_grid(),
                [1000] => {
                    self.set_mouse_mode(MouseProtocolMode::PressRelease);
                }
                [1002] => {
                    self.set_mouse_mode(MouseProtocolMode::ButtonMotion);
                }
                [1003] => self.set_mouse_mode(MouseProtocolMode::AnyMotion),
                [1005] => {
                    self.set_mouse_encoding(MouseProtocolEncoding::Utf8);
                }
                [1006] => {
                    self.set_mouse_encoding(MouseProtocolEncoding::Sgr);
                }
                [1049] => {
                    self.decsc();
                    self.alternate_grid.clear();
                    self.enter_alternate_grid();
                }
                [2004] => self.set_mode(MODE_BRACKETED_PASTE),
                _ => unhandled(self),
            }
        }
    }

    // CSI ? l
    pub(crate) fn decrst(&mut self, params: &vte::Params, mut unhandled: impl FnMut(&mut Self)) {
        for param in params {
            match param {
                [1] => self.clear_mode(MODE_APPLICATION_CURSOR),
                [6] => self.grid_mut().set_origin_mode(false),
                [9] => self.clear_mouse_mode(MouseProtocolMode::Press),
                [25] => self.set_mode(MODE_HIDE_CURSOR),
                [47] => {
                    self.exit_alternate_grid();
                }
                [1000] => {
                    self.clear_mouse_mode(MouseProtocolMode::PressRelease);
                }
                [1002] => {
                    self.clear_mouse_mode(MouseProtocolMode::ButtonMotion);
                }
                [1003] => {
                    self.clear_mouse_mode(MouseProtocolMode::AnyMotion);
                }
                [1005] => {
                    self.clear_mouse_encoding(MouseProtocolEncoding::Utf8);
                }
                [1006] => {
                    self.clear_mouse_encoding(MouseProtocolEncoding::Sgr);
                }
                [1049] => {
                    self.exit_alternate_grid();
                    self.decrc();
                }
                [2004] => self.clear_mode(MODE_BRACKETED_PASTE),
                _ => unhandled(self),
            }
        }
    }

    // CSI m
    pub(crate) fn sgr(&mut self, params: &vte::Params, mut unhandled: impl FnMut(&mut Self)) {
        // XXX really i want to just be able to pass in a default Params
        // instance with a 0 in it, but vte doesn't allow creating new Params
        // instances
        if params.is_empty() {
            self.attrs = crate::attrs::Attrs::default();
            return;
        }

        let mut iter = params.iter();

        macro_rules! next_param {
            () => {
                match iter.next() {
                    Some(n) => n,
                    _ => return,
                }
            };
        }

        macro_rules! to_u8 {
            ($n:expr) => {
                if let Some(n) = u16_to_u8($n) {
                    n
                } else {
                    return;
                }
            };
        }

        macro_rules! next_param_u8 {
            () => {
                if let &[n] = next_param!() {
                    to_u8!(n)
                } else {
                    return;
                }
            };
        }

        loop {
            match next_param!() {
                [0] => self.attrs = crate::attrs::Attrs::default(),
                [1] => self.attrs.set_bold(),
                [2] => self.attrs.set_dim(),
                [3] => self.attrs.set_italic(true),
                [4] => self.attrs.set_underline(true),
                [7] => self.attrs.set_inverse(true),
                [22] => self.attrs.set_normal_intensity(),
                [23] => self.attrs.set_italic(false),
                [24] => self.attrs.set_underline(false),
                [27] => self.attrs.set_inverse(false),
                [n] if (30..=37).contains(n) => {
                    self.attrs.fgcolor = crate::Color::Idx(to_u8!(*n) - 30);
                }
                [38, 2, r, g, b] => {
                    self.attrs.fgcolor = crate::Color::Rgb(to_u8!(*r), to_u8!(*g), to_u8!(*b));
                }
                [38, 5, i] => {
                    self.attrs.fgcolor = crate::Color::Idx(to_u8!(*i));
                }
                [38] => match next_param!() {
                    [2] => {
                        let r = next_param_u8!();
                        let g = next_param_u8!();
                        let b = next_param_u8!();
                        self.attrs.fgcolor = crate::Color::Rgb(r, g, b);
                    }
                    [5] => {
                        self.attrs.fgcolor = crate::Color::Idx(next_param_u8!());
                    }
                    _ => {
                        unhandled(self);
                        return;
                    }
                },
                [39] => {
                    self.attrs.fgcolor = crate::Color::Default;
                }
                [n] if (40..=47).contains(n) => {
                    self.attrs.bgcolor = crate::Color::Idx(to_u8!(*n) - 40);
                }
                [48, 2, r, g, b] => {
                    self.attrs.bgcolor = crate::Color::Rgb(to_u8!(*r), to_u8!(*g), to_u8!(*b));
                }
                [48, 5, i] => {
                    self.attrs.bgcolor = crate::Color::Idx(to_u8!(*i));
                }
                [48] => match next_param!() {
                    [2] => {
                        let r = next_param_u8!();
                        let g = next_param_u8!();
                        let b = next_param_u8!();
                        self.attrs.bgcolor = crate::Color::Rgb(r, g, b);
                    }
                    [5] => {
                        self.attrs.bgcolor = crate::Color::Idx(next_param_u8!());
                    }
                    _ => {
                        unhandled(self);
                        return;
                    }
                },
                [49] => {
                    self.attrs.bgcolor = crate::Color::Default;
                }
                [n] if (90..=97).contains(n) => {
                    self.attrs.fgcolor = crate::Color::Idx(to_u8!(*n) - 82);
                }
                [n] if (100..=107).contains(n) => {
                    self.attrs.bgcolor = crate::Color::Idx(to_u8!(*n) - 92);
                }
                _ => unhandled(self),
            }
        }
    }

    // CSI r
    pub(crate) fn decstbm(&mut self, (_top, _bottom): (u16, u16)) {
        // No-op: no scroll region to set
        self.grid_mut().set_pos(crate::grid::Pos { row: 0, col: 0 });
    }
}

fn u16_to_u8(i: u16) -> Option<u8> {
    if i > u16::from(u8::MAX) {
        None
    } else {
        // safe because we just ensured that the value fits in a u8
        Some(i.try_into().unwrap())
    }
}
