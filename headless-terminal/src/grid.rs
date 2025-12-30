use crate::term::BufWrite as _;
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct Grid {
    size: Size,
    pos: Pos,
    saved_pos: Pos,
    damaged_rows: HashSet<u16>,
}

impl Grid {
    pub fn new(size: Size, _scrollback_len: usize) -> Self {
        Self {
            size,
            pos: Pos::default(),
            saved_pos: Pos::default(),
            damaged_rows: HashSet::new(),
        }
    }

    pub fn clear(&mut self) {
        self.pos = Pos::default();
        self.saved_pos = Pos::default();
        // Mark all rows as damaged since clearing writes to all rows
        self.damaged_rows.clear();
        for row in 0..self.size.rows {
            self.damaged_rows.insert(row);
        }
    }

    pub fn size(&self) -> Size {
        self.size
    }

    pub fn set_size(&mut self, size: Size) {
        self.size = size;
        self.row_clamp();
        self.col_clamp();

        if self.saved_pos.row > self.size.rows - 1 {
            self.saved_pos.row = self.size.rows - 1;
        }
        if self.saved_pos.col > self.size.cols - 1 {
            self.saved_pos.col = self.size.cols - 1;
        }
    }

    pub fn pos(&self) -> Pos {
        self.pos
    }

    pub fn set_pos(&mut self, pos: Pos) {
        self.pos = pos;
        self.row_clamp();
        self.col_clamp();
    }

    pub fn save_cursor(&mut self) {
        self.saved_pos = self.pos;
    }

    pub fn restore_cursor(&mut self) {
        self.pos = self.saved_pos;
    }

    /// Marks a row as damaged (written to).
    pub fn mark_row_damaged(&mut self, row: u16) {
        if row < self.size.rows {
            self.damaged_rows.insert(row);
        }
    }

    /// Marks a range of rows as damaged (written to).
    pub fn mark_rows_damaged(&mut self, start_row: u16, end_row: u16) {
        let end = end_row.min(self.size.rows);
        for row in start_row..end {
            self.damaged_rows.insert(row);
        }
    }

    /// Marks all rows as damaged.
    pub fn mark_all_rows_damaged(&mut self) {
        for row in 0..self.size.rows {
            self.damaged_rows.insert(row);
        }
    }

    /// Returns the set of damaged rows and clears the damage tracking.
    pub fn take_damaged_rows(&mut self) -> HashSet<u16> {
        std::mem::take(&mut self.damaged_rows)
    }

    /// Returns a reference to the set of damaged rows without clearing it.
    pub fn damaged_rows(&self) -> &HashSet<u16> {
        &self.damaged_rows
    }

    pub fn write_cursor_position_formatted(
        &self,
        contents: &mut Vec<u8>,
        prev_pos: Option<Pos>,
        _prev_attrs: Option<crate::attrs::Attrs>,
    ) {
        if let Some(prev_pos) = prev_pos {
            if prev_pos != self.pos {
                crate::term::MoveFromTo::new(prev_pos, self.pos).write_buf(contents);
            }
        } else {
            crate::term::MoveTo::new(self.pos).write_buf(contents);
        }
    }

    pub fn set_origin_mode(&mut self, _mode: bool) {
        self.set_pos(Pos { row: 0, col: 0 });
    }

    pub fn row_inc_clamp(&mut self, count: u16) {
        self.pos.row = self.pos.row.saturating_add(count);
        self.row_clamp();
    }

    pub fn row_inc_scroll(&mut self, count: u16) -> u16 {
        self.pos.row = self.pos.row.saturating_add(count);
        self.row_clamp();
        0
    }

    pub fn row_dec_clamp(&mut self, count: u16) {
        self.pos.row = self.pos.row.saturating_sub(count);
        self.row_clamp();
    }

    pub fn row_dec_scroll(&mut self, count: u16) {
        self.pos.row = self.pos.row.saturating_sub(count);
        self.row_clamp();
    }

    pub fn row_set(&mut self, i: u16) {
        self.pos.row = i;
        self.row_clamp();
    }

    pub fn col_inc(&mut self, count: u16) {
        self.pos.col = self.pos.col.saturating_add(count);
    }

    pub fn col_inc_clamp(&mut self, count: u16) {
        self.pos.col = self.pos.col.saturating_add(count);
        self.col_clamp();
    }

    pub fn col_dec(&mut self, count: u16) {
        self.pos.col = self.pos.col.saturating_sub(count);
    }

    pub fn col_tab(&mut self) {
        self.pos.col -= self.pos.col % 8;
        self.pos.col += 8;
        self.col_clamp();
    }

    pub fn col_set(&mut self, i: u16) {
        self.pos.col = i;
        self.col_clamp();
    }

    pub fn col_wrap(&mut self, width: u16, _wrap: bool) {
        if self.pos.col > self.size.cols - width {
            // Mark current row as damaged before wrapping
            self.mark_row_damaged(self.pos.row);
            self.pos.col = 0;
            let old_row = self.pos.row;
            self.row_inc_scroll(1);
            // Mark the new row as damaged after wrapping
            if self.pos.row != old_row {
                self.mark_row_damaged(self.pos.row);
            }
        }
    }

    fn row_clamp(&mut self) {
        if self.pos.row > self.size.rows - 1 {
            self.pos.row = self.size.rows - 1;
        }
    }

    fn col_clamp(&mut self) {
        if self.pos.col > self.size.cols - 1 {
            self.pos.col = self.size.cols - 1;
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct Size {
    pub rows: u16,
    pub cols: u16,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct Pos {
    pub row: u16,
    pub col: u16,
}
