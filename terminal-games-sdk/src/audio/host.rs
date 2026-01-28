// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use super::FRAME_SIZE;
use crate::internal;

#[derive(Debug, Clone, Copy)]
pub struct AudioInfo {
    pub frame_size: u32,
    pub sample_rate: u32,
    pub pts: u64,
    pub buffer_available: u32,
}

/// Get audio timing information for synchronization.
pub fn info() -> Option<AudioInfo> {
    let mut frame_size: u32 = 0;
    let mut sample_rate: u32 = 0;
    let mut pts: u64 = 0;
    let mut buffer_available: u32 = 0;

    let ret = unsafe {
        internal::audio_info(
            &mut frame_size as *mut u32,
            &mut sample_rate as *mut u32,
            &mut pts as *mut u64,
            &mut buffer_available as *mut u32,
        )
    };

    if ret < 0 {
        return None;
    }

    Some(AudioInfo {
        frame_size,
        sample_rate,
        pts,
        buffer_available,
    })
}

/// Write interleaved stereo audio samples to the mixer buffer.
///
/// Samples should be interleaved stereo f32 values: `[L0, R0, L1, R1, ...]`.
/// Values should be in the range `[-1.0, 1.0]`.
///
/// This function is non-blocking. If the buffer is full, frames may be
/// dropped. Check the return value to see how many frames were actually
/// written.
///
/// Returns the number of frames written, or a negative value on error.
pub fn write(samples: &[f32]) -> i32 {
    if samples.is_empty() {
        return 0;
    }

    let frame_count = samples.len() / super::CHANNELS;
    unsafe { internal::audio_write(samples.as_ptr(), frame_count as u32) }
}

/// Helper for continuously writing audio with automatic timing management.
pub struct AudioWriter {
    pub next_pts: u64,
    pub target_buffer: u32,
}

impl AudioWriter {
    pub fn new(target_buffer: u32) -> Self {
        Self {
            next_pts: 0,
            target_buffer,
        }
    }

    /// Checks if more audio should be written based on buffer state.
    ///
    /// Returns the number of samples that should be written, or 0 if the buffer
    /// is sufficiently full. This is useful for rate-limiting audio generation.
    pub fn should_write(&mut self) -> usize {
        let Some(audio_info) = info() else {
            return 0;
        };

        if self.next_pts < audio_info.pts {
            self.next_pts = audio_info.pts;
        }

        if audio_info.buffer_available >= self.target_buffer {
            return 0;
        }

        let needed = (self.target_buffer - audio_info.buffer_available) as usize;
        let frames = (needed + FRAME_SIZE - 1) / FRAME_SIZE;
        frames * FRAME_SIZE
    }

    /// Calls the provided function to generate samples when needed.
    ///
    /// The callback receives the PTS and number of frames to generate, and should
    /// return interleaved stereo float32 samples: `[L0, R0, L1, R1, ...]`.
    ///
    /// This method updates `next_pts` automatically based on how many frames were
    /// written.
    pub fn write_callback<F>(&mut self, generate: F) -> i32
    where
        F: FnOnce(u64, usize) -> Vec<f32>,
    {
        let needed = self.should_write();
        if needed == 0 {
            return 0;
        }

        let samples = generate(self.next_pts, needed);
        let written = write(&samples);
        if written > 0 {
            self.next_pts += written as u64;
        }
        written
    }
}

impl Default for AudioWriter {
    fn default() -> Self {
        Self::new(FRAME_SIZE as u32 * 2)
    }
}
