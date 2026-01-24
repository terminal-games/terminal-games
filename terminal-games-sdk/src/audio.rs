// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Audio support for terminal-games SDK.
//!
//! This module provides functions for WASM apps to produce audio that gets
//! mixed and streamed to connected clients.
//!
//! # Audio Model
//!
//! The host runs an audio mixer at 48kHz stereo. WASM apps write samples to a
//! ring buffer, and the mixer consumes them. If the buffer is empty, silence
//! is output. If the buffer is full, samples are dropped.
//!
//! # Timing
//!
//! Use [`info()`] to get the current playback position (PTS) and buffer state.
//! Apps should generate samples ahead of the current PTS to avoid underruns.
//!
//! # Example
//!
//! ```no_run
//! use terminal_games_sdk::audio;
//!
//! // Get audio timing info
//! let info = audio::info().unwrap();
//! println!("Sample rate: {}, Frame size: {}", info.sample_rate, info.frame_size);
//!
//! // Generate a simple sine wave
//! let frequency = 440.0; // A4
//! let amplitude = 0.3;
//! let mut samples = vec![0.0f32; info.frame_size * 2]; // stereo
//!
//! for i in 0..info.frame_size {
//!     let t = (info.pts + i as u64) as f32 / info.sample_rate as f32;
//!     let value = amplitude * (2.0 * std::f32::consts::PI * frequency * t).sin();
//!     samples[i * 2] = value;     // left
//!     samples[i * 2 + 1] = value; // right
//! }
//!
//! // Write samples to the mixer
//! let written = audio::write(&samples);
//! ```

use crate::internal;

pub const SAMPLE_RATE: u32 = 48000;
pub const FRAME_SIZE: usize = 960;
pub const CHANNELS: usize = 2;

#[derive(Debug, Clone, Copy)]
pub struct AudioInfo {
    pub frame_size: u32,
    pub sample_rate: u32,
    pub pts: u64,
    pub buffer_available: u32,
}

/// Get audio timing information for synchronization.
///
/// Returns information about the audio system including the current playback
/// position (PTS), which apps can use to generate correctly timed samples.
///
/// # Returns
///
/// `Some(AudioInfo)` on success, `None` if the host call fails.
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

/// Write stereo audio samples to the mixer buffer.
///
/// Samples should be interleaved stereo f32 values: `[L0, R0, L1, R1, ...]`.
/// Values should be in the range `[-1.0, 1.0]`.
///
/// This function is non-blocking. If the buffer is full, samples may be
/// dropped. Check the return value to see how many sample pairs were actually
/// written.
///
/// # Arguments
///
/// * `samples` - Interleaved stereo f32 samples. Length must be even.
///
/// # Returns
///
/// Number of stereo sample pairs written, or -1 on error.
///
/// # Example
///
/// ```no_run
/// use terminal_games_sdk::audio;
///
/// // Write 100 sample pairs (200 floats)
/// let samples = vec![0.0f32; 200];
/// let written = audio::write(&samples);
/// assert!(written >= 0);
/// ```
pub fn write(samples: &[f32]) -> i32 {
    if samples.is_empty() {
        return 0;
    }

    let sample_pairs = samples.len() / CHANNELS;
    if sample_pairs == 0 {
        return 0;
    }

    unsafe { internal::audio_write(samples.as_ptr(), sample_pairs as u32) }
}
