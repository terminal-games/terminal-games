// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Raw audio resource from pre-decoded interleaved stereo samples.

use super::{CHANNELS, Decoder, Instance, Resource, SAMPLE_RATE, mixer};
use std::sync::Arc;
use std::time::Duration;

/// An audio resource created from raw interleaved stereo f32 samples.
///
/// This is useful when you have pre-generated or pre-decoded audio samples.
///
/// # Example
///
/// ```no_run
/// use terminal_games_sdk::audio::{RawResource, Resource, SAMPLE_RATE, CHANNELS};
/// use std::sync::Arc;
///
/// // Generate a 1-second stereo sine wave (same on both channels)
/// let frequency = 440.0;
/// let samples: Vec<f32> = (0..SAMPLE_RATE as usize)
///     .flat_map(|i| {
///         let t = i as f32 / SAMPLE_RATE as f32;
///         let sample = (2.0 * std::f32::consts::PI * frequency * t).sin() * 0.5;
///         [sample, sample] // Left, Right
///     })
///     .collect();
///
/// let resource = Arc::new(RawResource::new(samples));
/// let instance = resource.new_instance();
/// instance.play();
/// ```
pub struct RawResource {
    samples: Vec<f32>,
}

impl RawResource {
    /// Creates a new RawResource from the given interleaved stereo samples.
    ///
    /// Samples should be interleaved stereo f32 values in the range [-1.0, 1.0]:
    /// `[L0, R0, L1, R1, ...]`.
    pub fn new(samples: Vec<f32>) -> Self {
        Self { samples }
    }

    /// Creates a new RawResource from a slice of interleaved stereo samples.
    ///
    /// This copies the samples into the resource.
    pub fn from_slice(samples: &[f32]) -> Self {
        Self {
            samples: samples.to_vec(),
        }
    }
}

impl Resource for RawResource {
    fn duration(&self) -> Duration {
        let frame_count = self.samples.len() / CHANNELS;
        Duration::from_secs_f64(frame_count as f64 / SAMPLE_RATE as f64)
    }

    fn sample_count(&self) -> usize {
        self.samples.len() / CHANNELS
    }

    fn new_instance(self: &Arc<Self>) -> Arc<Instance> {
        let decoder = Box::new(RawDecoder::new(Arc::clone(self)));
        let instance = Arc::new(Instance::new(decoder));
        mixer().add_instance(&instance);
        instance
    }
}

/// Decoder for raw interleaved stereo samples.
struct RawDecoder {
    resource: Arc<RawResource>,
    position: usize,
}

impl RawDecoder {
    fn new(resource: Arc<RawResource>) -> Self {
        Self {
            resource,
            position: 0,
        }
    }
}

impl Decoder for RawDecoder {
    fn read(&mut self, buffer: &mut [f32]) -> usize {
        let samples = &self.resource.samples;
        let total_frames = samples.len() / CHANNELS;
        let remaining_frames = total_frames.saturating_sub(self.position);

        if remaining_frames == 0 {
            return 0;
        }

        let frames_to_read = (buffer.len() / CHANNELS).min(remaining_frames);
        let values_to_read = frames_to_read * CHANNELS;
        let src_offset = self.position * CHANNELS;

        buffer[..values_to_read]
            .copy_from_slice(&samples[src_offset..src_offset + values_to_read]);
        self.position += frames_to_read;

        values_to_read
    }

    fn seek(&mut self, position: usize) {
        let total_frames = self.resource.samples.len() / CHANNELS;
        self.position = position.min(total_frames);
    }

    fn position(&self) -> usize {
        self.position
    }

    fn length(&self) -> usize {
        self.resource.samples.len() / CHANNELS
    }
}
