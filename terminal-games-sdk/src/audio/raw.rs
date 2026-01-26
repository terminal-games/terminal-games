// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Raw audio resource from pre-decoded samples.

use super::{Decoder, Instance, Resource, SAMPLE_RATE, mixer};
use std::sync::Arc;
use std::time::Duration;

/// An audio resource created from raw f32 samples.
///
/// This is useful when you have pre-generated or pre-decoded audio samples.
///
/// # Example
///
/// ```no_run
/// use terminal_games_sdk::audio::{RawResource, Resource, SAMPLE_RATE};
/// use std::sync::Arc;
///
/// // Generate a 1-second sine wave
/// let frequency = 440.0;
/// let samples: Vec<f32> = (0..SAMPLE_RATE as usize)
///     .map(|i| {
///         let t = i as f32 / SAMPLE_RATE as f32;
///         (2.0 * std::f32::consts::PI * frequency * t).sin() * 0.5
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
    /// Creates a new RawResource from the given samples.
    ///
    /// Samples should be mono f32 values in the range [-1.0, 1.0].
    pub fn new(samples: Vec<f32>) -> Self {
        Self { samples }
    }

    /// Creates a new RawResource from a slice of samples.
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
        Duration::from_secs_f64(self.samples.len() as f64 / SAMPLE_RATE as f64)
    }

    fn sample_count(&self) -> usize {
        self.samples.len()
    }

    fn new_instance(self: &Arc<Self>) -> Arc<Instance> {
        let decoder = Box::new(RawDecoder::new(Arc::clone(self)));
        let instance = Arc::new(Instance::new(decoder));
        mixer().add_instance(&instance);
        instance
    }
}

/// Decoder for raw samples.
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
        let remaining = samples.len().saturating_sub(self.position);

        if remaining == 0 {
            return 0;
        }

        let to_read = buffer.len().min(remaining);
        buffer[..to_read].copy_from_slice(&samples[self.position..self.position + to_read]);
        self.position += to_read;

        to_read
    }

    fn seek(&mut self, position: usize) {
        self.position = position.min(self.resource.samples.len());
    }

    fn position(&self) -> usize {
        self.position
    }

    fn length(&self) -> usize {
        self.resource.samples.len()
    }
}
