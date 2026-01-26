// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use super::{Decoder, SAMPLE_RATE};
use std::sync::Mutex;
use std::time::Duration;

struct InstanceState {
    decoder: Box<dyn Decoder>,
    volume: f32,
    playing: bool,
    looping: bool,
}

/// A playing instance of an audio resource, with playback controls.
///
/// Instances are created via [`Resource::new_instance()`](super::Resource::new_instance).
pub struct Instance {
    state: Mutex<InstanceState>,
}

impl Instance {
    pub(crate) fn new(decoder: Box<dyn Decoder>) -> Self {
        Self {
            state: Mutex::new(InstanceState {
                decoder,
                volume: 1.0,
                playing: false,
                looping: false,
            }),
        }
    }

    /// Starts or resumes playback.
    pub fn play(&self) {
        let mut state = self.state.lock().unwrap();
        state.playing = true;
    }

    /// Pauses playback without resetting position.
    pub fn pause(&self) {
        let mut state = self.state.lock().unwrap();
        state.playing = false;
    }

    /// Stops playback and resets position to the beginning.
    pub fn stop(&self) {
        let mut state = self.state.lock().unwrap();
        state.playing = false;
        state.decoder.seek(0);
    }

    /// Sets the volume multiplier.
    ///
    /// 0.0 = silent, 1.0 = full volume. Values > 1.0 are allowed for amplification.
    pub fn set_volume(&self, volume: f32) {
        let mut state = self.state.lock().unwrap();
        state.volume = volume;
    }

    /// Returns the current volume.
    pub fn volume(&self) -> f32 {
        let state = self.state.lock().unwrap();
        state.volume
    }

    /// Sets whether the instance should loop when reaching the end.
    pub fn set_loop(&self, looping: bool) {
        let mut state = self.state.lock().unwrap();
        state.looping = looping;
    }

    /// Returns whether looping is enabled.
    pub fn is_looping(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.looping
    }

    /// Returns true if the instance is currently playing.
    pub fn is_playing(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.playing
    }

    /// Returns the current playback position in samples.
    pub fn position(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.decoder.position()
    }

    /// Sets the playback position in samples.
    pub fn seek(&self, position: usize) {
        let mut state = self.state.lock().unwrap();
        state.decoder.seek(position);
    }

    /// Sets the playback position as a duration from the start.
    pub fn seek_duration(&self, duration: Duration) {
        let samples = (duration.as_secs_f64() * SAMPLE_RATE as f64) as usize;
        self.seek(samples);
    }

    /// Returns the current playback position as a duration.
    pub fn current_time(&self) -> Duration {
        let state = self.state.lock().unwrap();
        let position = state.decoder.position();
        Duration::from_secs_f64(position as f64 / SAMPLE_RATE as f64)
    }

    /// Returns the total duration of the audio.
    pub fn duration(&self) -> Duration {
        let state = self.state.lock().unwrap();
        let length = state.decoder.length();
        Duration::from_secs_f64(length as f64 / SAMPLE_RATE as f64)
    }

    pub(crate) fn fill_buffer(&self, buffer: &mut [f32]) -> usize {
        let mut state = self.state.lock().unwrap();

        if !state.playing {
            return 0;
        }

        let mut written = 0;

        while written < buffer.len() {
            let n = state.decoder.read(&mut buffer[written..]);

            if n > 0 {
                let volume = state.volume;
                for sample in &mut buffer[written..written + n] {
                    *sample *= volume;
                }
                written += n;
            }

            if n == 0 {
                if state.looping {
                    state.decoder.seek(0);
                } else {
                    state.playing = false;
                    break;
                }
            }
        }

        written
    }
}
