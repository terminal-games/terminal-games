// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod host;
mod instance;
mod mixer;

#[cfg(feature = "ogg-vorbis")]
mod ogg_vorbis;
mod raw;

pub use host::{AudioInfo, AudioWriter, info, write};
pub use instance::Instance;
pub use mixer::{Mixer, mixer};

#[cfg(feature = "ogg-vorbis")]
pub use ogg_vorbis::OggVorbisResource;
pub use raw::RawResource;

use std::sync::Arc;
use std::time::Duration;

pub const SAMPLE_RATE: u32 = 48000;
pub const FRAME_SIZE: usize = 480;
pub const CHANNELS: usize = 1;

/// Resources represent audio data that can be instantiated for playback.
/// A single Resource can have multiple Instances playing simultaneously.
pub trait Resource: Send + Sync {
    /// Returns the total duration of the audio.
    fn duration(&self) -> Duration;

    /// Returns the total number of samples.
    fn sample_count(&self) -> usize;

    /// Creates a new playable Instance of this Resource.
    ///
    /// The instance starts paused at position 0 with volume 1.0.
    /// The instance is automatically registered with the global mixer.
    fn new_instance(self: &Arc<Self>) -> Arc<Instance>;
}

pub(crate) trait Decoder: Send {
    /// Fills the buffer with samples and returns the number of samples read.
    ///
    /// Returns 0 when reaching the end of the audio.
    fn read(&mut self, buffer: &mut [f32]) -> usize;

    /// Sets the playback position in samples.
    fn seek(&mut self, position: usize);

    /// Returns the current playback position in samples.
    fn position(&self) -> usize;

    /// Returns the total number of samples.
    fn length(&self) -> usize;
}
