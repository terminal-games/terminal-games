// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use super::{FRAME_SIZE, Instance, host};
use std::sync::{Arc, Mutex, OnceLock, Weak};

static GLOBAL_MIXER: OnceLock<Mixer> = OnceLock::new();

pub fn mixer() -> &'static Mixer {
    GLOBAL_MIXER.get_or_init(Mixer::new)
}

struct MixerState {
    instances: Vec<Weak<Instance>>,
    master_volume: f32,
    mix_buffer: Vec<f32>,
    scratch_buffer: Vec<f32>,
}

pub struct Mixer {
    state: Mutex<MixerState>,
}

impl Mixer {
    fn new() -> Self {
        Self {
            state: Mutex::new(MixerState {
                instances: Vec::new(),
                master_volume: 1.0,
                mix_buffer: Vec::new(),
                scratch_buffer: Vec::new(),
            }),
        }
    }

    /// Sets the master volume for all mixed audio.
    ///
    /// 0.0 = silent, 1.0 = full volume.
    pub fn set_master_volume(&self, volume: f32) {
        let mut state = self.state.lock().unwrap();
        state.master_volume = volume;
    }

    pub fn master_volume(&self) -> f32 {
        let state = self.state.lock().unwrap();
        state.master_volume
    }

    pub(crate) fn add_instance(&self, instance: &Arc<Instance>) {
        let mut state = self.state.lock().unwrap();
        state.instances.push(Arc::downgrade(instance));
    }

    fn mix(&self, num_frames: usize) -> Vec<f32> {
        let mut state = self.state.lock().unwrap();

        let num_values = num_frames * super::CHANNELS;
        if state.mix_buffer.len() < num_values {
            state.mix_buffer.resize(num_values, 0.0);
            state.scratch_buffer.resize(num_values, 0.0);
        }

        for sample in &mut state.mix_buffer[..num_values] {
            *sample = 0.0;
        }

        let live_instances: Vec<_> = state
            .instances
            .iter()
            .filter_map(|weak| weak.upgrade())
            .collect();
        state.instances.retain(|weak| weak.strong_count() > 0);

        for instance in live_instances {
            for sample in &mut state.scratch_buffer[..num_values] {
                *sample = 0.0;
            }

            instance.fill_buffer(&mut state.scratch_buffer[..num_values]);

            for i in 0..num_values {
                state.mix_buffer[i] += state.scratch_buffer[i];
            }
        }

        let master_volume = state.master_volume;
        for sample in &mut state.mix_buffer[..num_values] {
            *sample *= master_volume;
            *sample = sample.clamp(-1.0, 1.0);
        }

        state.mix_buffer[..num_values].to_vec()
    }

    /// Performs one tick of mixing and writing to the host.
    ///
    /// This should be called periodically from the main loop, typically
    /// every 5-10ms for smooth audio.
    pub fn tick(&self) {
        let Some(audio_info) = host::info() else {
            return;
        };

        let target_buffer = FRAME_SIZE as u32 * 2;

        if audio_info.buffer_available >= target_buffer {
            return;
        }

        let needed = (target_buffer - audio_info.buffer_available) as usize;
        let frames = (needed + FRAME_SIZE - 1) / FRAME_SIZE;
        let num_frames = frames * FRAME_SIZE;

        if num_frames == 0 {
            return;
        }

        let mixed = self.mix(num_frames);
        host::write(&mixed);
    }
}
