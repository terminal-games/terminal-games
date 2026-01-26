// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Convert any audio file into OGG Vorbis with an ffmpeg command like the following ahead of time:
//!
//! ```
//! ffmpeg -i input.mp3 -af "pan=mono|c0=c1" -c:a libvorbis -qscale:a 2 -ar 48000 output.ogg
//! ```

use super::{Decoder, Instance, Resource, SAMPLE_RATE, mixer};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{CODEC_TYPE_NULL, Decoder as SymphoniaDecoder, DecoderOptions};
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::{FormatOptions, FormatReader, SeekMode, SeekTo};
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use symphonia::core::units::Time;

#[derive(Debug)]
pub enum OggVorbisError {
    ProbeError(String),
    NoTrack,
    DecoderError(String),
    UnsupportedCodec,
}

impl std::fmt::Display for OggVorbisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OggVorbisError::ProbeError(e) => write!(f, "Failed to probe media: {}", e),
            OggVorbisError::NoTrack => write!(f, "No audio track found"),
            OggVorbisError::DecoderError(e) => write!(f, "Decoder error: {}", e),
            OggVorbisError::UnsupportedCodec => write!(f, "Unsupported codec"),
        }
    }
}

impl std::error::Error for OggVorbisError {}

pub struct OggVorbisResource {
    data: Vec<u8>,
    source_sample_rate: u32,
    #[allow(dead_code)]
    source_channels: usize,
    total_length: usize,
}

impl OggVorbisResource {
    /// Creates a new OggVorbisResource from raw OGG Vorbis data.
    ///
    /// This probes the file to get metadata but does NOT decode the audio.
    /// Decoding happens on-demand when playing.
    pub fn new(data: &[u8]) -> Result<Self, OggVorbisError> {
        let cursor = Cursor::new(data.to_vec());
        let mss = MediaSourceStream::new(Box::new(cursor), Default::default());

        let mut hint = Hint::new();
        hint.with_extension("ogg");

        let probed = symphonia::default::get_probe()
            .format(
                &hint,
                mss,
                &FormatOptions::default(),
                &MetadataOptions::default(),
            )
            .map_err(|e| OggVorbisError::ProbeError(e.to_string()))?;

        let format = probed.format;

        let track = format
            .tracks()
            .iter()
            .find(|t| t.codec_params.codec != CODEC_TYPE_NULL)
            .ok_or(OggVorbisError::NoTrack)?;

        let codec_params = &track.codec_params;

        let source_sample_rate = codec_params.sample_rate.unwrap_or(SAMPLE_RATE);
        let source_channels = codec_params.channels.map(|c| c.count()).unwrap_or(1);

        let source_samples = codec_params.n_frames.unwrap_or(0) as usize;

        let total_length = if source_sample_rate != SAMPLE_RATE {
            (source_samples as f64 * SAMPLE_RATE as f64 / source_sample_rate as f64) as usize
        } else {
            source_samples
        };

        Ok(Self {
            data: data.to_vec(),
            source_sample_rate,
            source_channels,
            total_length,
        })
    }
}

impl Resource for OggVorbisResource {
    fn duration(&self) -> Duration {
        Duration::from_secs_f64(self.total_length as f64 / SAMPLE_RATE as f64)
    }

    fn sample_count(&self) -> usize {
        self.total_length
    }

    fn new_instance(self: &Arc<Self>) -> Arc<Instance> {
        let decoder = Box::new(OggVorbisDecoder::new(Arc::clone(self)));
        let instance = Arc::new(Instance::new(decoder));
        mixer().add_instance(&instance);
        instance
    }
}

struct OggVorbisDecoder {
    resource: Arc<OggVorbisResource>,
    position: usize,
    format: Box<dyn FormatReader>,
    decoder: Box<dyn SymphoniaDecoder>,
    track_id: u32,
    decode_buffer: Vec<f32>,
    buffer_pos: usize,
    resample_state: Option<ResampleState>,
}

struct ResampleState {
    source_rate: u32,
    target_rate: u32,
    frac_pos: f64,
    last_sample: f32,
}

impl OggVorbisDecoder {
    fn new(resource: Arc<OggVorbisResource>) -> Self {
        let (format, decoder, track_id) = create_symphonia_decoder(&resource.data);

        let resample_state = if resource.source_sample_rate != SAMPLE_RATE {
            Some(ResampleState {
                source_rate: resource.source_sample_rate,
                target_rate: SAMPLE_RATE,
                frac_pos: 0.0,
                last_sample: 0.0,
            })
        } else {
            None
        };

        Self {
            resource,
            position: 0,
            format,
            decoder,
            track_id,
            decode_buffer: Vec::with_capacity(8192),
            buffer_pos: 0,
            resample_state,
        }
    }

    fn decode_more(&mut self) -> bool {
        loop {
            let packet = match self.format.next_packet() {
                Ok(packet) => packet,
                Err(SymphoniaError::IoError(e))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return false;
                }
                Err(SymphoniaError::ResetRequired) => {
                    self.decoder.reset();
                    continue;
                }
                Err(_) => return false,
            };

            if packet.track_id() != self.track_id {
                continue;
            }

            let decoded = match self.decoder.decode(&packet) {
                Ok(decoded) => decoded,
                Err(SymphoniaError::DecodeError(_)) => continue,
                Err(_) => return false,
            };

            let spec = *decoded.spec();
            let duration = decoded.capacity();

            let mut sample_buf = SampleBuffer::<f32>::new(duration as u64, spec);
            sample_buf.copy_interleaved_ref(decoded);

            let samples = sample_buf.samples();
            let channels = spec.channels.count();

            self.decode_buffer.clear();
            self.buffer_pos = 0;

            if channels == 1 {
                self.decode_buffer.extend_from_slice(samples);
            } else {
                for chunk in samples.chunks(channels) {
                    let sum: f32 = chunk.iter().sum();
                    self.decode_buffer.push(sum / channels as f32);
                }
            }

            return true;
        }
    }

    fn read_resampled(&mut self, buffer: &mut [f32]) -> usize {
        let mut written = 0;

        while written < buffer.len() {
            if self.buffer_pos >= self.decode_buffer.len() {
                if !self.decode_more() {
                    break;
                }
            }

            if let Some(ref mut resample) = self.resample_state {
                // Resample with linear interpolation
                let ratio = resample.source_rate as f64 / resample.target_rate as f64;

                while written < buffer.len() && self.buffer_pos < self.decode_buffer.len() {
                    let src_idx = resample.frac_pos as usize;
                    let frac = (resample.frac_pos - src_idx as f64) as f32;

                    let current = if src_idx < self.decode_buffer.len() {
                        self.decode_buffer[src_idx]
                    } else {
                        resample.last_sample
                    };

                    let next = if src_idx + 1 < self.decode_buffer.len() {
                        self.decode_buffer[src_idx + 1]
                    } else {
                        current
                    };

                    buffer[written] = current + (next - current) * frac;
                    written += 1;

                    resample.frac_pos += ratio;

                    while resample.frac_pos >= 1.0 && self.buffer_pos < self.decode_buffer.len() {
                        resample.last_sample = self.decode_buffer[self.buffer_pos];
                        self.buffer_pos += 1;
                        resample.frac_pos -= 1.0;
                    }
                }
            } else {
                // No resampling needed
                let available = self.decode_buffer.len() - self.buffer_pos;
                let to_copy = (buffer.len() - written).min(available);

                buffer[written..written + to_copy].copy_from_slice(
                    &self.decode_buffer[self.buffer_pos..self.buffer_pos + to_copy],
                );

                self.buffer_pos += to_copy;
                written += to_copy;
            }
        }

        written
    }
}

impl Decoder for OggVorbisDecoder {
    fn read(&mut self, buffer: &mut [f32]) -> usize {
        let remaining = self.resource.total_length.saturating_sub(self.position);
        if remaining == 0 {
            return 0;
        }

        let to_read = buffer.len().min(remaining);
        let read = self.read_resampled(&mut buffer[..to_read]);
        self.position += read;
        read
    }

    fn seek(&mut self, position: usize) {
        let position = position.min(self.resource.total_length);

        let source_position = if self.resource.source_sample_rate != SAMPLE_RATE {
            (position as f64 * self.resource.source_sample_rate as f64 / SAMPLE_RATE as f64) as u64
        } else {
            position as u64
        };

        let seek_result = self.format.seek(
            SeekMode::Accurate,
            SeekTo::Time {
                time: Time::from(source_position as f64 / self.resource.source_sample_rate as f64),
                track_id: Some(self.track_id),
            },
        );

        if seek_result.is_err() {
            // If seeking fails, recreate the decoder and seek from start
            let (format, decoder, track_id) = create_symphonia_decoder(&self.resource.data);
            self.format = format;
            self.decoder = decoder;
            self.track_id = track_id;

            // For position 0, we're done
            // For other positions, we'd need to decode and skip, but that's expensive
            // so we just reset to 0 if seeking fails
        }

        self.position = position;
        self.decode_buffer.clear();
        self.buffer_pos = 0;

        if let Some(ref mut resample) = self.resample_state {
            resample.frac_pos = 0.0;
            resample.last_sample = 0.0;
        }
    }

    fn position(&self) -> usize {
        self.position
    }

    fn length(&self) -> usize {
        self.resource.total_length
    }
}

fn create_symphonia_decoder(
    data: &[u8],
) -> (Box<dyn FormatReader>, Box<dyn SymphoniaDecoder>, u32) {
    let cursor = Cursor::new(data.to_vec());
    let mss = MediaSourceStream::new(Box::new(cursor), Default::default());

    let mut hint = Hint::new();
    hint.with_extension("ogg");

    let probed = symphonia::default::get_probe()
        .format(
            &hint,
            mss,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )
        .expect("Failed to probe media");

    let format = probed.format;

    let track = format
        .tracks()
        .iter()
        .find(|t| t.codec_params.codec != CODEC_TYPE_NULL)
        .expect("No audio track");

    let track_id = track.id;
    let codec_params = track.codec_params.clone();

    let decoder = symphonia::default::get_codecs()
        .make(&codec_params, &DecoderOptions::default())
        .expect("Failed to create decoder");

    (format, decoder, track_id)
}
