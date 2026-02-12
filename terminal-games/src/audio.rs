// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    io::{self, Write},
    time::{Duration, Instant},
};

use ogg::writing::{PacketWriteEndInfo, PacketWriter};
use opus::{Application, Bitrate, Channels, Encoder};
use tokio_util::sync::CancellationToken;

pub const SAMPLE_RATE: u32 = 48000;
pub const FRAME_SIZE: usize = 480;
pub const CHANNELS: usize = 2;

struct AudioBufferInner {
    samples: Vec<f32>,
    write_pos: usize,
    read_pos: usize,
}

pub struct AudioBuffer {
    inner: Mutex<AudioBufferInner>,
    capacity: usize,
    pub pts: AtomicUsize,
}

impl AudioBuffer {
    pub fn new(capacity_frames: usize) -> Self {
        Self {
            inner: Mutex::new(AudioBufferInner {
                samples: vec![0.0; capacity_frames * CHANNELS],
                write_pos: 0,
                read_pos: 0,
            }),
            capacity: capacity_frames,
            pts: AtomicUsize::new(0),
        }
    }

    /// Write interleaved stereo samples to the buffer. Returns number of frames written.
    /// Input should be interleaved: [L0, R0, L1, R1, ...].
    /// This is non-blocking; if buffer is full, frames are dropped.
    pub fn write(&self, samples: &[f32]) -> usize {
        let frame_count = samples.len() / CHANNELS;
        if frame_count == 0 {
            return 0;
        }

        let mut inner = self.inner.lock().unwrap();

        let used = if inner.write_pos >= inner.read_pos {
            inner.write_pos - inner.read_pos
        } else {
            self.capacity - inner.read_pos + inner.write_pos
        };
        let available = self.capacity.saturating_sub(used + 1);

        let to_write = frame_count.min(available);
        if to_write == 0 {
            return 0;
        }

        let frames_until_wrap = self.capacity - inner.write_pos;
        let first_chunk = to_write.min(frames_until_wrap);
        let second_chunk = to_write - first_chunk;

        let dest_start = inner.write_pos * CHANNELS;
        inner.samples[dest_start..dest_start + first_chunk * CHANNELS]
            .copy_from_slice(&samples[..first_chunk * CHANNELS]);

        if second_chunk > 0 {
            inner.samples[..second_chunk * CHANNELS].copy_from_slice(
                &samples[first_chunk * CHANNELS..first_chunk * CHANNELS + second_chunk * CHANNELS],
            );
        }

        inner.write_pos = (inner.write_pos + to_write) % self.capacity;

        to_write
    }

    /// Read interleaved stereo samples from the buffer into the destination.
    /// Returns number of frames read. Missing frames are filled with silence.
    fn read(&self, dest: &mut [f32], frame_count: usize) -> usize {
        let mut inner = self.inner.lock().unwrap();

        let available = if inner.write_pos >= inner.read_pos {
            inner.write_pos - inner.read_pos
        } else {
            self.capacity - inner.read_pos + inner.write_pos
        };

        let to_read = frame_count.min(available);

        if to_read > 0 {
            let frames_until_wrap = self.capacity - inner.read_pos;
            let first_chunk = to_read.min(frames_until_wrap);
            let second_chunk = to_read - first_chunk;

            let src_start = inner.read_pos * CHANNELS;
            dest[..first_chunk * CHANNELS]
                .copy_from_slice(&inner.samples[src_start..src_start + first_chunk * CHANNELS]);

            if second_chunk > 0 {
                let dest_start = first_chunk * CHANNELS;
                dest[dest_start..dest_start + second_chunk * CHANNELS]
                    .copy_from_slice(&inner.samples[..second_chunk * CHANNELS]);
            }

            inner.read_pos = (inner.read_pos + to_read) % self.capacity;
        }

        if to_read < frame_count {
            let silence_start = to_read * CHANNELS;
            let silence_end = frame_count * CHANNELS;
            dest[silence_start..silence_end].fill(0.0);
        }

        to_read
    }

    pub fn available(&self) -> usize {
        let inner = self.inner.lock().unwrap();

        if inner.write_pos >= inner.read_pos {
            inner.write_pos - inner.read_pos
        } else {
            self.capacity - inner.read_pos + inner.write_pos
        }
    }
}

pub struct Mixer {
    encoder: Encoder,
    packet_writer: PacketWriter<'static, ChannelWriter>,
    serial: u32,
    granule_pos: u64,
    pts: usize,
    audio_buffer: Arc<AudioBuffer>,
}

impl Mixer {
    pub fn new(
        audio_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
        audio_buffer: Arc<AudioBuffer>,
    ) -> anyhow::Result<Self> {
        let mut encoder = Encoder::new(SAMPLE_RATE, Channels::Stereo, Application::LowDelay)?;
        encoder.set_bitrate(Bitrate::Bits(48_000))?;
        encoder.set_vbr(false)?;
        let pre_skip = encoder.get_lookahead()? as u16;

        let writer = ChannelWriter { audio_tx };
        let mut packet_writer = PacketWriter::new(writer);
        let serial = 0x5447_4F50;

        packet_writer.write_packet(opus_head(pre_skip), serial, PacketWriteEndInfo::EndPage, 0)?;
        packet_writer.write_packet(opus_tags(), serial, PacketWriteEndInfo::EndPage, 0)?;

        Ok(Self {
            encoder,
            packet_writer,
            serial,
            granule_pos: pre_skip as u64,
            pts: 0,
            audio_buffer,
        })
    }

    pub fn run(&mut self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let frame_size = FRAME_SIZE;
        let sample_rate = SAMPLE_RATE;
        let start_time = Instant::now();

        let mut read_buffer = vec![0.0f32; frame_size * CHANNELS];
        let mut packet_buffer = vec![0u8; 1500];

        // tracing::info!(frame_size, sample_rate, channels = CHANNELS, "mixer started");

        loop {
            if cancellation_token.is_cancelled() {
                return Ok(());
            }

            self.audio_buffer.pts.store(self.pts, Ordering::Release);

            let frames_read = self.audio_buffer.read(&mut read_buffer, frame_size);

            if frames_read > 0 && frames_read < frame_size {
                tracing::trace!(frames_read, frame_size, "audio buffer underrun");
            }

            let expected_time =
                start_time + Duration::from_secs_f64(self.pts as f64 / sample_rate as f64);
            let sleep_duration = expected_time.saturating_duration_since(Instant::now());
            if sleep_duration > Duration::ZERO {
                std::thread::sleep(sleep_duration);
            }

            self.pts += frame_size;
            let packet_len = self
                .encoder
                .encode_float(&read_buffer, &mut packet_buffer)
                .map_err(|e| anyhow::anyhow!("opus encode failed: {e:?}"))?;
            self.granule_pos += frame_size as u64;
            self.packet_writer.write_packet(
                packet_buffer[..packet_len].to_vec(),
                self.serial,
                PacketWriteEndInfo::EndPage,
                self.granule_pos,
            )?;
        }
    }
}

struct ChannelWriter {
    audio_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.audio_tx
            .blocking_send(buf.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "audio receiver dropped"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn opus_head(pre_skip: u16) -> Vec<u8> {
    let mut head = Vec::with_capacity(19);
    head.extend_from_slice(b"OpusHead");
    head.push(1);
    head.push(CHANNELS as u8);
    head.extend_from_slice(&pre_skip.to_le_bytes());
    head.extend_from_slice(&SAMPLE_RATE.to_le_bytes());
    head.extend_from_slice(&0i16.to_le_bytes());
    head.push(0);
    head
}

fn opus_tags() -> Vec<u8> {
    const VENDOR: &[u8] = b"terminal-games";
    let mut tags = Vec::with_capacity(16 + VENDOR.len());
    tags.extend_from_slice(b"OpusTags");
    tags.extend_from_slice(&(VENDOR.len() as u32).to_le_bytes());
    tags.extend_from_slice(VENDOR);
    tags.extend_from_slice(&0u32.to_le_bytes());
    tags
}
