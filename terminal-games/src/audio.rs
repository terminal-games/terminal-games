// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    io::{self, Write},
    time::{Duration, Instant},
};

use ogg::writing::{PacketWriteEndInfo, PacketWriter};
use opus::{Application, Bitrate, Channels, Encoder};
pub const SAMPLE_RATE: u32 = 48000;
pub const FRAME_SIZE: usize = 480;
pub const CHANNELS: usize = 2;
const OUTPUT_BUFFER_CAPACITY: usize = 16 * 1024;

struct AudioBufferInner {
    samples: Vec<f32>,
    write_pos: usize,
    read_pos: usize,
}

pub struct Mixer {
    encoder: Encoder,
    packet_writer: PacketWriter<'static, ChannelWriter>,
    audio_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    ring: AudioBufferInner,
    capacity: usize,
    serial: u32,
    granule_pos: u64,
    pts: usize,
    next_tick_at: Instant,
    read_buffer: Vec<f32>,
    packet_buffer: Vec<u8>,
}

impl Mixer {
    pub fn new(audio_tx: tokio::sync::mpsc::Sender<Vec<u8>>) -> anyhow::Result<Self> {
        let mut encoder = Encoder::new(SAMPLE_RATE, Channels::Stereo, Application::LowDelay)?;
        encoder.set_bitrate(Bitrate::Bits(48_000))?;
        encoder.set_vbr(false)?;
        let pre_skip = encoder.get_lookahead()? as u16;

        let writer = ChannelWriter {
            output_buffer: Vec::with_capacity(OUTPUT_BUFFER_CAPACITY),
        };
        let packet_writer = PacketWriter::new(writer);
        let serial = 0x5447_4F50;

        let mut mixer = Self {
            encoder,
            packet_writer,
            audio_tx,
            ring: AudioBufferInner {
                samples: vec![0.0; SAMPLE_RATE as usize * CHANNELS],
                write_pos: 0,
                read_pos: 0,
            },
            capacity: SAMPLE_RATE as usize,
            serial,
            granule_pos: pre_skip as u64,
            pts: 0,
            next_tick_at: Instant::now(),
            read_buffer: vec![0.0f32; FRAME_SIZE * CHANNELS],
            packet_buffer: vec![0u8; 1500],
        };

        mixer.packet_writer.write_packet(
            opus_head(pre_skip),
            mixer.serial,
            PacketWriteEndInfo::EndPage,
            0,
        )?;
        let _ = mixer.flush_output();
        mixer.packet_writer.write_packet(
            opus_tags(),
            mixer.serial,
            PacketWriteEndInfo::EndPage,
            0,
        )?;
        let _ = mixer.flush_output();

        Ok(mixer)
    }

    pub fn write(&mut self, samples: &[f32]) -> usize {
        let written = self.write_ring(samples);
        self.tick_due();
        written
    }

    pub fn info(&mut self) -> (usize, usize) {
        self.tick_due();
        (self.pts, self.available())
    }

    fn write_ring(&mut self, samples: &[f32]) -> usize {
        let frame_count = samples.len() / CHANNELS;
        if frame_count == 0 {
            return 0;
        }
        let used = if self.ring.write_pos >= self.ring.read_pos {
            self.ring.write_pos - self.ring.read_pos
        } else {
            self.capacity - self.ring.read_pos + self.ring.write_pos
        };
        let available = self.capacity.saturating_sub(used + 1);
        let to_write = frame_count.min(available);
        if to_write == 0 {
            return 0;
        }

        let frames_until_wrap = self.capacity - self.ring.write_pos;
        let first_chunk = to_write.min(frames_until_wrap);
        let second_chunk = to_write - first_chunk;

        let dest_start = self.ring.write_pos * CHANNELS;
        self.ring.samples[dest_start..dest_start + first_chunk * CHANNELS]
            .copy_from_slice(&samples[..first_chunk * CHANNELS]);
        if second_chunk > 0 {
            self.ring.samples[..second_chunk * CHANNELS].copy_from_slice(
                &samples[first_chunk * CHANNELS..first_chunk * CHANNELS + second_chunk * CHANNELS],
            );
        }
        self.ring.write_pos = (self.ring.write_pos + to_write) % self.capacity;
        to_write
    }

    fn available(&self) -> usize {
        if self.ring.write_pos >= self.ring.read_pos {
            self.ring.write_pos - self.ring.read_pos
        } else {
            self.capacity - self.ring.read_pos + self.ring.write_pos
        }
    }

    fn read_ring(&mut self, dest: &mut [f32], frame_count: usize) -> usize {
        let to_read = frame_count.min(self.available());
        if to_read > 0 {
            let frames_until_wrap = self.capacity - self.ring.read_pos;
            let first_chunk = to_read.min(frames_until_wrap);
            let second_chunk = to_read - first_chunk;

            let src_start = self.ring.read_pos * CHANNELS;
            dest[..first_chunk * CHANNELS]
                .copy_from_slice(&self.ring.samples[src_start..src_start + first_chunk * CHANNELS]);
            if second_chunk > 0 {
                let dest_start = first_chunk * CHANNELS;
                dest[dest_start..dest_start + second_chunk * CHANNELS]
                    .copy_from_slice(&self.ring.samples[..second_chunk * CHANNELS]);
            }
            self.ring.read_pos = (self.ring.read_pos + to_read) % self.capacity;
        }
        if to_read < frame_count {
            let silence_start = to_read * CHANNELS;
            let silence_end = frame_count * CHANNELS;
            dest[silence_start..silence_end].fill(0.0);
        }
        to_read
    }

    fn tick_due(&mut self) {
        let frame_duration = Duration::from_secs_f64(FRAME_SIZE as f64 / SAMPLE_RATE as f64);
        let now = Instant::now();
        if self.next_tick_at > now {
            return;
        }
        let mut ticks = 0usize;
        while self.next_tick_at <= Instant::now() {
            if let Err(error) = self.tick_once() {
                tracing::error!(?error, "Mixer tick failed");
                break;
            }
            self.next_tick_at += frame_duration;
            ticks += 1;
            if ticks >= 8 {
                self.next_tick_at = Instant::now() + frame_duration;
                break;
            }
        }
    }

    fn tick_once(&mut self) -> anyhow::Result<()> {
        if !self.flush_output() {
            return Ok(());
        }

        let mut read_buffer = std::mem::take(&mut self.read_buffer);
        let frames_read = self.read_ring(&mut read_buffer, FRAME_SIZE);
        if frames_read > 0 && frames_read < FRAME_SIZE {
            tracing::trace!(frames_read, FRAME_SIZE, "audio buffer underrun");
        }

        self.pts += FRAME_SIZE;
        let packet_len_result = self
            .encoder
            .encode_float(&read_buffer, &mut self.packet_buffer);
        self.read_buffer = read_buffer;
        let packet_len =
            packet_len_result.map_err(|e| anyhow::anyhow!("opus encode failed: {e:?}"))?;
        self.granule_pos += FRAME_SIZE as u64;
        self.packet_writer.write_packet(
            self.packet_buffer[..packet_len].to_vec(),
            self.serial,
            PacketWriteEndInfo::EndPage,
            self.granule_pos,
        )?;
        let _ = self.flush_output();
        Ok(())
    }

    fn flush_output(&mut self) -> bool {
        let output_buffer = &mut self.packet_writer.inner_mut().output_buffer;
        if output_buffer.is_empty() {
            return true;
        }
        let chunk = std::mem::replace(output_buffer, Vec::with_capacity(OUTPUT_BUFFER_CAPACITY));
        if chunk.is_empty() {
            return true;
        }
        match self.audio_tx.try_send(chunk) {
            Err(tokio::sync::mpsc::error::TrySendError::Full(chunk)) => {
                self.packet_writer.inner_mut().output_buffer = chunk;
                false
            }
            Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => true,
        }
    }
}

struct ChannelWriter {
    output_buffer: Vec<u8>,
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.output_buffer.extend_from_slice(buf);
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
