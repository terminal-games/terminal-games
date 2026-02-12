// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::VecDeque,
    io::{self, Read, Seek, SeekFrom},
    sync::{Arc, Mutex, mpsc},
};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ogg::reading::PacketReader;
use opus::{Channels, Decoder};

const SAMPLE_RATE: u32 = 48000;
const CHANNELS: usize = 2;

pub struct AudioPlayerHandle {
    sender: mpsc::Sender<Vec<u8>>,
}

impl AudioPlayerHandle {
    pub fn push_audio(&self, data: Vec<u8>) {
        let _ = self.sender.send(data);
    }
}

pub fn spawn_audio_player() -> anyhow::Result<AudioPlayerHandle> {
    let (sender, receiver) = mpsc::channel::<Vec<u8>>();

    std::thread::spawn(move || {
        if let Err(e) = run_audio_player(receiver) {
            tracing::error!(?e, "Audio player error");
        }
    });

    Ok(AudioPlayerHandle { sender })
}

fn run_audio_player(receiver: mpsc::Receiver<Vec<u8>>) -> anyhow::Result<()> {
    let samples: Arc<Mutex<VecDeque<f32>>> = Arc::new(Mutex::new(VecDeque::new()));

    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .ok_or_else(|| anyhow::anyhow!("No audio output device"))?;

    let config = cpal::StreamConfig {
        channels: CHANNELS as u16,
        sample_rate: SAMPLE_RATE,
        buffer_size: cpal::BufferSize::Default,
    };

    let samples_clone = samples.clone();
    let stream = device.build_output_stream(
        &config,
        move |output: &mut [f32], _| {
            let mut buf = samples_clone.lock().unwrap();
            for sample in output.iter_mut() {
                *sample = buf.pop_front().unwrap_or(0.0);
            }
        },
        |e| tracing::error!(?e, "Audio stream error"),
        None,
    )?;

    decode_ogg_opus(receiver, samples, stream)
}

struct OggReader {
    receiver: mpsc::Receiver<Vec<u8>>,
    buffer: Vec<u8>,
    position: usize,
    eof: bool,
}

impl OggReader {
    fn fill_until(&mut self, offset: usize) {
        while self.buffer.len() < offset && !self.eof {
            match self.receiver.recv() {
                Ok(chunk) => self.buffer.extend_from_slice(&chunk),
                Err(_) => self.eof = true,
            }
        }
    }
}

impl Read for OggReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        self.fill_until(self.position + 1);
        if self.position >= self.buffer.len() {
            return Ok(0);
        }

        let available = self.buffer.len() - self.position;
        let count = available.min(buf.len());
        buf[..count].copy_from_slice(&self.buffer[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

impl Seek for OggReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let target = match pos {
            SeekFrom::Start(v) => v as i64,
            SeekFrom::Current(v) => self.position as i64 + v,
            SeekFrom::End(v) => self.buffer.len() as i64 + v,
        };
        if target < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot seek before start",
            ));
        }

        let target = target as usize;
        self.fill_until(target);
        if target > self.buffer.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "seek past end"));
        }

        self.position = target;
        Ok(self.position as u64)
    }
}

fn decode_ogg_opus(
    receiver: mpsc::Receiver<Vec<u8>>,
    samples: Arc<Mutex<VecDeque<f32>>>,
    stream: cpal::Stream,
) -> anyhow::Result<()> {
    let reader = OggReader {
        receiver,
        buffer: Vec::new(),
        position: 0,
        eof: false,
    };

    let mut packet_reader = PacketReader::new(reader);
    let mut decoder: Option<Decoder> = None;
    let mut pre_skip = 0usize;
    let mut stream_serial: Option<u32> = None;

    let prebuffer_samples = SAMPLE_RATE as usize * CHANNELS / 50;
    let mut started = false;
    let mut decoded = vec![0.0f32; 5760 * CHANNELS];

    loop {
        let packet = match packet_reader.read_packet() {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
            Err(e) => return Err(anyhow::anyhow!("failed to read Ogg packet: {e:?}")),
        };

        if stream_serial.is_none() {
            stream_serial = Some(packet.stream_serial());
        }
        if Some(packet.stream_serial()) != stream_serial {
            continue;
        }

        if packet.data.starts_with(b"OpusHead") {
            if packet.data.len() < 19 {
                anyhow::bail!("invalid OpusHead");
            }
            if packet.data[9] != CHANNELS as u8 {
                anyhow::bail!("unsupported channel count: {}", packet.data[9]);
            }
            pre_skip = u16::from_le_bytes([packet.data[10], packet.data[11]]) as usize;
            decoder = Some(Decoder::new(SAMPLE_RATE, Channels::Stereo)?);
            continue;
        }

        if packet.data.starts_with(b"OpusTags") {
            continue;
        }

        let Some(decoder) = decoder.as_mut() else {
            continue;
        };

        let frame_count = decoder.decode_float(&packet.data, &mut decoded, false)?;
        let mut usable = frame_count * CHANNELS;
        let mut start = 0usize;
        if pre_skip > 0 {
            let skip = (pre_skip * CHANNELS).min(usable);
            start = skip;
            usable -= skip;
            pre_skip -= skip / CHANNELS;
        }

        if usable == 0 {
            continue;
        }

        {
            let mut buf = samples.lock().unwrap();
            buf.extend(decoded[start..start + usable].iter().copied());

            const MAX_BUFFER_MS: usize = 40;
            let max_buffer_samples = SAMPLE_RATE as usize * CHANNELS * MAX_BUFFER_MS / 1000;
            let excess = buf.len().saturating_sub(max_buffer_samples);
            if excess > 0 {
                buf.drain(..excess);
            }

            if !started && buf.len() >= prebuffer_samples {
                stream.play()?;
                tracing::debug!("Audio started with {} samples buffered", buf.len());
                started = true;
            }
        }
    }

    Ok(())
}
