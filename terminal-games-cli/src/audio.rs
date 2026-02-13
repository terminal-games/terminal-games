// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    io::{self, Read, Seek, SeekFrom},
    sync::mpsc,
};

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

#[cfg(target_os = "linux")]
fn run_audio_player(receiver: mpsc::Receiver<Vec<u8>>) -> anyhow::Result<()> {
    use std::{ffi::CString, io::BufReader, os::unix::net::UnixStream};

    use pulseaudio::protocol::{self, stream::{BufferAttr, StreamFlags}};

    const BYTES_PER_MS: u32 = SAMPLE_RATE * CHANNELS as u32 * 4 / 1000;

    let socket_path = pulseaudio::socket_path_from_env()
        .ok_or_else(|| anyhow::anyhow!("PulseAudio/PipeWire socket not found"))?;
    let mut sock = BufReader::new(UnixStream::connect(socket_path)?);

    let cookie = pulseaudio::cookie_path_from_env()
        .and_then(|path| std::fs::read(path).ok())
        .unwrap_or_default();

    protocol::write_command_message(
        sock.get_mut(),
        0,
        &protocol::Command::Auth(protocol::AuthParams {
            version: protocol::MAX_VERSION,
            supports_shm: false,
            supports_memfd: false,
            cookie,
        }),
        protocol::MAX_VERSION,
    )?;

    let (_, auth_reply) =
        protocol::read_reply_message::<protocol::AuthReply>(&mut sock, protocol::MAX_VERSION)?;
    let proto_ver = std::cmp::min(protocol::MAX_VERSION, auth_reply.version);

    let mut props = protocol::Props::new();
    props.set(
        protocol::Prop::ApplicationName,
        CString::new("terminal-games").unwrap(),
    );
    protocol::write_command_message(
        sock.get_mut(),
        1,
        &protocol::Command::SetClientName(props),
        proto_ver,
    )?;
    let _ = protocol::read_reply_message::<protocol::SetClientNameReply>(&mut sock, proto_ver)?;

    protocol::write_command_message(
        sock.get_mut(),
        2,
        &protocol::Command::CreatePlaybackStream(protocol::PlaybackStreamParams {
            sample_spec: protocol::SampleSpec {
                format: protocol::SampleFormat::Float32Le,
                channels: CHANNELS as u8,
                sample_rate: SAMPLE_RATE,
            },
            channel_map: protocol::ChannelMap::stereo(),
            cvolume: Some(protocol::ChannelVolume::norm(CHANNELS as u8)),
            sink_name: Some(protocol::DEFAULT_SINK.to_owned()),
            buffer_attr: BufferAttr {
                max_length: u32::MAX,
                target_length: BYTES_PER_MS * 10,
                pre_buffering: 0,
                minimum_request_length: BYTES_PER_MS * 2,
                ..Default::default()
            },
            flags: StreamFlags {
                adjust_latency: true,
                early_requests: true,
                ..Default::default()
            },
            ..Default::default()
        }),
        proto_ver,
    )?;

    let (_, stream_info) = protocol::read_reply_message::<protocol::CreatePlaybackStreamReply>(
        &mut sock, proto_ver,
    )?;
    let channel = stream_info.channel;
    tracing::debug!(channel, "PulseAudio playback stream created");

    let mut dec = OggOpusDecoder::new(receiver);

    let initial = dec.read_pcm_bytes(stream_info.requested_bytes as usize)?;
    if !initial.is_empty() {
        protocol::write_memblock(sock.get_mut(), channel, &initial, 0)?;
    }

    const DRAIN_TAG: u32 = 100;
    let mut draining = false;

    loop {
        let (seq, msg) = protocol::read_command_message(&mut sock, proto_ver)?;
        match msg {
            protocol::Command::Started(_) => {
                tracing::debug!("PulseAudio playback started");
            }
            protocol::Command::Request(req) if req.channel == channel => {
                if draining {
                    continue;
                }
                let data = dec.read_pcm_bytes(req.length as usize)?;
                if !data.is_empty() {
                    protocol::write_memblock(sock.get_mut(), channel, &data, 0)?;
                } else {
                    protocol::write_command_message(
                        sock.get_mut(),
                        DRAIN_TAG,
                        &protocol::Command::DrainPlaybackStream(channel),
                        proto_ver,
                    )?;
                    draining = true;
                }
            }
            protocol::Command::Reply if seq == DRAIN_TAG => break,
            protocol::Command::Underflow(_) => {}
            protocol::Command::Overflow(_) => {}
            protocol::Command::Error(e) => anyhow::bail!("PulseAudio error: {e:?}"),
            _ => {}
        }
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn run_audio_player(receiver: mpsc::Receiver<Vec<u8>>) -> anyhow::Result<()> {
    use std::{collections::VecDeque, sync::{Arc, Mutex}};

    use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};

    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .ok_or_else(|| anyhow::anyhow!("No audio output device"))?;

    let config = cpal::StreamConfig {
        channels: CHANNELS as u16,
        sample_rate: SAMPLE_RATE,
        buffer_size: cpal::BufferSize::Default,
    };

    let samples: Arc<Mutex<VecDeque<f32>>> = Arc::new(Mutex::new(VecDeque::new()));
    let samples_cb = samples.clone();
    let stream = device.build_output_stream(
        &config,
        move |output: &mut [f32], _| {
            let mut buf = samples_cb.lock().unwrap();
            for sample in output.iter_mut() {
                *sample = buf.pop_front().unwrap_or(0.0);
            }
        },
        |e| tracing::error!(?e, "Audio stream error"),
        None,
    )?;

    let prebuffer_samples = SAMPLE_RATE as usize * CHANNELS / 50;
    let mut started = false;
    let samples_ref = samples.clone();

    decode_ogg_opus(receiver, |new_samples| {
        let mut buf = samples_ref.lock().unwrap();
        buf.extend(new_samples.iter().copied());

        const MAX_BUFFER_MS: usize = 40;
        let max = SAMPLE_RATE as usize * CHANNELS * MAX_BUFFER_MS / 1000;
        let excess = buf.len().saturating_sub(max);
        if excess > 0 {
            buf.drain(..excess);
        }

        if !started && buf.len() >= prebuffer_samples {
            stream.play()?;
            started = true;
        }
        Ok(())
    })
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

#[cfg(target_os = "linux")]
struct OggOpusDecoder {
    packet_reader: PacketReader<OggReader>,
    decoder: Option<Decoder>,
    pre_skip: usize,
    stream_serial: Option<u32>,
    frame_buf: Vec<f32>,
    pcm_bytes: Vec<u8>,
}

#[cfg(target_os = "linux")]
impl OggOpusDecoder {
    fn new(receiver: mpsc::Receiver<Vec<u8>>) -> Self {
        let reader = OggReader {
            receiver,
            buffer: Vec::new(),
            position: 0,
            eof: false,
        };
        Self {
            packet_reader: PacketReader::new(reader),
            decoder: None,
            pre_skip: 0,
            stream_serial: None,
            frame_buf: vec![0.0f32; 5760 * CHANNELS],
            pcm_bytes: Vec::new(),
        }
    }

    fn read_pcm_bytes(&mut self, max_bytes: usize) -> anyhow::Result<Vec<u8>> {
        while self.pcm_bytes.len() < max_bytes {
            if !self.decode_one_frame()? {
                break;
            }
        }
        let n = max_bytes.min(self.pcm_bytes.len());
        let out = self.pcm_bytes[..n].to_vec();
        self.pcm_bytes.drain(..n);
        Ok(out)
    }

    fn decode_one_frame(&mut self) -> anyhow::Result<bool> {
        loop {
            let packet = match self.packet_reader.read_packet() {
                Ok(Some(p)) => p,
                Ok(None) => return Ok(false),
                Err(e) => return Err(anyhow::anyhow!("Ogg read error: {e:?}")),
            };

            if self.stream_serial.is_none() {
                self.stream_serial = Some(packet.stream_serial());
            }
            if Some(packet.stream_serial()) != self.stream_serial {
                continue;
            }

            if packet.data.starts_with(b"OpusHead") {
                if packet.data.len() < 19 {
                    anyhow::bail!("invalid OpusHead");
                }
                if packet.data[9] != CHANNELS as u8 {
                    anyhow::bail!("unsupported channel count: {}", packet.data[9]);
                }
                self.pre_skip =
                    u16::from_le_bytes([packet.data[10], packet.data[11]]) as usize;
                self.decoder = Some(Decoder::new(SAMPLE_RATE, Channels::Stereo)?);
                continue;
            }
            if packet.data.starts_with(b"OpusTags") {
                continue;
            }

            let Some(dec) = self.decoder.as_mut() else {
                continue;
            };

            let frames = dec.decode_float(&packet.data, &mut self.frame_buf, false)?;
            let mut usable = frames * CHANNELS;
            let mut start = 0;
            if self.pre_skip > 0 {
                let skip = (self.pre_skip * CHANNELS).min(usable);
                start = skip;
                usable -= skip;
                self.pre_skip -= skip / CHANNELS;
            }

            if usable > 0 {
                for &s in &self.frame_buf[start..start + usable] {
                    self.pcm_bytes.extend_from_slice(&s.to_le_bytes());
                }
                return Ok(true);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn decode_ogg_opus(
    receiver: mpsc::Receiver<Vec<u8>>,
    mut emit: impl FnMut(&[f32]) -> anyhow::Result<()>,
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

        if usable > 0 {
            emit(&decoded[start..start + usable])?;
        }
    }

    Ok(())
}
