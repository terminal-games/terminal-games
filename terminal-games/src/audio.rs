// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    ffi::CString,
    pin::Pin,
    ptr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::anyhow;
use ffmpeg_next as ffmpeg;
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
            inner.samples[..second_chunk * CHANNELS]
                .copy_from_slice(&samples[first_chunk * CHANNELS..first_chunk * CHANNELS + second_chunk * CHANNELS]);
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
    encoder: ffmpeg::encoder::Audio,
    fmt_ctx: *mut ffmpeg::ffi::AVFormatContext,
    _io: Pin<Box<OutputOpaque>>,
    frame: *mut ffmpeg::ffi::AVFrame,
    packet: *mut ffmpeg::ffi::AVPacket,
    output_stream: *mut ffmpeg::ffi::AVStream,
    pts: usize,
    audio_buffer: Arc<AudioBuffer>,
}

const ALIGN: libc::c_int = 0;

impl Mixer {
    pub fn new(
        audio_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
        audio_buffer: Arc<AudioBuffer>,
    ) -> anyhow::Result<Self> {
        ffmpeg::init().unwrap();
        let codec = ffmpeg::encoder::find(ffmpeg::codec::Id::OPUS)
            .ok_or(anyhow!("failed to find Opus encoder"))?;
        let mut encoder = ffmpeg::codec::Context::new_with_codec(codec)
            .encoder()
            .audio()?;
        encoder.set_channel_layout(ffmpeg::ChannelLayout::STEREO);
        encoder.set_time_base((1, 48000));
        encoder.set_format(ffmpeg::format::Sample::F32(
            ffmpeg::format::sample::Type::Packed,
        ));
        encoder.set_rate(48000);
        encoder.set_bit_rate(48000);
        encoder.set_flags(ffmpeg::codec::Flags::LOW_DELAY);

        let mut opts = ffmpeg::Dictionary::new();
        opts.set("application", "lowdelay");
        opts.set("frame_duration", "10");
        let encoder = encoder.open_with(opts)?;

        let (fmt_ctx, io) = unsafe {
            let buffer_size = 4096;
            let buffer = ffmpeg::ffi::av_malloc(buffer_size);
            if buffer.is_null() {
                anyhow::bail!("failed to allocate buffer");
            }

            let io = Box::pin(OutputOpaque {
                write: Box::new(move |buf| {
                    let len = buf.len() as i32;
                    // tracing::info!(capacity=audio_tx_clone.capacity(), len);
                    match audio_tx.blocking_send(buf.to_vec()) {
                        Ok(_) => len,
                        Err(tokio::sync::mpsc::error::SendError(_)) => {
                            ffmpeg::ffi::AVERROR(libc::EIO)
                        }
                    }
                }),
            });

            let write_flag = 1;
            let mut avio_ctx = ffmpeg::ffi::avio_alloc_context(
                buffer as *mut libc::c_uchar,
                buffer_size as i32,
                write_flag,
                io.as_ref().get_ref() as *const _ as *mut libc::c_void,
                None,
                Some(write_packet_wrapper),
                None,
            );
            if avio_ctx.is_null() {
                ffmpeg::ffi::av_free(buffer);
                anyhow::bail!("failed to allocate avio context");
            }

            let format_name = CString::new("ogg")?;
            let mut fmt_ctx = ptr::null_mut();
            match ffmpeg::ffi::avformat_alloc_output_context2(
                &mut fmt_ctx,
                ptr::null_mut(),
                format_name.as_ptr(),
                ptr::null(),
            ) {
                0 => {
                    (*fmt_ctx).pb = avio_ctx;
                    (*fmt_ctx).flags =
                        ffmpeg::ffi::AVFMT_FLAG_FLUSH_PACKETS | ffmpeg::ffi::AVFMT_FLAG_NOBUFFER;
                    Ok((fmt_ctx, io))
                }
                e => {
                    ffmpeg::ffi::avio_context_free(&mut avio_ctx);
                    Err(ffmpeg::Error::from(e))
                }
            }
        }?;

        let output_stream = unsafe { ffmpeg::ffi::avformat_new_stream(fmt_ctx, ptr::null()) };
        if output_stream.is_null() {
            anyhow::bail!("failed to create new output stream");
        }

        match unsafe {
            ffmpeg::ffi::avcodec_parameters_from_context(
                (*output_stream).codecpar,
                encoder.as_ptr(),
            )
        } {
            0.. => Ok(()),
            e => Err(ffmpeg::Error::from(e)),
        }?;
        unsafe { (*output_stream).time_base = encoder.time_base().into() };

        let mut opts: *mut ffmpeg::ffi::AVDictionary = ptr::null_mut();
        unsafe {
            ffmpeg::ffi::av_dict_set(
                &mut opts,
                b"page_duration\0".as_ptr() as *const _,
                b"10000\0".as_ptr() as *const _,
                0,
            );
            ffmpeg::ffi::avformat_write_header(fmt_ctx, &mut opts);
            ffmpeg::ffi::av_dict_free(&mut opts);
        }

        let packet = unsafe { ffmpeg::ffi::av_packet_alloc() };
        if packet.is_null() {
            anyhow::bail!("failed to allocate packet");
        }

        let frame = unsafe {
            let frame = ffmpeg::ffi::av_frame_alloc();
            if frame.is_null() {
                anyhow::bail!("failed to allocate frame");
            }
            (*frame).ch_layout = encoder.channel_layout().0;
            (*frame).nb_samples = encoder.frame_size() as i32;
            (*frame).format = ffmpeg::ffi::AVSampleFormat::from(encoder.format()) as i32;
            (*frame).sample_rate = encoder.rate() as i32;
            frame
        };

        match unsafe { ffmpeg::ffi::av_frame_get_buffer(frame, ALIGN) } {
            0.. => Ok(()),
            e => Err(ffmpeg::Error::from(e)),
        }?;

        match unsafe { ffmpeg::ffi::av_frame_make_writable(frame) } {
            0.. => Ok(()),
            e => Err(ffmpeg::Error::from(e)),
        }?;

        Ok(Self {
            encoder,
            fmt_ctx,
            _io: io,
            frame,
            packet,
            output_stream,
            pts: 0,
            audio_buffer,
        })
    }

    pub fn run(&mut self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let frame_size = self.encoder.frame_size() as usize;
        let sample_rate = self.encoder.rate();
        let start_time = Instant::now();

        let mut read_buffer = vec![0.0f32; frame_size * CHANNELS];

        tracing::info!(frame_size, sample_rate, channels = CHANNELS, "mixer started");

        loop {
            if cancellation_token.is_cancelled() {
                return Ok(());
            }

            self.audio_buffer.pts.store(self.pts, Ordering::Release);

            unsafe {
                (*self.frame).pts = self.pts as i64;

                let frames_read = self.audio_buffer.read(&mut read_buffer, frame_size);

                let data = (*self.frame).data[0] as *mut f32;
                std::ptr::copy_nonoverlapping(
                    read_buffer.as_ptr(),
                    data,
                    frame_size * CHANNELS,
                );

                if frames_read > 0 && frames_read < frame_size {
                    tracing::trace!(frames_read, frame_size, "audio buffer underrun");
                }
            }

            let expected_time =
                start_time + Duration::from_secs_f64(self.pts as f64 / sample_rate as f64);
            let sleep_duration = expected_time.saturating_duration_since(Instant::now());
            if sleep_duration > Duration::ZERO {
                std::thread::sleep(sleep_duration);
            }

            self.pts += frame_size;

            match unsafe { ffmpeg::ffi::avcodec_send_frame(self.encoder.as_mut_ptr(), self.frame) }
            {
                e if e < 0 => Err(ffmpeg::Error::from(e)),
                _ => Ok(()),
            }?;

            loop {
                match unsafe {
                    ffmpeg::ffi::avcodec_receive_packet(self.encoder.as_mut_ptr(), self.packet)
                } {
                    e if e < 0 => match ffmpeg::Error::from(e) {
                        ffmpeg::Error::Eof
                        | ffmpeg::Error::Other {
                            errno: libc::EAGAIN,
                        } => break,
                        error => Err(error)?,
                    },
                    _ => {}
                };

                unsafe { (*self.packet).stream_index = 0 };
                unsafe {
                    ffmpeg::ffi::av_packet_rescale_ts(
                        self.packet,
                        (*self.encoder.as_ptr()).time_base,
                        (*self.output_stream).time_base,
                    );
                };
                match unsafe { ffmpeg::ffi::av_write_frame(self.fmt_ctx, self.packet) } {
                    e if e < 0 => Err(ffmpeg::Error::from(e)),
                    _ => Ok(()),
                }?;
                unsafe { ffmpeg::ffi::av_packet_unref(self.packet) };
            }
        }
    }
}

impl Drop for Mixer {
    fn drop(&mut self) {
        if self.fmt_ctx.is_null() {
            return;
        }
        unsafe {
            ffmpeg::ffi::avcodec_send_frame(self.encoder.as_mut_ptr(), ptr::null());
            loop {
                let ret =
                    ffmpeg::ffi::avcodec_receive_packet(self.encoder.as_mut_ptr(), self.packet);
                if ret < 0 {
                    break;
                }
                ffmpeg::ffi::av_packet_unref(self.packet);
            }
            ffmpeg::ffi::av_frame_free(&mut self.frame);
            ffmpeg::ffi::av_packet_free(&mut self.packet);
            let mut pb = (*self.fmt_ctx).pb;
            let buffer = (*pb).buffer;
            ffmpeg::ffi::avformat_free_context(self.fmt_ctx);
            ffmpeg::ffi::avio_context_free(&mut pb);
            ffmpeg::ffi::av_free(buffer as *mut libc::c_void);
        }
        tracing::info!("dropped mixer");
    }
}

struct OutputOpaque {
    write: Box<dyn FnMut(&[u8]) -> i32 + Send>,
}

unsafe extern "C" fn write_packet_wrapper(
    opaque: *mut libc::c_void,
    buf: *const u8,
    buf_size: libc::c_int,
) -> libc::c_int {
    if buf.is_null() {
        return ffmpeg::ffi::AVERROR(ffmpeg::ffi::EIO);
    }
    let context = unsafe { &mut *(opaque as *mut OutputOpaque) };
    let slice = unsafe { std::slice::from_raw_parts(buf, buf_size as usize) };

    (*context.write)(slice)
}
