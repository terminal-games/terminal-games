// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    ffi::CString,
    ptr,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use ffmpeg_next as ffmpeg;

pub struct Mixer {
    encoder: ffmpeg::encoder::Audio,
    fmt_ctx: *mut ffmpeg::ffi::AVFormatContext,
    _io: Box<OutputOpaque>,
    frame: *mut ffmpeg::ffi::AVFrame,
    packet: *mut ffmpeg::ffi::AVPacket,
    output_stream: *mut ffmpeg::ffi::AVStream,
    pts: usize,
}

unsafe impl Send for Mixer{}

const ALIGN: libc::c_int = 0;

impl Mixer {
    pub fn new(audio_tx: tokio::sync::mpsc::Sender<Vec<u8>>) -> anyhow::Result<Self> {
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
        opts.set("frame_duration", "20");
        let encoder = encoder.open_with(opts)?;

        let (fmt_ctx, io) = unsafe {
            let buffer_size = 2048;
            let buffer = ffmpeg::ffi::av_malloc(buffer_size);
            if buffer.is_null() {
                anyhow::bail!("failed to allocate buffer");
            }

            let opaque = Box::into_raw(Box::new(OutputOpaque {
                write: Box::new(move |buf| {
                    let len = buf.len() as i32;
                    // tracing::info!(capacity=audio_tx.capacity());
                    match audio_tx.try_send(buf.to_vec()) {
                        Ok(_) => len,
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => ffmpeg::ffi::AVERROR(libc::EAGAIN),
                        // Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => 0,
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => ffmpeg::ffi::AVERROR(libc::EIO),
                    }
                }),
            }));

            let write_flag = 1;
            let mut avio_ctx = ffmpeg::ffi::avio_alloc_context(
                buffer as *mut libc::c_uchar,
                buffer_size as i32,
                write_flag,
                opaque as *mut libc::c_void,
                None,
                Some(write_packet_wrapper),
                None,
            );
            if avio_ctx.is_null() {
                ffmpeg::ffi::av_free(buffer);
                anyhow::bail!("failed to allocate avio context");
            }


            let io = Box::from_raw(opaque);

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
                    (*fmt_ctx).flags = ffmpeg::ffi::AVFMT_FLAG_FLUSH_PACKETS | ffmpeg::ffi::AVFMT_FLAG_NOBUFFER;
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

        match unsafe { ffmpeg::ffi::avcodec_parameters_from_context((*output_stream).codecpar, encoder.as_ptr()) } {
            0.. => Ok(()),
            e => Err(ffmpeg::Error::from(e)),
        }?;
        unsafe { (*output_stream).time_base = encoder.time_base().into() };

        let mut opts: *mut ffmpeg::ffi::AVDictionary = ptr::null_mut();
        unsafe {
            ffmpeg::ffi::av_dict_set(&mut opts, b"page_duration\0".as_ptr() as *const _, b"20000\0".as_ptr() as *const _, 0);
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

        Ok(Self { encoder, fmt_ctx, _io: io, frame, packet, output_stream, pts: 0, })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let num_samples = self.encoder.frame_size() as usize;
        let sample_rate = self.encoder.rate();
        const A4_HZ: f32 = 440.0;
        let amplitude = 0.5_f32;
        let start_time = Instant::now();
        let mut prev_semitone_steps: i32 = -1;
        tracing::info!(num_samples, sample_rate);

        loop {
            let elapsed = start_time.elapsed().as_secs_f32();
            let semitone_steps = (elapsed / 5.0).floor() as i32;
            let frequency = A4_HZ * 2f32.powf(semitone_steps as f32 / 12.0);

            unsafe {
                (*self.frame).pts = self.pts as i64;

                let data = (*self.frame).data[0] as *mut f32;
                for i in 0..num_samples {
                    let t = (self.pts + i) as f32 / sample_rate as f32;
                    let value =
                        amplitude * (2.0 * std::f32::consts::PI * frequency * t).sin();
                    *data.add(i*2) = value;
                    *data.add(i*2+1) = value;
                }
            }

            let expected_time =
                start_time + Duration::from_secs_f64(self.pts as f64 / sample_rate as f64);
            let sleep_duration = expected_time.saturating_duration_since(Instant::now());
            // tracing::info!(?sleep_duration);
            if sleep_duration > Duration::ZERO {
                tokio::time::sleep(sleep_duration).await;
            }

            if semitone_steps != prev_semitone_steps {
                tracing::info!(%frequency, semitone_steps, "frequency went up a semitone");
                prev_semitone_steps = semitone_steps;
            }

            self.pts += num_samples;

            match unsafe { ffmpeg::ffi::avcodec_send_frame(self.encoder.as_mut_ptr(), self.frame) } {
                e if e < 0 => Err(ffmpeg::Error::from(e)),
                _ => Ok(()),
            }?;

            loop {
                match unsafe { ffmpeg::ffi::avcodec_receive_packet(self.encoder.as_mut_ptr(), self.packet) } {
                    e if e < 0 => match ffmpeg::Error::from(e) {
                        ffmpeg::Error::Eof | ffmpeg::Error::Other { errno: libc::EAGAIN } => break,
                        error => Err(error)?,
                    },
                    _ => {},
                };

                unsafe { (*self.packet).stream_index = 0 };
                unsafe { ffmpeg::ffi::av_packet_rescale_ts(self.packet, (*self.encoder.as_ptr()).time_base, (*self.output_stream).time_base); };
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
            if !(*self.fmt_ctx).pb.is_null() {
                ffmpeg::ffi::avio_context_free(&mut (*self.fmt_ctx).pb);
            }
            ffmpeg::ffi::avformat_free_context(self.fmt_ctx);
            self.fmt_ctx = ptr::null_mut();
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
