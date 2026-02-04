// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::VecDeque,
    ffi::CString,
    ptr,
    sync::{mpsc, Arc, Mutex},
};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ffmpeg_next as ffmpeg;

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
    ffmpeg::init()?;
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
}

unsafe extern "C" fn read_ogg(opaque: *mut libc::c_void, buf: *mut u8, size: i32) -> i32 {
    let reader = unsafe { &mut *(opaque as *mut OggReader) };

    if !reader.buffer.is_empty() {
        let n = (size as usize).min(reader.buffer.len());
        unsafe { ptr::copy_nonoverlapping(reader.buffer.as_ptr(), buf, n) };
        reader.buffer.drain(..n);
        return n as i32;
    }

    match reader.receiver.recv() {
        Ok(chunk) => {
            let n = (size as usize).min(chunk.len());
            unsafe { ptr::copy_nonoverlapping(chunk.as_ptr(), buf, n) };
            reader.buffer.extend_from_slice(&chunk[n..]);
            n as i32
        }
        Err(_) => ffmpeg::ffi::AVERROR_EOF,
    }
}

fn decode_ogg_opus(
    receiver: mpsc::Receiver<Vec<u8>>,
    samples: Arc<Mutex<VecDeque<f32>>>,
    stream: cpal::Stream,
) -> anyhow::Result<()> {
    let reader = Box::new(OggReader { receiver, buffer: Vec::new() });
    let reader_ptr = Box::into_raw(reader);

    let buffer_size = 4096;
    let avio_buffer = unsafe { ffmpeg::ffi::av_malloc(buffer_size) as *mut u8 };
    let avio = unsafe {
        ffmpeg::ffi::avio_alloc_context(
            avio_buffer,
            buffer_size as i32,
            0,
            reader_ptr as *mut libc::c_void,
            Some(read_ogg),
            None,
            None,
        )
    };

    let mut fmt_ctx = unsafe { ffmpeg::ffi::avformat_alloc_context() };
    unsafe {
        (*fmt_ctx).pb = avio;
        (*fmt_ctx).probesize = 32;
        (*fmt_ctx).max_analyze_duration = 0;
    }

    let ogg = CString::new("ogg")?;
    let input_fmt = unsafe { ffmpeg::ffi::av_find_input_format(ogg.as_ptr()) };
    if unsafe { ffmpeg::ffi::avformat_open_input(&mut fmt_ctx, ptr::null(), input_fmt, ptr::null_mut()) } < 0 {
        anyhow::bail!("Failed to open OGG input");
    }

    let decoder = unsafe { ffmpeg::ffi::avcodec_find_decoder(ffmpeg::ffi::AVCodecID::AV_CODEC_ID_OPUS) };
    if decoder.is_null() {
        anyhow::bail!("Opus decoder not found");
    }

    let codec_ctx = unsafe { ffmpeg::ffi::avcodec_alloc_context3(decoder) };
    unsafe {
        (*codec_ctx).sample_rate = SAMPLE_RATE as i32;
        (*codec_ctx).ch_layout.nb_channels = CHANNELS as i32;

        if (*fmt_ctx).nb_streams > 0 {
            let stream = *(*fmt_ctx).streams;
            (*codec_ctx).pkt_timebase = (*stream).time_base;
            let params = (*stream).codecpar;
            if !(*params).extradata.is_null() {
                let size = (*params).extradata_size as usize;
                (*codec_ctx).extradata = ffmpeg::ffi::av_mallocz(size + 32) as *mut u8;
                (*codec_ctx).extradata_size = size as i32;
                ptr::copy_nonoverlapping((*params).extradata, (*codec_ctx).extradata, size);
            }
        }
    }

    if unsafe { ffmpeg::ffi::avcodec_open2(codec_ctx, decoder, ptr::null_mut()) } < 0 {
        anyhow::bail!("Failed to open Opus decoder");
    }

    let packet = unsafe { ffmpeg::ffi::av_packet_alloc() };
    let frame = unsafe { ffmpeg::ffi::av_frame_alloc() };

    let prebuffer_samples = SAMPLE_RATE as usize * CHANNELS / 50;
    let mut started = false;

    loop {
        let ret = unsafe { ffmpeg::ffi::av_read_frame(fmt_ctx, packet) };
        if ret < 0 {
            break;
        }

        if unsafe { ffmpeg::ffi::avcodec_send_packet(codec_ctx, packet) } < 0 {
            unsafe { ffmpeg::ffi::av_packet_unref(packet) };
            continue;
        }
        unsafe { ffmpeg::ffi::av_packet_unref(packet) };

        while unsafe { ffmpeg::ffi::avcodec_receive_frame(codec_ctx, frame) } >= 0 {
            let nb = unsafe { (*frame).nb_samples } as usize;
            let ch = unsafe { (*frame).ch_layout.nb_channels } as usize;
            let fmt = unsafe { (*frame).format };

            let decoded: Vec<f32> = unsafe {
                if fmt == ffmpeg::ffi::AVSampleFormat::AV_SAMPLE_FMT_FLTP as i32 {
                    let mut out = Vec::with_capacity(nb * ch);
                    for i in 0..nb {
                        for c in 0..ch {
                            let ptr = (*frame).data[c] as *const f32;
                            out.push(*ptr.add(i));
                        }
                    }
                    out
                } else if fmt == ffmpeg::ffi::AVSampleFormat::AV_SAMPLE_FMT_FLT as i32 {
                    let ptr = (*frame).data[0] as *const f32;
                    std::slice::from_raw_parts(ptr, nb * ch).to_vec()
                } else {
                    continue;
                }
            };

            {
                let mut buf = samples.lock().unwrap();
                buf.extend(decoded);

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
            unsafe { ffmpeg::ffi::av_frame_unref(frame) };
        }
    }

    unsafe {
        ffmpeg::ffi::av_frame_free(&mut (frame as *mut _));
        ffmpeg::ffi::av_packet_free(&mut (packet as *mut _));
        ffmpeg::ffi::avcodec_free_context(&mut (codec_ctx as *mut _));
        ffmpeg::ffi::avformat_close_input(&mut fmt_ctx);
        drop(Box::from_raw(reader_ptr));
    }

    Ok(())
}
