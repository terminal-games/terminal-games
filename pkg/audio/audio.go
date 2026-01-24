// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package audio provides audio support for terminal-games SDK.
//
// This package allows WASM apps to produce audio that gets mixed and streamed
// to connected clients.
//
// # Audio Model
//
// The host runs an audio mixer at 48kHz stereo. WASM apps write samples to a
// ring buffer, and the mixer consumes them. If the buffer is empty, silence
// is output. If the buffer is full, samples are dropped.
//
// # Timing
//
// Use Info() to get the current playback position (PTS) and buffer state.
// Apps should generate samples ahead of the current PTS to avoid underruns.
//
// # Example
//
//	info, _ := audio.Info()
//	samples := make([]float32, info.FrameSize*2) // stereo
//
//	// Generate a sine wave
//	frequency := float32(440.0)
//	amplitude := float32(0.3)
//	for i := uint32(0); i < info.FrameSize; i++ {
//	    t := float32(info.PTS+uint64(i)) / float32(info.SampleRate)
//	    value := amplitude * float32(math.Sin(float64(2*math.Pi*frequency*t)))
//	    samples[i*2] = value     // left
//	    samples[i*2+1] = value   // right
//	}
//
//	written := audio.Write(samples)
package audio

import (
	"errors"
	"unsafe"
)

const (
	SampleRate = 48000
	FrameSize  = 960
	Channels   = 2
)

//go:wasmimport terminal_games audio_write
//go:noescape
func audio_write(ptr unsafe.Pointer, sampleCount uint32) int32

//go:wasmimport terminal_games audio_info
//go:noescape
func audio_info(
	frameSizePtr unsafe.Pointer,
	sampleRatePtr unsafe.Pointer,
	ptsPtr unsafe.Pointer,
	bufferAvailablePtr unsafe.Pointer,
) int32

type AudioInfo struct {
	FrameSize       uint32
	SampleRate      uint32
	PTS             uint64
	BufferAvailable uint32
}

var ErrAudioInfoFailed = errors.New("audio_info host call failed")

// Info retrieves audio timing information for synchronization.
//
// Returns information about the audio system including the current playback
// position (PTS), which apps can use to generate correctly timed samples.
func Info() (AudioInfo, error) {
	var frameSize uint32
	var sampleRate uint32
	var pts uint64
	var bufferAvailable uint32

	ret := audio_info(
		unsafe.Pointer(&frameSize),
		unsafe.Pointer(&sampleRate),
		unsafe.Pointer(&pts),
		unsafe.Pointer(&bufferAvailable),
	)

	if ret < 0 {
		return AudioInfo{}, ErrAudioInfoFailed
	}

	return AudioInfo{
		FrameSize:       frameSize,
		SampleRate:      sampleRate,
		PTS:             pts,
		BufferAvailable: bufferAvailable,
	}, nil
}

// Write writes stereo audio samples to the mixer buffer.
//
// Samples should be interleaved stereo float32 values: [L0, R0, L1, R1, ...].
// Values should be in the range [-1.0, 1.0].
//
// This function is non-blocking. If the buffer is full, samples may be
// dropped. Check the return value to see how many sample pairs were actually
// written.
//
// Returns the number of stereo sample pairs written, or a negative value on error.
func Write(samples []float32) int {
	if len(samples) == 0 {
		return 0
	}

	samplePairs := len(samples) / Channels
	if samplePairs == 0 {
		return 0
	}

	ret := audio_write(unsafe.Pointer(&samples[0]), uint32(samplePairs))
	return int(ret)
}

// AudioWriter is a helper for continuously writing audio with automatic
// timing management.
type AudioWriter struct {
	NextPTS      uint64
	TargetBuffer uint32
}

// NewAudioWriter creates a new AudioWriter.
//
// targetBuffer specifies how many samples to try to keep buffered. A good
// default is 2-3 frames (1920-2880 samples) for low latency with some
// buffer against underruns.
func NewAudioWriter(targetBuffer uint32) *AudioWriter {
	return &AudioWriter{
		NextPTS:      0,
		TargetBuffer: targetBuffer,
	}
}

// ShouldWrite checks if more audio should be written based on buffer state.
//
// Returns the number of samples that should be written, or 0 if the buffer
// is sufficiently full. This is useful for rate-limiting audio generation.
func (w *AudioWriter) ShouldWrite() int {
	info, err := Info()
	if err != nil {
		return 0
	}

	if w.NextPTS < info.PTS {
		w.NextPTS = info.PTS
	}

	if info.BufferAvailable >= w.TargetBuffer {
		return 0
	}

	needed := int(w.TargetBuffer - info.BufferAvailable)
	frames := (needed + FrameSize - 1) / FrameSize
	return frames * FrameSize
}

// WriteCallback calls the provided function to generate samples when needed.
//
// The callback receives the PTS and number of samples to generate, and should
// return interleaved stereo samples.
//
// This method updates NextPTS automatically based on how many samples were
// written.
func (w *AudioWriter) WriteCallback(generate func(pts uint64, numSamples int) []float32) int {
	needed := w.ShouldWrite()
	if needed == 0 {
		return 0
	}

	samples := generate(w.NextPTS, needed)
	written := Write(samples)
	if written > 0 {
		w.NextPTS += uint64(written)
	}
	return written
}
