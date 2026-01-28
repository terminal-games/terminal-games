// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package audio

import (
	"errors"
	"unsafe"
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

// AudioInfo contains information about the audio system state.
type AudioInfo struct {
	FrameSize       uint32
	SampleRate      uint32
	PTS             uint64
	BufferAvailable uint32
}

var ErrAudioInfoFailed = errors.New("audio_info host call failed")

// Info retrieves audio timing information for synchronization.
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

// Write writes interleaved stereo audio samples to the mixer buffer.
//
// Samples should be interleaved stereo float32 values: [L0, R0, L1, R1, ...].
// Values should be in the range [-1.0, 1.0].
//
// This function is non-blocking. If the buffer is full, samples may be
// dropped. Check the return value to see how many frames were actually
// written.
//
// Returns the number of frames written, or a negative value on error.
func Write(samples []float32) int {
	if len(samples) == 0 {
		return 0
	}

	frameCount := len(samples) / Channels
	if frameCount == 0 {
		return 0
	}

	ret := audio_write(unsafe.Pointer(&samples[0]), uint32(frameCount))
	return int(ret)
}

// AudioWriter is a helper for continuously writing audio with automatic
// timing management.
type AudioWriter struct {
	NextPTS      uint64
	TargetBuffer uint32
}

// NewAudioWriter creates a new AudioWriter.
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
// The callback receives the PTS and number of frames to generate, and should
// return interleaved stereo float32 samples: [L0, R0, L1, R1, ...].
//
// This method updates NextPTS automatically based on how many frames were
// written.
func (w *AudioWriter) WriteCallback(generate func(pts uint64, numFrames int) []float32) int {
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
