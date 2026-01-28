// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package audio

import (
	"time"
)

func init() {
	startMixer()
}

const (
	SampleRate = 48000
	FrameSize  = 480
	Channels   = 2
)

// Resource represents audio data that can be instantiated for playback.
// A single Resource can have multiple Instances playing simultaneously.
type Resource interface {
	// Duration returns the total duration of the audio.
	Duration() time.Duration

	// SampleCount returns the total number of samples.
	SampleCount() int

	// NewInstance creates a new playable Instance of this Resource.
	// The instance starts paused at position 0 with volume 1.0.
	NewInstance() *Instance
}

type decoder interface {
	// Read fills the buffer with interleaved stereo samples and returns the
	// number of f32 values read (frames * Channels).
	// Returns io.EOF when reaching the end of the audio.
	Read(buffer []float32) (int, error)

	// Seek sets the playback position in frames.
	Seek(position int)

	// Position returns the current playback position in frames.
	Position() int

	// Length returns the total number of frames.
	Length() int
}
