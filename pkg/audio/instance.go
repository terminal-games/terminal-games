// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package audio

import (
	"io"
	"sync"
	"time"
)

// Instance represents a playing instance of a Resource.
type Instance struct {
	dec     decoder
	volume  float32
	playing bool
	loop    bool
	mu      sync.Mutex
}

func newInstance(dec decoder) *Instance {
	inst := &Instance{
		dec:     dec,
		volume:  1.0,
		playing: false,
		loop:    false,
	}
	globalMixer.addInstance(inst)
	return inst
}

// Play starts or resumes playback.
func (i *Instance) Play() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.playing = true
}

// Pause pauses playback without resetting position.
func (i *Instance) Pause() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.playing = false
}

// Stop pauses playback and resets position to the beginning.
func (i *Instance) Stop() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.playing = false
	i.dec.Seek(0)
}

// SetVolume sets the volume multiplier.
// 0.0 = silent, 1.0 = full volume. Values > 1.0 are allowed for amplification.
func (i *Instance) SetVolume(v float32) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.volume = v
}

// Volume returns the current volume.
func (i *Instance) Volume() float32 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.volume
}

// SetLoop sets whether the instance should loop when reaching the end.
func (i *Instance) SetLoop(loop bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.loop = loop
}

// Loop returns whether looping is enabled.
func (i *Instance) Loop() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.loop
}

// IsPlaying returns true if the instance is currently playing.
func (i *Instance) IsPlaying() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.playing
}

// Position returns the current playback position in samples.
func (i *Instance) Position() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.dec.Position()
}

// Seek sets the playback position in samples.
func (i *Instance) Seek(position int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.dec.Seek(position)
}

// SeekDuration sets the playback position as a duration from the start.
func (i *Instance) SeekDuration(d time.Duration) {
	samples := int(float64(d) / float64(time.Second) * float64(SampleRate))
	i.Seek(samples)
}

// CurrentTime returns the current playback position as a duration.
func (i *Instance) CurrentTime() time.Duration {
	i.mu.Lock()
	defer i.mu.Unlock()
	return time.Duration(float64(i.dec.Position()) / float64(SampleRate) * float64(time.Second))
}

// Duration returns the total duration of the audio.
func (i *Instance) Duration() time.Duration {
	return time.Duration(float64(i.dec.Length()) / float64(SampleRate) * float64(time.Second))
}

// Destroy removes this instance from the mixer.
// After calling Destroy, the instance should not be used.
func (i *Instance) Destroy() {
	globalMixer.removeInstance(i)
}

func (i *Instance) fillBuffer(buffer []float32) int {
	i.mu.Lock()
	defer i.mu.Unlock()

	if !i.playing {
		return 0
	}

	written := 0
	for written < len(buffer) {
		n, err := i.dec.Read(buffer[written:])
		if n > 0 {
			for j := range n {
				buffer[written+j] *= i.volume
			}
			written += n
		}

		if err == io.EOF {
			if i.loop {
				i.dec.Seek(0)
			} else {
				i.playing = false
				break
			}
		} else if err != nil {
			i.playing = false
			break
		}

		if n == 0 && err == nil {
			break
		}
	}

	return written
}
