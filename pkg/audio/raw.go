// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package audio

import (
	"io"
	"time"
)

type RawResource struct {
	samples []float32
}

var _ Resource = (*RawResource)(nil)

// NewResourceFromSamples creates a RawResource from interleaved stereo samples.
// Samples should be in the format: [L0, R0, L1, R1, ...].
func NewResourceFromSamples(samples []float32) *RawResource {
	return &RawResource{
		samples: samples,
	}
}

func (r *RawResource) Duration() time.Duration {
	frameCount := len(r.samples) / Channels
	return time.Duration(float64(frameCount) / float64(SampleRate) * float64(time.Second))
}

func (r *RawResource) SampleCount() int {
	return len(r.samples) / Channels
}

func (r *RawResource) NewInstance() *Instance {
	dec := newRawDecoder(r)
	return newInstance(dec)
}

type rawDecoder struct {
	resource *RawResource
	position int
}

var _ decoder = (*rawDecoder)(nil)

func newRawDecoder(r *RawResource) *rawDecoder {
	return &rawDecoder{
		resource: r,
		position: 0,
	}
}

func (d *rawDecoder) Read(buffer []float32) (int, error) {
	samples := d.resource.samples
	totalFrames := len(samples) / Channels
	remainingFrames := totalFrames - d.position

	if remainingFrames <= 0 {
		return 0, io.EOF
	}

	framesToRead := min(len(buffer)/Channels, remainingFrames)
	valuesToRead := framesToRead * Channels

	srcOffset := d.position * Channels
	copy(buffer[:valuesToRead], samples[srcOffset:srcOffset+valuesToRead])
	d.position += framesToRead

	return valuesToRead, nil
}

func (d *rawDecoder) Seek(position int) {
	totalFrames := len(d.resource.samples) / Channels
	if position < 0 {
		position = 0
	}
	if position > totalFrames {
		position = totalFrames
	}
	d.position = position
}

func (d *rawDecoder) Position() int {
	return d.position
}

func (d *rawDecoder) Length() int {
	return len(d.resource.samples) / Channels
}
