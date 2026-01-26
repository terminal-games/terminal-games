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

func NewResourceFromSamples(samples []float32) *RawResource {
	return &RawResource{
		samples: samples,
	}
}

func (r *RawResource) Duration() time.Duration {
	return time.Duration(float64(len(r.samples)) / float64(SampleRate) * float64(time.Second))
}

func (r *RawResource) SampleCount() int {
	return len(r.samples)
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
	remaining := len(samples) - d.position

	if remaining <= 0 {
		return 0, io.EOF
	}

	toRead := min(len(buffer), remaining)

	copy(buffer[:toRead], samples[d.position:d.position+toRead])
	d.position += toRead

	return toRead, nil
}

func (d *rawDecoder) Seek(position int) {
	if position < 0 {
		position = 0
	}
	if position > len(d.resource.samples) {
		position = len(d.resource.samples)
	}
	d.position = position
}

func (d *rawDecoder) Position() int {
	return d.position
}

func (d *rawDecoder) Length() int {
	return len(d.resource.samples)
}
