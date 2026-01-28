// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Convert any audio file into OGG Vorbis with an ffmpeg command like the following ahead of time:
//
//	ffmpeg -i input.mp3 -c:a libvorbis -qscale:a 2 -ar 48000 output.ogg
package audio

import (
	"bytes"
	"io"
	"time"

	"github.com/jfreymuth/oggvorbis"
)

type OGGVorbisResource struct {
	data           []byte
	sampleRate     int
	sourceChannels int
	totalLength    int
}

var _ Resource = (*OGGVorbisResource)(nil)

func NewResourceFromOGGVorbis(data []byte) (*OGGVorbisResource, error) {
	r, err := oggvorbis.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	return &OGGVorbisResource{
		data:           data,
		sampleRate:     r.SampleRate(),
		sourceChannels: r.Channels(),
		totalLength:    int(r.Length()),
	}, nil
}

func (r *OGGVorbisResource) Duration() time.Duration {
	return time.Duration(float64(r.totalLength) / float64(SampleRate) * float64(time.Second))
}

func (r *OGGVorbisResource) SampleCount() int {
	return r.totalLength
}

func (r *OGGVorbisResource) NewInstance() *Instance {
	dec := newOGGVorbisDecoder(r)
	return newInstance(dec)
}

type oggVorbisDecoder struct {
	resource   *OGGVorbisResource
	reader     *oggvorbis.Reader
	position   int
	readBuffer []float32
}

var _ decoder = (*oggVorbisDecoder)(nil)

func newOGGVorbisDecoder(r *OGGVorbisResource) *oggVorbisDecoder {
	reader, _ := oggvorbis.NewReader(bytes.NewReader(r.data))
	return &oggVorbisDecoder{
		resource:   r,
		reader:     reader,
		position:   0,
		readBuffer: make([]float32, 4096),
	}
}

func (d *oggVorbisDecoder) Read(buffer []float32) (int, error) {
	if d.reader == nil {
		return 0, io.EOF
	}

	remaining := d.resource.totalLength - d.position
	if remaining <= 0 {
		return 0, io.EOF
	}

	maxFrames := len(buffer) / Channels
	toReadFrames := min(maxFrames, remaining)
	if toReadFrames == 0 {
		return 0, nil
	}

	sourceChannels := d.resource.sourceChannels
	written := 0

	for written/Channels < toReadFrames {
		sourceValuesToRead := min(toReadFrames-written/Channels, len(d.readBuffer)/sourceChannels) * sourceChannels
		n, err := d.reader.Read(d.readBuffer[:sourceValuesToRead])

		if n > 0 {
			framesRead := n / sourceChannels

			switch sourceChannels {
			case 1:
				for i := range framesRead {
					sample := d.readBuffer[i]
					buffer[written] = sample
					buffer[written+1] = sample
					written += 2
				}
			case 2:
				copy(buffer[written:], d.readBuffer[:n])
				written += n
			default:
				// Multi-channel: mixdown to stereo
				for i := range framesRead {
					var left, right float32
					for ch := range sourceChannels {
						sample := d.readBuffer[i*sourceChannels+ch]
						if ch%2 == 0 {
							left += sample
						} else {
							right += sample
						}
					}
					leftChannels := (sourceChannels + 1) / 2
					rightChannels := sourceChannels / 2
					if leftChannels > 0 {
						left /= float32(leftChannels)
					}
					if rightChannels > 0 {
						right /= float32(rightChannels)
					}
					buffer[written] = left
					buffer[written+1] = right
					written += 2
				}
			}

			d.position += framesRead
		}

		if err == io.EOF {
			if written > 0 {
				return written, nil
			}
			return 0, io.EOF
		}
		if err != nil {
			return written, err
		}
		if n == 0 {
			break
		}
	}

	return written, nil
}

func (d *oggVorbisDecoder) Seek(position int) {
	if position < 0 {
		position = 0
	}
	if position > d.resource.totalLength {
		position = d.resource.totalLength
	}
	d.position = position
	if d.reader != nil {
		d.reader.SetPosition(int64(position))
	}
}

func (d *oggVorbisDecoder) Position() int {
	return d.position
}

func (d *oggVorbisDecoder) Length() int {
	return d.resource.totalLength
}
