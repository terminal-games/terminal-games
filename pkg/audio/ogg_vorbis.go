// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Convert any audio file into OGG Vorbis with an ffmpeg command like the following ahead of time:
//
//	ffmpeg -i input.mp3 -af "pan=mono|c0=c1" -c:a libvorbis -qscale:a 2 -ar 48000 output.ogg
package audio

import (
	"bytes"
	"io"
	"time"

	"github.com/jfreymuth/oggvorbis"
)

type OGGVorbisResource struct {
	data        []byte
	sampleRate  int
	totalLength int
}

var _ Resource = (*OGGVorbisResource)(nil)

func NewResourceFromOGGVorbis(data []byte) (*OGGVorbisResource, error) {
	r, err := oggvorbis.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	return &OGGVorbisResource{
		data:        data,
		sampleRate:  r.SampleRate(),
		totalLength: int(r.Length()),
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

	toRead := min(len(buffer), remaining)

	written := 0
	for written < toRead {
		chunkSize := min(toRead-written, len(d.readBuffer))

		n, err := d.reader.Read(d.readBuffer[:chunkSize])
		if n > 0 {
			copy(buffer[written:], d.readBuffer[:n])
			written += n
			d.position += n
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
