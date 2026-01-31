// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package audio

import (
	"sync"
	"time"
)

type Mixer struct {
	instances []*Instance
	mu        sync.Mutex
	running   bool
	volume    float32

	mixBuffer     []float32
	scratchBuffer []float32
}

var globalMixer = &Mixer{
	instances: make([]*Instance, 0),
	volume:    1.0,
}

func GetMixer() *Mixer {
	return globalMixer
}

// SetMasterVolume sets the master volume for all mixed audio.
//
// 0.0 = silent, 1.0 = full volume.
func (m *Mixer) SetMasterVolume(v float32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.volume = v
}

func (m *Mixer) MasterVolume() float32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.volume
}

func (m *Mixer) addInstance(inst *Instance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.instances = append(m.instances, inst)
}

func (m *Mixer) removeInstance(inst *Instance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.instances {
		if existing == inst {
			m.instances = append(m.instances[:i], m.instances[i+1:]...)
			break
		}
	}
}

func (m *Mixer) mix(numFrames int) []float32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	numValues := numFrames * Channels
	if len(m.mixBuffer) < numValues {
		m.mixBuffer = make([]float32, numValues)
		m.scratchBuffer = make([]float32, numValues)
	}

	for i := range numValues {
		m.mixBuffer[i] = 0
	}

	for _, inst := range m.instances {
		if !inst.playing {
			continue
		}

		for i := range numValues {
			m.scratchBuffer[i] = 0
		}

		inst.fillBuffer(m.scratchBuffer[:numValues])

		for i := range numValues {
			m.mixBuffer[i] += m.scratchBuffer[i]
		}
	}

	for i := range numValues {
		sample := m.mixBuffer[i] * m.volume
		if sample > 1.0 {
			sample = 1.0
		} else if sample < -1.0 {
			sample = -1.0
		}
		m.mixBuffer[i] = sample
	}

	return m.mixBuffer[:numValues]
}

func startMixer() {
	globalMixer.mu.Lock()
	if globalMixer.running {
		globalMixer.mu.Unlock()
		return
	}
	globalMixer.running = true
	globalMixer.mu.Unlock()

	go func() {
		writer := NewAudioWriter(FrameSize * 4)

		for {
			neededFrames := writer.ShouldWrite()
			if neededFrames > 0 {
				mixed := globalMixer.mix(neededFrames)
				written := Write(mixed)
				if written > 0 {
					writer.NextPTS += uint64(written)
				}
			}

			time.Sleep(5 * time.Millisecond)
		}
	}()
}
