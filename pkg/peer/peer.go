// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package peer

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"time"
	"unsafe"
)

//go:wasmimport terminal_games peer_send
//go:noescape
func peer_send(peer_ids_ptr unsafe.Pointer, peer_ids_count uint32, data_ptr unsafe.Pointer, data_len uint32) int32

//go:wasmimport terminal_games peer_recv
//go:noescape
func peer_recv(from_peer_ptr unsafe.Pointer, data_ptr unsafe.Pointer, data_max_len uint32) int32

//go:wasmimport terminal_games region_latency
//go:noescape
func region_latency(region_ptr unsafe.Pointer) int32

//go:wasmimport terminal_games peer_list
//go:noescape
func peer_list(peer_ids_ptr unsafe.Pointer, max_length uint32, total_count_ptr unsafe.Pointer) int32

// ID represents a peer identifier
type ID [16]byte

type RegionID [4]byte

func (r RegionID) String() string {
	// Filter out null bytes and return as string
	end := 4
	for i := 0; i < 4; i++ {
		if r[i] == 0 {
			end = i
			break
		}
	}
	return string(r[:end])
}

var ErrLatencyUnknown = errors.New("latency unknown")

// Latency returns the current latency to this region in milliseconds.
func (r RegionID) Latency() (uint32, error) {
	ms := region_latency(unsafe.Pointer(&r[0]))
	if ms < 0 {
		return 0, ErrLatencyUnknown
	}
	return uint32(ms), nil
}

// String returns a hex-encoded string representation of the ID
func (id ID) String() string {
	return hex.EncodeToString(id[:])
}

// ParseID parses a hex-encoded string into an ID
func ParseID(s string) (ID, error) {
	var id ID
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return id, fmt.Errorf("failed to decode hex: %w", err)
	}
	if len(decoded) != 16 {
		return id, fmt.Errorf("invalid ID length: expected 16 bytes, got %d", len(decoded))
	}
	copy(id[:], decoded)
	return id, nil
}

// Timestamp returns the time the peer ID was created
func (id ID) Timestamp() time.Time {
	ms := binary.BigEndian.Uint64(id[4:12])
	return time.Unix(int64(ms/1000), int64(ms%1000)*int64(time.Millisecond))
}

// Randomness returns the randomness component of the peer ID
func (id ID) Randomness() uint32 {
	return binary.BigEndian.Uint32(id[12:16])
}

// Region returns the region component of the peer ID
func (id ID) Region() RegionID {
	var region [4]byte
	copy(region[:], id[0:4])
	return region
}

// Latency returns the current latency to this peer in milliseconds
func (id ID) Latency() (uint32, error) {
	return id.Region().Latency()
}

// Send is shorthand for `peer.Send(data, id)`
func (id ID) Send(data []byte) error {
	return Send(data, id)
}

func CurrentID() ID {
	// PEER_ID is always defined as a valid peer id, so we can ignore the error
	id, _ := ParseID(os.Getenv("PEER_ID"))
	return id
}

// List returns all peers currently connected to this app across all regions.
// This includes the current peer.
func List() ([]ID, error) {
	for {
		preCount, err := Count()
		if err != nil {
			return nil, err
		}
		peers, totalCount, err := listN(preCount)
		if err != nil {
			return nil, err
		}
		if preCount == totalCount {
			return peers, nil
		}
		runtime.Gosched()
	}
}

func listN(length uint32) ([]ID, uint32, error) {
	var totalCount uint32

	if length == 0 {
		ret := peer_list(nil, 0, unsafe.Pointer(&totalCount))
		if ret < 0 {
			return nil, 0, errors.New("peer_list failed")
		}
		return nil, totalCount, nil
	}

	buf := make([]byte, length*16)
	ret := peer_list(unsafe.Pointer(&buf[0]), length, unsafe.Pointer(&totalCount))
	if ret < 0 {
		return nil, totalCount, errors.New("peer_list failed")
	}

	count := int(ret)
	peers := unsafe.Slice((*ID)(unsafe.Pointer(&buf[0])), count)

	return peers, totalCount, nil
}

// Count returns the total number of peers connected to this app without fetching the list.
func Count() (uint32, error) {
	_, totalCount, err := listN(0)
	if err != nil {
		return 0, err
	}
	return totalCount, nil
}

// Message represents a message received from a peer
type Message struct {
	From ID
	Data []byte
}

// Send sends data to one or more peers
func Send(data []byte, peerIDs ...ID) error {
	return SendTo(data, peerIDs)
}

// SendTo sends data to one or more peers
func SendTo(data []byte, peerIDs []ID) error {
	if len(peerIDs) == 0 {
		return errors.New("at least one peer ID is required")
	}
	if len(data) > 64*1024 {
		return errors.New("data too large: maximum 64KB")
	}
	if len(peerIDs) > 1024 {
		return errors.New("too many peer IDs: maximum 1024")
	}

	peerIDsBuf := make([]byte, len(peerIDs)*16)
	for i, id := range peerIDs {
		copy(peerIDsBuf[i*16:(i+1)*16], id[:])
	}

	var dataPtr unsafe.Pointer
	if len(data) > 0 {
		dataPtr = unsafe.Pointer(&data[0])
	}

	ret := peer_send(
		unsafe.Pointer(&peerIDsBuf[0]),
		uint32(len(peerIDs)),
		dataPtr,
		uint32(len(data)),
	)
	if ret < 0 {
		return errors.New("peer_send failed")
	}

	return nil
}

// Recv waits for a message from any peer
func Recv() (Message, error) {
	fromPeerBuf := make([]byte, 16)
	dataBuf := make([]byte, 64*1024)

	for {
		ret := peer_recv(
			unsafe.Pointer(&fromPeerBuf[0]),
			unsafe.Pointer(&dataBuf[0]),
			uint32(len(dataBuf)),
		)

		if ret > 0 {
			var fromPeer ID
			copy(fromPeer[:], fromPeerBuf)
			return Message{
				From: fromPeer,
				Data: dataBuf[:ret],
			}, nil
		} else if ret == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		} else {
			return Message{}, errors.New("peer_recv failed")
		}
	}
}

type ByPeerId []ID

var _ sort.Interface = ByPeerId(nil)

func (p ByPeerId) Len() int           { return len(p) }
func (p ByPeerId) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p ByPeerId) Less(i, j int) bool { return bytes.Compare(p[i][:], p[j][:]) < 0 }
