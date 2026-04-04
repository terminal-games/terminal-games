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
	"unicode/utf8"
	"unsafe"
)

const (
	peerSendErrInvalidPeerCount = -1
	peerSendErrDataTooLarge     = -2
	peerSendErrChannelFull      = -3
	peerSendErrChannelClosed    = -4
	peerSendErrInvalidPeerID    = -5

	peerRecvErrChannelDisconnected = -1
)

//go:wasmimport terminal_games peer_send
//go:noescape
func peer_send(peer_ids_ptr unsafe.Pointer, peer_ids_count uint32, data_ptr unsafe.Pointer, data_len uint32) int32

//go:wasmimport terminal_games peer_recv
//go:noescape
func peer_recv(from_peer_ptr unsafe.Pointer, data_ptr unsafe.Pointer, data_max_len uint32) int32

//go:wasmimport terminal_games node_latency
//go:noescape
func node_latency(node_ptr unsafe.Pointer) int32

//go:wasmimport terminal_games peer_list
//go:noescape
func peer_list(peer_ids_ptr unsafe.Pointer, max_length uint32, total_count_ptr unsafe.Pointer) int32

// ID represents a peer identifier
type ID struct {
	node       NodeID
	timestamp  uint64
	randomness uint32
}

type NodeID [4]byte

const (
	nodeIDByteLen = 4
	idByteLen     = 16
)

func (r NodeID) String() string {
	return string(r[:])
}

var (
	ErrLatencyUnknown      = errors.New("latency unknown")
	ErrInvalidPeerCount    = errors.New("invalid peer count (must be 1-1024)")
	ErrDataTooLarge        = errors.New("data too large: maximum 64KB")
	ErrChannelFull         = errors.New("send channel full, message dropped")
	ErrChannelClosed       = errors.New("send channel closed")
	ErrInvalidPeerID       = errors.New("invalid peer ID: peer ID contains invalid node bytes")
	ErrChannelDisconnected = errors.New("receive channel disconnected")
	ErrListFailed          = errors.New("peer_list failed")
)

// Latency returns the current latency to this node in milliseconds.
func (r NodeID) Latency() (uint32, error) {
	ms := node_latency(unsafe.Pointer(&r[0]))
	if ms < 0 {
		return 0, ErrLatencyUnknown
	}
	return uint32(ms), nil
}

// String returns the human-readable node-timestamp-randomness representation of the ID.
func (id ID) String() string {
	return fmt.Sprintf("%s-%d-%d", id.node, id.timestamp, id.randomness)
}

// ParseID parses a hex-encoded string into an ID
func ParseID(s string) (ID, error) {
	var id ID
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return id, fmt.Errorf("failed to decode hex: %w", err)
	}
	if len(decoded) != idByteLen {
		return id, fmt.Errorf("invalid ID length: expected %d bytes, got %d", idByteLen, len(decoded))
	}
	var bytes [idByteLen]byte
	copy(bytes[:], decoded)
	id = idFromBytes(bytes)
	if !utf8.Valid(id.node[:]) {
		return ID{}, ErrInvalidPeerID
	}
	return id, nil
}

// Timestamp returns the time the peer ID was created
func (id ID) Timestamp() time.Time {
	ms := id.timestamp
	return time.Unix(int64(ms/1000), int64(ms%1000)*int64(time.Millisecond))
}

// Randomness returns the randomness component of the peer ID
func (id ID) Randomness() uint32 {
	return id.randomness
}

// Node returns the node component of the peer ID
func (id ID) Node() NodeID {
	return id.node
}

// Latency returns the current latency to this peer in milliseconds
func (id ID) Latency() (uint32, error) {
	return id.Node().Latency()
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

// List returns all peers currently connected to this app across all nodes.
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
			return nil, 0, ErrListFailed
		}
		return nil, totalCount, nil
	}

	buf := make([]byte, length*idByteLen)
	ret := peer_list(unsafe.Pointer(&buf[0]), length, unsafe.Pointer(&totalCount))
	if ret < 0 {
		return nil, totalCount, ErrListFailed
	}

	count := int(ret)
	peers := make([]ID, count)
	for i := range peers {
		offset := i * idByteLen
		var peerBytes [idByteLen]byte
		copy(peerBytes[:], buf[offset:offset+idByteLen])
		peers[i] = idFromBytes(peerBytes)
	}

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
	if len(peerIDs) == 0 || len(peerIDs) > 1024 {
		return ErrInvalidPeerCount
	}
	if len(data) > 64*1024 {
		return ErrDataTooLarge
	}

	peerIDsBuf := make([]byte, len(peerIDs)*idByteLen)
	for i, id := range peerIDs {
		peerBytes := id.toBytes()
		copy(peerIDsBuf[i*idByteLen:(i+1)*idByteLen], peerBytes[:])
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
		return sendErrorFromCode(ret)
	}

	return nil
}

func sendErrorFromCode(code int32) error {
	switch code {
	case peerSendErrInvalidPeerCount:
		return ErrInvalidPeerCount
	case peerSendErrDataTooLarge:
		return ErrDataTooLarge
	case peerSendErrChannelFull:
		return ErrChannelFull
	case peerSendErrChannelClosed:
		return ErrChannelClosed
	case peerSendErrInvalidPeerID:
		return ErrInvalidPeerID
	default:
		return fmt.Errorf("unknown peer_send error: %d", code)
	}
}

// Recv waits for a message from any peer
func Recv() (Message, error) {
	fromPeerBuf := make([]byte, idByteLen)
	dataBuf := make([]byte, 64*1024)

	for {
		ret := peer_recv(
			unsafe.Pointer(&fromPeerBuf[0]),
			unsafe.Pointer(&dataBuf[0]),
			uint32(len(dataBuf)),
		)

		if ret > 0 {
			var fromPeerBytes [idByteLen]byte
			copy(fromPeerBytes[:], fromPeerBuf)
			return Message{
				From: idFromBytes(fromPeerBytes),
				Data: dataBuf[:ret],
			}, nil
		} else if ret == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		} else {
			return Message{}, recvErrorFromCode(ret)
		}
	}
}

func recvErrorFromCode(code int32) error {
	switch code {
	case peerRecvErrChannelDisconnected:
		return ErrChannelDisconnected
	default:
		return fmt.Errorf("unknown peer_recv error: %d", code)
	}
}

type ByPeerId []ID

var _ sort.Interface = ByPeerId(nil)

func (p ByPeerId) Len() int      { return len(p) }
func (p ByPeerId) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p ByPeerId) Less(i, j int) bool {
	leftNode := p[i].node
	rightNode := p[j].node
	if leftNode != rightNode {
		return bytes.Compare(leftNode[:], rightNode[:]) < 0
	}

	leftTimestamp := p[i].timestamp
	rightTimestamp := p[j].timestamp
	if leftTimestamp != rightTimestamp {
		return leftTimestamp < rightTimestamp
	}

	return p[i].randomness < p[j].randomness
}

func idFromBytes(bytes [idByteLen]byte) ID {
	var node NodeID
	copy(node[:], bytes[:nodeIDByteLen])
	return ID{
		node:       node,
		timestamp:  binary.BigEndian.Uint64(bytes[nodeIDByteLen:12]),
		randomness: binary.BigEndian.Uint32(bytes[12:idByteLen]),
	}
}

func (id ID) toBytes() [idByteLen]byte {
	var bytes [idByteLen]byte
	copy(bytes[:nodeIDByteLen], id.node[:])
	binary.BigEndian.PutUint64(bytes[nodeIDByteLen:12], id.timestamp)
	binary.BigEndian.PutUint32(bytes[12:idByteLen], id.randomness)
	return bytes
}
