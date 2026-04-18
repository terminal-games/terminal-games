// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/terminal-games/terminal-games/pkg/internal/hosterr"
)

const (
	reqErrInvalidInput    = -1
	reqErrTooManyRequests = -2

	pollPending           = -1
	pollErrBufferTooSmall = -3
	pollErrRequestFailed  = -4
	pollErrInvalidID      = -8

	keyPartString = 1
	keyPartBytes  = 2
	keyPartI64    = 3
	keyPartU64    = 4
	keyPartBool   = 5

	cmdSet          = 1
	cmdDelete       = 2
	cmdCheckValue   = 3
	cmdCheckExists  = 4
	cmdCheckMissing = 5

	listRequestHeaderSize  = 16
	listResponseHeaderSize = 8
	optionalKeyMissing     = math.MaxUint32

	errorUnavailable   = 1
	errorTooManyWrites = 2
	errorCheckFailed   = 3
	errorQuotaExceeded = 4

	checkFailedKeyMissing    = 1
	checkFailedKeyExists     = 2
	checkFailedValueMismatch = 3
)

//go:wasmimport terminal_games kv_get_v1
//go:noescape
func kv_get(keyPtr unsafe.Pointer, keyLen uint32) int32

//go:wasmimport terminal_games kv_get_poll_v1
//go:noescape
func kv_get_poll(requestID int32, valuePtr unsafe.Pointer, valueMaxLen uint32, valueLenPtr unsafe.Pointer) int32

//go:wasmimport terminal_games kv_exec_v1
//go:noescape
func kv_exec(commandsPtr unsafe.Pointer, commandsLen uint32) int32

//go:wasmimport terminal_games kv_exec_poll_v1
//go:noescape
func kv_exec_poll(requestID int32, dataPtr unsafe.Pointer, dataMaxLen uint32, dataLenPtr unsafe.Pointer) int32

//go:wasmimport terminal_games kv_list_v1
//go:noescape
func kv_list(requestPtr unsafe.Pointer, requestLen uint32) int32

//go:wasmimport terminal_games kv_list_poll_v1
//go:noescape
func kv_list_poll(requestID int32, dataPtr unsafe.Pointer, dataMaxLen uint32, dataLenPtr unsafe.Pointer) int32

//go:wasmimport terminal_games kv_storage_used_v1
//go:noescape
func kv_storage_used() int32

//go:wasmimport terminal_games kv_storage_used_poll_v1
//go:noescape
func kv_storage_used_poll(requestID int32, dataPtr unsafe.Pointer, dataMaxLen uint32, dataLenPtr unsafe.Pointer) int32

var (
	ErrInvalidInput     = errors.New("invalid kv request")
	ErrTooManyRequests  = errors.New("too many pending kv requests")
	ErrInvalidRequestID = errors.New("invalid kv request ID")
)

type UnavailableError struct{}

func (e *UnavailableError) Error() string {
	return "kv unavailable"
}

type TooManyWritesInAtomicTransactionError struct{}

func (e *TooManyWritesInAtomicTransactionError) Error() string {
	return "kv atomic transactions may contain at most one write"
}

type CheckFailureReason uint8

const (
	CheckFailureReasonKeyMissing CheckFailureReason = iota + 1
	CheckFailureReasonKeyExists
	CheckFailureReasonValueMismatch
)

type CheckFailedError struct {
	Reason CheckFailureReason
}

func (e *CheckFailedError) Error() string {
	if e == nil {
		return "kv check failed"
	}
	switch e.Reason {
	case CheckFailureReasonKeyMissing:
		return "kv check failed: key missing"
	case CheckFailureReasonKeyExists:
		return "kv check failed: key exists"
	case CheckFailureReasonValueMismatch:
		return "kv check failed: value mismatch"
	default:
		return "kv check failed"
	}
}

type QuotaExceededError struct {
	UsedBytes  uint64
	LimitBytes uint64
}

func (e *QuotaExceededError) Error() string {
	if e == nil {
		return "kv quota exceeded"
	}
	return fmt.Sprintf("kv quota exceeded: %d > %d", e.UsedBytes, e.LimitBytes)
}

type Tuple []any

type Command struct {
	kind  uint32
	key   Tuple
	value any
}

type Entry struct {
	Key   Tuple
	Value any
}

type Iterator = iter.Seq2[Entry, error]

type optionalBytes struct {
	value   []byte
	present bool
}

type listPage struct {
	entries   []Entry
	nextAfter optionalBytes
}

type AtomicChecksBuilder struct {
	commands []Command
	err      error
}

type AtomicWriteBuilder struct {
	commands []Command
	err      error
}

type guestCommand struct {
	Tag      uint32
	KeyPtr   uint32
	KeyLen   uint32
	ValuePtr uint32
	ValueLen uint32
}

func Get(keyParts ...any) (any, error) {
	key, err := Parts(keyParts...)
	if err != nil {
		return nil, err
	}
	return GetTuple(key)
}

func GetTuple(key Tuple) (any, error) {
	encodedKey, err := encodeKey(key)
	if err != nil {
		return nil, err
	}
	requestID := kv_get(bufferPointer(encodedKey), uint32(len(encodedKey)))
	if requestID < 0 {
		return nil, hostCodeError(requestID)
	}
	bytes, found, err := pollGet(requestID)
	if err != nil || !found {
		return nil, err
	}
	return decodeValue(bytes)
}

func Set(value any, keyParts ...any) error {
	key, err := Parts(keyParts...)
	if err != nil {
		return err
	}
	return SetTuple(value, key)
}

func SetTuple(value any, key Tuple) error {
	return ExecList([]Command{{kind: cmdSet, key: key, value: value}})
}

func Atomic() *AtomicChecksBuilder { return &AtomicChecksBuilder{} }

func Exec(commands ...Command) error { return ExecList(commands) }

func ExecList(commands []Command) error {
	if err := validateCommands(commands); err != nil {
		return err
	}
	encoded, err := encodeCommands(commands)
	if err != nil {
		return err
	}
	requestID := kv_exec(
		unsafe.Pointer(slicePointer(encoded.commands)),
		uint32(len(encoded.commands))*uint32(unsafe.Sizeof(guestCommand{})),
	)
	if requestID < 0 {
		return hostCodeError(requestID)
	}
	return pollExec(requestID)
}

func List(prefix, start, end Tuple) Iterator {
	return func(yield func(Entry, error) bool) {
		prefixBytes, err := encodeKey(prefix)
		if err != nil {
			yield(Entry{}, err)
			return
		}
		startBytes, err := encodeOptionalTuple(start)
		if err != nil {
			yield(Entry{}, err)
			return
		}
		endBytes, err := encodeOptionalTuple(end)
		if err != nil {
			yield(Entry{}, err)
			return
		}

		var afterBytes optionalBytes
		for {
			request, err := encodeListRequest(prefixBytes, startBytes, endBytes, afterBytes)
			if err != nil {
				yield(Entry{}, err)
				return
			}
			requestID := kv_list(bufferPointer(request), uint32(len(request)))
			if requestID < 0 {
				yield(Entry{}, hostCodeError(requestID))
				return
			}
			page, err := pollListPage(requestID)
			if err != nil {
				yield(Entry{}, err)
				return
			}
			for _, entry := range page.entries {
				if !yield(entry, nil) {
					return
				}
			}
			if !page.nextAfter.present {
				return
			}
			afterBytes = page.nextAfter
		}
	}
}

func StorageUsed() (uint64, error) {
	requestID := kv_storage_used()
	if requestID < 0 {
		return 0, hostCodeError(requestID)
	}
	return pollStorageUsed(requestID)
}

func SetCommand(value any, keyParts ...any) (Command, error) {
	key, err := Parts(keyParts...)
	if err != nil {
		return Command{}, err
	}
	return Command{kind: cmdSet, key: key, value: value}, nil
}

func DeleteCommand(keyParts ...any) (Command, error) {
	key, err := Parts(keyParts...)
	if err != nil {
		return Command{}, err
	}
	return Command{kind: cmdDelete, key: key}, nil
}

func CheckCommand(value any, keyParts ...any) (Command, error) {
	key, err := Parts(keyParts...)
	if err != nil {
		return Command{}, err
	}
	return Command{kind: cmdCheckValue, key: key, value: value}, nil
}

func CheckExistsCommand(keyParts ...any) (Command, error) {
	key, err := Parts(keyParts...)
	if err != nil {
		return Command{}, err
	}
	return Command{kind: cmdCheckExists, key: key}, nil
}

func CheckMissingCommand(keyParts ...any) (Command, error) {
	key, err := Parts(keyParts...)
	if err != nil {
		return Command{}, err
	}
	return Command{kind: cmdCheckMissing, key: key}, nil
}

func Parts(values ...any) (Tuple, error) {
	return normalizeTuple(values)
}

func (b *AtomicChecksBuilder) Set(value any, keyParts ...any) *AtomicWriteBuilder {
	if b.err != nil {
		return &AtomicWriteBuilder{commands: append([]Command(nil), b.commands...), err: b.err}
	}
	key, err := Parts(keyParts...)
	if err != nil {
		b.err = err
		return &AtomicWriteBuilder{commands: append([]Command(nil), b.commands...), err: b.err}
	}
	return b.SetTuple(value, key)
}

func (b *AtomicChecksBuilder) SetTuple(value any, key Tuple) *AtomicWriteBuilder {
	commands := append(append([]Command(nil), b.commands...), Command{kind: cmdSet, key: key, value: value})
	return &AtomicWriteBuilder{commands: commands, err: b.err}
}

func (b *AtomicChecksBuilder) Delete(keyParts ...any) *AtomicWriteBuilder {
	if b.err != nil {
		return &AtomicWriteBuilder{commands: append([]Command(nil), b.commands...), err: b.err}
	}
	key, err := Parts(keyParts...)
	if err != nil {
		b.err = err
		return &AtomicWriteBuilder{commands: append([]Command(nil), b.commands...), err: b.err}
	}
	return b.DeleteTuple(key)
}

func (b *AtomicChecksBuilder) DeleteTuple(key Tuple) *AtomicWriteBuilder {
	commands := append(append([]Command(nil), b.commands...), Command{kind: cmdDelete, key: key})
	return &AtomicWriteBuilder{commands: commands, err: b.err}
}

func (b *AtomicChecksBuilder) Check(value any, keyParts ...any) *AtomicChecksBuilder {
	if b.err != nil {
		return b
	}
	key, err := Parts(keyParts...)
	if err != nil {
		b.err = err
		return b
	}
	return b.CheckTuple(value, key)
}

func (b *AtomicChecksBuilder) CheckTuple(value any, key Tuple) *AtomicChecksBuilder {
	if b.err == nil {
		b.commands = append(b.commands, Command{kind: cmdCheckValue, key: key, value: value})
	}
	return b
}

func (b *AtomicChecksBuilder) CheckExists(keyParts ...any) *AtomicChecksBuilder {
	if b.err != nil {
		return b
	}
	key, err := Parts(keyParts...)
	if err != nil {
		b.err = err
		return b
	}
	return b.CheckExistsTuple(key)
}

func (b *AtomicChecksBuilder) CheckExistsTuple(key Tuple) *AtomicChecksBuilder {
	if b.err == nil {
		b.commands = append(b.commands, Command{kind: cmdCheckExists, key: key})
	}
	return b
}

func (b *AtomicChecksBuilder) CheckMissing(keyParts ...any) *AtomicChecksBuilder {
	if b.err != nil {
		return b
	}
	key, err := Parts(keyParts...)
	if err != nil {
		b.err = err
		return b
	}
	return b.CheckMissingTuple(key)
}

func (b *AtomicChecksBuilder) CheckMissingTuple(key Tuple) *AtomicChecksBuilder {
	if b.err == nil {
		b.commands = append(b.commands, Command{kind: cmdCheckMissing, key: key})
	}
	return b
}

func (b *AtomicChecksBuilder) Exec() error {
	if b.err != nil {
		return b.err
	}
	return ExecList(b.commands)
}

func (b *AtomicWriteBuilder) Check(value any, keyParts ...any) *AtomicWriteBuilder {
	if b.err != nil {
		return b
	}
	key, err := Parts(keyParts...)
	if err != nil {
		b.err = err
		return b
	}
	return b.CheckTuple(value, key)
}

func (b *AtomicWriteBuilder) CheckTuple(value any, key Tuple) *AtomicWriteBuilder {
	if b.err == nil {
		b.commands = append(b.commands, Command{kind: cmdCheckValue, key: key, value: value})
	}
	return b
}

func (b *AtomicWriteBuilder) CheckExists(keyParts ...any) *AtomicWriteBuilder {
	if b.err != nil {
		return b
	}
	key, err := Parts(keyParts...)
	if err != nil {
		b.err = err
		return b
	}
	return b.CheckExistsTuple(key)
}

func (b *AtomicWriteBuilder) CheckExistsTuple(key Tuple) *AtomicWriteBuilder {
	if b.err == nil {
		b.commands = append(b.commands, Command{kind: cmdCheckExists, key: key})
	}
	return b
}

func (b *AtomicWriteBuilder) CheckMissing(keyParts ...any) *AtomicWriteBuilder {
	if b.err != nil {
		return b
	}
	key, err := Parts(keyParts...)
	if err != nil {
		b.err = err
		return b
	}
	return b.CheckMissingTuple(key)
}

func (b *AtomicWriteBuilder) CheckMissingTuple(key Tuple) *AtomicWriteBuilder {
	if b.err == nil {
		b.commands = append(b.commands, Command{kind: cmdCheckMissing, key: key})
	}
	return b
}

func (b *AtomicWriteBuilder) Exec() error {
	if b.err != nil {
		return b.err
	}
	return ExecList(b.commands)
}

func validateCommands(commands []Command) error {
	writeCount := 0
	for _, command := range commands {
		switch command.kind {
		case cmdSet, cmdDelete:
			writeCount++
			if writeCount > 1 {
				return &TooManyWritesInAtomicTransactionError{}
			}
		}
	}
	return nil
}

type encodedCommands struct {
	keys     [][]byte
	values   [][]byte
	commands []guestCommand
}

func encodeCommands(commands []Command) (encodedCommands, error) {
	encoded := encodedCommands{
		keys:     make([][]byte, 0, len(commands)),
		values:   make([][]byte, 0, len(commands)),
		commands: make([]guestCommand, 0, len(commands)),
	}
	for _, command := range commands {
		key, err := encodeKey(command.key)
		if err != nil {
			return encodedCommands{}, err
		}
		encoded.keys = append(encoded.keys, key)
		keyBytes := encoded.keys[len(encoded.keys)-1]

		var valueBytes []byte
		switch command.kind {
		case cmdSet, cmdCheckValue:
			valueBytes, err = encodeValue(command.value)
			if err != nil {
				return encodedCommands{}, err
			}
			encoded.values = append(encoded.values, valueBytes)
			valueBytes = encoded.values[len(encoded.values)-1]
		case cmdDelete, cmdCheckExists, cmdCheckMissing:
		default:
			return encodedCommands{}, fmt.Errorf("unknown kv command %d", command.kind)
		}

		encoded.commands = append(encoded.commands, guestCommand{
			Tag:      command.kind,
			KeyPtr:   uint32(uintptr(unsafe.Pointer(slicePointer(keyBytes)))),
			KeyLen:   uint32(len(keyBytes)),
			ValuePtr: uint32(uintptr(unsafe.Pointer(slicePointer(valueBytes)))),
			ValueLen: uint32(len(valueBytes)),
		})
	}
	return encoded, nil
}

func pollGet(requestID int32) ([]byte, bool, error) {
	buffer := make([]byte, 256)
	for {
		var valueLen uint32
		result := kv_get_poll(
			requestID,
			bufferPointer(buffer),
			uint32(len(buffer)),
			unsafe.Pointer(&valueLen),
		)
		switch result {
		case 1:
			return append([]byte(nil), buffer[:valueLen]...), true, nil
		case 0:
			return nil, false, nil
		case pollPending:
			time.Sleep(10 * time.Millisecond)
		case pollErrBufferTooSmall:
			buffer = make([]byte, valueLen)
		case pollErrRequestFailed:
			return nil, false, decodeRequestFailed(buffer[:valueLen])
		default:
			return nil, false, hostCodeError(result)
		}
	}
}

func pollExec(requestID int32) error {
	buffer := make([]byte, 256)
	for {
		var length uint32
		result := kv_exec_poll(requestID, bufferPointer(buffer), uint32(len(buffer)), unsafe.Pointer(&length))
		switch result {
		case 1:
			return nil
		case pollPending:
			time.Sleep(10 * time.Millisecond)
		case pollErrBufferTooSmall:
			buffer = make([]byte, length)
		case pollErrRequestFailed:
			return decodeRequestFailed(buffer[:length])
		default:
			return hostCodeError(result)
		}
	}
}

func pollListPage(requestID int32) (listPage, error) {
	buffer := make([]byte, 256)
	for {
		var length uint32
		result := kv_list_poll(requestID, bufferPointer(buffer), uint32(len(buffer)), unsafe.Pointer(&length))
		switch result {
		case 1:
			return decodeListPage(buffer[:length])
		case pollPending:
			time.Sleep(10 * time.Millisecond)
		case pollErrBufferTooSmall:
			buffer = make([]byte, length)
		case pollErrRequestFailed:
			return listPage{}, decodeRequestFailed(buffer[:length])
		default:
			return listPage{}, hostCodeError(result)
		}
	}
}

func pollStorageUsed(requestID int32) (uint64, error) {
	buffer := make([]byte, 8)
	for {
		var length uint32
		result := kv_storage_used_poll(requestID, bufferPointer(buffer), uint32(len(buffer)), unsafe.Pointer(&length))
		switch result {
		case 1:
			if length != 8 {
				return 0, errors.New("invalid kv storage-used payload length")
			}
			return binary.LittleEndian.Uint64(buffer[:8]), nil
		case pollPending:
			time.Sleep(10 * time.Millisecond)
		case pollErrBufferTooSmall:
			buffer = make([]byte, length)
		case pollErrRequestFailed:
			return 0, decodeRequestFailed(buffer[:length])
		default:
			return 0, hostCodeError(result)
		}
	}
}

func encodeKey(tuple Tuple) ([]byte, error) {
	var out []byte
	for _, part := range tuple {
		switch part := part.(type) {
		case string:
			out = append(out, keyPartString)
			out = appendLengthPrefixed(out, []byte(part))
		case []byte:
			out = append(out, keyPartBytes)
			out = appendLengthPrefixed(out, part)
		case int:
			out = appendSigned(out, int64(part))
		case int8:
			out = appendSigned(out, int64(part))
		case int16:
			out = appendSigned(out, int64(part))
		case int32:
			out = appendSigned(out, int64(part))
		case int64:
			out = appendSigned(out, part)
		case uint:
			out = appendUnsigned(out, uint64(part))
		case uint8:
			out = appendUnsigned(out, uint64(part))
		case uint16:
			out = appendUnsigned(out, uint64(part))
		case uint32:
			out = appendUnsigned(out, uint64(part))
		case uint64:
			out = appendUnsigned(out, part)
		case bool:
			out = append(out, keyPartBool)
			if part {
				out = appendLengthPrefixed(out, []byte{1})
			} else {
				out = appendLengthPrefixed(out, []byte{0})
			}
		default:
			return nil, fmt.Errorf("unsupported kv key part type %T", part)
		}
	}
	return out, nil
}

func encodeValue(value any) ([]byte, error) {
	switch value := value.(type) {
	case string:
		return appendPart(nil, keyPartString, []byte(value)), nil
	case []byte:
		return appendPart(nil, keyPartBytes, value), nil
	case int:
		return encodeSignedValue(int64(value)), nil
	case int8:
		return encodeSignedValue(int64(value)), nil
	case int16:
		return encodeSignedValue(int64(value)), nil
	case int32:
		return encodeSignedValue(int64(value)), nil
	case int64:
		return encodeSignedValue(value), nil
	case uint:
		return encodeUnsignedValue(uint64(value)), nil
	case uint8:
		return encodeUnsignedValue(uint64(value)), nil
	case uint16:
		return encodeUnsignedValue(uint64(value)), nil
	case uint32:
		return encodeUnsignedValue(uint64(value)), nil
	case uint64:
		return encodeUnsignedValue(value), nil
	case bool:
		if value {
			return appendPart(nil, keyPartBool, []byte{1}), nil
		}
		return appendPart(nil, keyPartBool, []byte{0}), nil
	default:
		return nil, fmt.Errorf("unsupported kv value type %T", value)
	}
}

func decodeValue(bytes []byte) (any, error) {
	if len(bytes) < 5 {
		return nil, errors.New("unexpected end of kv payload")
	}
	tag := bytes[0]
	length := binary.BigEndian.Uint32(bytes[1:5])
	if int(length) != len(bytes)-5 {
		return nil, errors.New("invalid kv payload length")
	}
	payload := bytes[5:]
	switch tag {
	case keyPartString:
		if !utf8.Valid(payload) {
			return nil, errors.New("invalid UTF-8 in kv string")
		}
		return string(payload), nil
	case keyPartBytes:
		return append([]byte(nil), payload...), nil
	case keyPartI64:
		if len(payload) != 8 {
			return nil, errors.New("invalid kv i64 payload length")
		}
		return int64(binary.BigEndian.Uint64(payload) ^ (1 << 63)), nil
	case keyPartU64:
		if len(payload) != 8 {
			return nil, errors.New("invalid kv u64 payload length")
		}
		return binary.BigEndian.Uint64(payload), nil
	case keyPartBool:
		if len(payload) != 1 {
			return nil, errors.New("invalid kv bool payload")
		}
		switch payload[0] {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return nil, errors.New("invalid kv bool payload")
		}
	default:
		return nil, fmt.Errorf("unknown kv value tag %d", tag)
	}
}

func decodeKey(bytes []byte) (Tuple, error) {
	var out Tuple
	for len(bytes) > 0 {
		if len(bytes) < 5 {
			return nil, errors.New("unexpected end of kv key payload")
		}
		tag := bytes[0]
		length := binary.BigEndian.Uint32(bytes[1:5])
		if int(length) > len(bytes)-5 {
			return nil, errors.New("invalid kv key payload length")
		}
		payload := bytes[5 : 5+length]
		switch tag {
		case keyPartString:
			if !utf8.Valid(payload) {
				return nil, errors.New("invalid UTF-8 in kv string")
			}
			out = append(out, string(payload))
		case keyPartBytes:
			out = append(out, append([]byte(nil), payload...))
		case keyPartI64:
			if len(payload) != 8 {
				return nil, errors.New("invalid kv i64 payload length")
			}
			out = append(out, int64(binary.BigEndian.Uint64(payload)^(1<<63)))
		case keyPartU64:
			if len(payload) != 8 {
				return nil, errors.New("invalid kv u64 payload length")
			}
			out = append(out, binary.BigEndian.Uint64(payload))
		case keyPartBool:
			if len(payload) != 1 {
				return nil, errors.New("invalid kv bool payload")
			}
			switch payload[0] {
			case 0:
				out = append(out, false)
			case 1:
				out = append(out, true)
			default:
				return nil, errors.New("invalid kv bool payload")
			}
		default:
			return nil, fmt.Errorf("unknown kv key tag %d", tag)
		}
		bytes = bytes[5+length:]
	}
	return out, nil
}

func encodeOptionalTuple(tuple Tuple) (optionalBytes, error) {
	if tuple == nil {
		return optionalBytes{}, nil
	}
	bytes, err := encodeKey(tuple)
	if err != nil {
		return optionalBytes{}, err
	}
	return optionalBytes{value: bytes, present: true}, nil
}

func encodeListRequest(prefix []byte, start, end, after optionalBytes) ([]byte, error) {
	out := make([]byte, 0, listRequestHeaderSize+len(prefix)+len(start.value)+len(end.value)+len(after.value))
	out = appendUint32LE(out, uint32(len(prefix)))
	out = appendOptionalKeyLength(out, start)
	out = appendOptionalKeyLength(out, end)
	out = appendOptionalKeyLength(out, after)
	out = append(out, prefix...)
	out = append(out, start.value...)
	out = append(out, end.value...)
	out = append(out, after.value...)
	return out, nil
}

func decodeListPage(bytes []byte) (listPage, error) {
	if len(bytes) < listResponseHeaderSize {
		return listPage{}, errors.New("unexpected end of kv list payload")
	}
	count := int(binary.LittleEndian.Uint32(bytes[:4]))
	nextAfterLen := binary.LittleEndian.Uint32(bytes[4:8])
	bytes = bytes[listResponseHeaderSize:]

	var nextAfter optionalBytes
	if nextAfterLen != optionalKeyMissing {
		if int(nextAfterLen) > len(bytes) {
			return listPage{}, errors.New("unexpected end of kv list cursor payload")
		}
		nextAfter = optionalBytes{
			value:   append([]byte(nil), bytes[:nextAfterLen]...),
			present: true,
		}
		bytes = bytes[nextAfterLen:]
	}

	entries := make([]Entry, 0, count)
	for i := 0; i < count; i++ {
		if len(bytes) < 8 {
			return listPage{}, errors.New("unexpected end of kv list entry header")
		}
		keyLen := int(binary.LittleEndian.Uint32(bytes[:4]))
		valueLen := int(binary.LittleEndian.Uint32(bytes[4:8]))
		bytes = bytes[8:]
		if len(bytes) < keyLen+valueLen {
			return listPage{}, errors.New("unexpected end of kv list entry payload")
		}
		key, err := decodeKey(bytes[:keyLen])
		if err != nil {
			return listPage{}, err
		}
		value, err := decodeValue(bytes[keyLen : keyLen+valueLen])
		if err != nil {
			return listPage{}, err
		}
		entries = append(entries, Entry{Key: key, Value: value})
		bytes = bytes[keyLen+valueLen:]
	}
	if len(bytes) != 0 {
		return listPage{}, errors.New("unexpected trailing kv list payload bytes")
	}
	return listPage{entries: entries, nextAfter: nextAfter}, nil
}

func appendOptionalKeyLength(out []byte, value optionalBytes) []byte {
	if !value.present {
		return appendUint32LE(out, optionalKeyMissing)
	}
	return appendUint32LE(out, uint32(len(value.value)))
}

func normalizeTuple(values []any) (Tuple, error) {
	out := make(Tuple, len(values))
	for i, value := range values {
		switch value := value.(type) {
		case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, bool:
			out[i] = value
		case []byte:
			out[i] = append([]byte(nil), value...)
		default:
			return nil, fmt.Errorf("unsupported kv key part type %T", value)
		}
	}
	return out, nil
}

func appendSigned(out []byte, value int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value)^(1<<63))
	return appendPart(out, keyPartI64, buf[:])
}

func appendUnsigned(out []byte, value uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return appendPart(out, keyPartU64, buf[:])
}

func encodeSignedValue(value int64) []byte {
	return appendSigned(nil, value)
}

func encodeUnsignedValue(value uint64) []byte {
	return appendUnsigned(nil, value)
}

func appendPart(out []byte, tag byte, payload []byte) []byte {
	out = append(out, tag)
	return appendLengthPrefixed(out, payload)
}

func appendLengthPrefixed(out []byte, payload []byte) []byte {
	if len(payload) > math.MaxUint32 {
		panic("kv payload too large")
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	out = append(out, lenBuf[:]...)
	out = append(out, payload...)
	return out
}

func appendUint32LE(out []byte, value uint32) []byte {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], value)
	return append(out, lenBuf[:]...)
}

func hostCodeError(code int32) error {
	if err := hosterr.MaybeVersionMismatch("terminal_games.kv", code); err != nil {
		return err
	}
	switch code {
	case reqErrInvalidInput:
		return ErrInvalidInput
	case reqErrTooManyRequests:
		return ErrTooManyRequests
	case pollErrInvalidID:
		return ErrInvalidRequestID
	default:
		return fmt.Errorf("kv host call failed with code %d", code)
	}
}

func decodeRequestFailed(bytes []byte) error {
	if len(bytes) < 4 {
		return errors.New("invalid kv error payload")
	}
	tag := binary.LittleEndian.Uint32(bytes[:4])
	switch tag {
	case errorUnavailable:
		return &UnavailableError{}
	case errorTooManyWrites:
		return &TooManyWritesInAtomicTransactionError{}
	case errorCheckFailed:
		if len(bytes) != 8 {
			return errors.New("invalid kv check-failed payload")
		}
		switch binary.LittleEndian.Uint32(bytes[4:8]) {
		case checkFailedKeyMissing:
			return &CheckFailedError{Reason: CheckFailureReasonKeyMissing}
		case checkFailedKeyExists:
			return &CheckFailedError{Reason: CheckFailureReasonKeyExists}
		case checkFailedValueMismatch:
			return &CheckFailedError{Reason: CheckFailureReasonValueMismatch}
		default:
			return errors.New("invalid kv check-failed payload")
		}
	case errorQuotaExceeded:
		if len(bytes) != 20 {
			return errors.New("invalid kv quota-exceeded payload")
		}
		return &QuotaExceededError{
			UsedBytes:  binary.LittleEndian.Uint64(bytes[4:12]),
			LimitBytes: binary.LittleEndian.Uint64(bytes[12:20]),
		}
	default:
		return errors.New("unknown kv error payload")
	}
}

func bufferPointer(buffer []byte) unsafe.Pointer {
	if len(buffer) == 0 {
		return nil
	}
	return unsafe.Pointer(&buffer[0])
}

func slicePointer[T any](buffer []T) *T {
	if len(buffer) == 0 {
		return nil
	}
	return &buffer[0]
}
