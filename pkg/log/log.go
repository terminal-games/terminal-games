// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package log

import "unsafe"

const (
	LevelTrace = 0
	LevelDebug = 1
	LevelInfo  = 2
	LevelWarn  = 3
	LevelError = 4
)

//go:wasmimport terminal_games log
//go:noescape
func logHost(level uint32, msgPtr unsafe.Pointer, msgLen uint32) int32

// Log sends a message to the host. The host injects shortname and user_id as attributes.
// Level must be one of LevelTrace, LevelDebug, LevelInfo, LevelWarn, LevelError.
func Log(level uint32, message string) {
	if message == "" {
		return
	}
	if level > LevelError {
		level = LevelInfo
	}
	const maxLen = 4096
	if len(message) > maxLen {
		message = message[:maxLen]
	}
	ptr := unsafe.Pointer(unsafe.StringData(message))
	logHost(level, ptr, uint32(len(message)))
}
