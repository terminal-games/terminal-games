// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package log

import (
	"encoding/json"
	"unsafe"
)

const (
	LevelTrace = 0
	LevelDebug = 1
	LevelInfo  = 2
	LevelWarn  = 3
	LevelError = 4
)

//go:wasmimport terminal_games log
//go:noescape
func hostLog(level uint32, msgPtr unsafe.Pointer, msgLen uint32) int32

func Log(level uint32, message string) {
	if message == "" {
		return
	}
	writeJSONLog(level, message, nil)
}

func writeJSONLog(level uint32, message string, attrs map[string]any) {
	entry := map[string]any{
		"level":   levelString(level),
		"message": message,
	}
	for key, value := range attrs {
		if key == "" || key == "level" || key == "message" || key == "msg" {
			continue
		}
		entry[key] = value
	}
	data, err := json.Marshal(entry)
	if err != nil {
		writeJSONLog(LevelError, "failed to marshal log entry", map[string]any{"log_error": err.Error()})
		return
	}
	if len(data) == 0 {
		return
	}
	n := len(data)
	if n > 16384 {
		n = 16384
	}
	hostLog(level, unsafe.Pointer(&data[0]), uint32(n))
}

func levelString(level uint32) string {
	switch level {
	case LevelTrace:
		return "trace"
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "info"
	}
}
