// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package log

import (
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/terminal-games/terminal-games/pkg/internal/hosterr"
)

const (
	LevelTrace = 0
	LevelDebug = 1
	LevelInfo  = 2
	LevelWarn  = 3
	LevelError = 4
)

//go:wasmimport terminal_games log_v1
//go:noescape
func hostLog(level uint32, msgPtr unsafe.Pointer, msgLen uint32) int32

func Log(level uint32, message string) error {
	if message == "" {
		return nil
	}
	return writeJSONLog(level, message, nil)
}

func writeJSONLog(level uint32, message string, attrs map[string]any) error {
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
		return err
	}
	if len(data) == 0 {
		return nil
	}
	n := len(data)
	if n > 16384 {
		n = 16384
	}
	ret := hostLog(level, unsafe.Pointer(&data[0]), uint32(n))
	if ret < 0 {
		if err := hosterr.MaybeVersionMismatch("terminal_games.log_v1", ret); err != nil {
			return err
		}
		return fmt.Errorf("log failed with code %d", ret)
	}
	return nil
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
