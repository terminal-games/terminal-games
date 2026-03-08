// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package log

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
)

const SlogLevelTrace = slog.Level(-8)

type hostHandler struct {
	attrs []slog.Attr
	level slog.Leveler
}

func (h *hostHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *hostHandler) Handle(_ context.Context, r slog.Record) error {
	level := slogLevelToHost(r.Level)
	attrs := make(map[string]any, len(h.attrs)+r.NumAttrs())
	for _, attr := range h.attrs {
		appendAttr(attrs, attr)
	}
	r.Attrs(func(a slog.Attr) bool {
		appendAttr(attrs, a)
		return true
	})
	if r.PC != 0 {
		frame, _ := runtime.CallersFrames([]uintptr{r.PC}).Next()
		if frame.File != "" {
			attrs["file"] = frame.File
		}
		if frame.Line > 0 {
			attrs["line"] = frame.Line
		}
	}
	writeJSONLog(level, r.Message, attrs)
	return nil
}

func (h *hostHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &hostHandler{
		attrs: append(sliceCopy(h.attrs), attrs...),
		level: h.level,
	}
}

func (h *hostHandler) WithGroup(_ string) slog.Handler {
	return &hostHandler{
		attrs: sliceCopy(h.attrs),
		level: h.level,
	}
}

func slogLevelToHost(l slog.Level) uint32 {
	switch {
	case l < slog.LevelDebug:
		return LevelTrace
	case l < slog.LevelInfo:
		return LevelDebug
	case l < slog.LevelWarn:
		return LevelInfo
	case l < slog.LevelError:
		return LevelWarn
	default:
		return LevelError
	}
}

func appendAttr(attrs map[string]any, attr slog.Attr) {
	if attr.Key == "" {
		return
	}
	attrs[attr.Key] = fmt.Sprint(attr.Value.Any())
}

func sliceCopy[T any](s []T) []T {
	out := make([]T, len(s))
	copy(out, s)
	return out
}

// NewHostHandler returns an slog.Handler that writes structured logs via the host log function.
func NewHostHandler(level slog.Leveler) slog.Handler {
	if level == nil {
		level = SlogLevelTrace
	}
	return &hostHandler{level: level}
}

// NewSlogLogger returns an slog.Logger that writes structured logs via the host log function.
func NewSlogLogger() *slog.Logger {
	return slog.New(NewHostHandler(nil))
}
