// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package log

import (
	"context"
	"log/slog"
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
	msg := r.Message
	if len(h.attrs) > 0 {
		msg = msg + " " + sprintAttrs(h.attrs)
	}
	r.Attrs(func(a slog.Attr) bool {
		msg = msg + " " + a.String()
		return true
	})
	Log(level, msg)
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

func sprintAttrs(attrs []slog.Attr) string {
	var buf []byte
	for i, a := range attrs {
		if i > 0 {
			buf = append(buf, ' ')
		}
		buf = append(buf, a.String()...)
	}
	return string(buf)
}

func sliceCopy[T any](s []T) []T {
	out := make([]T, len(s))
	copy(out, s)
	return out
}

// NewHostHandler returns an slog.Handler that forwards log records to the host.
func NewHostHandler(level slog.Leveler) slog.Handler {
	if level == nil {
		level = SlogLevelTrace
	}
	return &hostHandler{level: level}
}

// NewSlogLogger returns an slog.Logger that forwards log records to the host.
func NewSlogLogger() *slog.Logger {
	return slog.New(NewHostHandler(nil))
}
