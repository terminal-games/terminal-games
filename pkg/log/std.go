// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package log

import (
	"bytes"
	"io"
	"log"
	"log/slog"
	"sync"
)

type hostWriter struct {
	level uint32
	mu    sync.Mutex
	buf   bytes.Buffer
}

func (w *hostWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err = w.buf.Write(p)
	if err != nil {
		return n, err
	}
	for {
		line, err := w.buf.ReadBytes('\n')
		if err == io.EOF {
			w.buf.Reset()
			w.buf.Write(line)
			return n, nil
		}
		if err != nil {
			return n, err
		}
		msg := string(bytes.TrimRight(line, "\n"))
		if msg != "" {
			Log(w.level, msg)
		}
	}
}

func Writer(level uint32) io.Writer {
	return &hostWriter{level: level}
}

func init() {
	log.SetFlags(0)
	log.SetOutput(Writer(LevelInfo))
	slog.SetDefault(NewSlogLogger())
}
