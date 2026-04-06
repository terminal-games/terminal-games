// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bubblewrap

import (
	"bytes"
	"fmt"
	"runtime"
	"runtime/debug"
	"time"
	"unsafe"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"

	"github.com/terminal-games/terminal-games/pkg/app"
	"github.com/terminal-games/terminal-games/pkg/internal/hosterr"
)

type yieldingReadWriter struct {
	buf *bytes.Buffer
}

func (b *yieldingReadWriter) Read(p []byte) (n int, err error) {
	for b.buf.Len() == 0 {
		time.Sleep(1 * time.Millisecond)
	}
	return b.buf.Read(p)
}

func (b *yieldingReadWriter) Write(p []byte) (n int, err error) {
	return b.buf.Write(p)
}

//go:wasmimport terminal_games terminal_read_v1
//go:noescape
func terminal_read(address_ptr unsafe.Pointer, addressLen uint32) int32

//go:wasmimport terminal_games terminal_size_v1
//go:noescape
func terminal_size(width_ptr unsafe.Pointer, height_ptr unsafe.Pointer) int32

type dimensions struct {
	w uint16
	h uint16
}

var fromHost = &yieldingReadWriter{buf: bytes.NewBuffer(nil)}

func readTerminal(buffer []byte) (int, error) {
	n := terminal_read(unsafe.Pointer(&buffer[0]), uint32(len(buffer)))
	if n < 0 {
		if err := hosterr.MaybeVersionMismatch("terminal_games.terminal_read_v1", n); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("terminal_read failed with code %d", n)
	}
	return int(n), nil
}

func loadTerminalSize(size *dimensions) error {
	ret := terminal_size(unsafe.Pointer(&size.w), unsafe.Pointer(&size.h))
	if ret < 0 {
		if err := hosterr.MaybeVersionMismatch("terminal_games.terminal_size_v1", ret); err != nil {
			return err
		}
		return fmt.Errorf("terminal_size failed with code %d", ret)
	}
	return nil
}

func NewProgram(model tea.Model, opts ...tea.ProgramOption) (*tea.Program, error) {
	debug.SetMemoryLimit(24 * 1024 * 1024)

	var currentSize dimensions
	if err := loadTerminalSize(&currentSize); err != nil {
		return nil, err
	}

	profile := colorprofile.TrueColor
	if info, err := app.GetTerminalInfo(); err == nil {
		switch info.ColorMode {
		case app.TerminalColor16:
			profile = colorprofile.ANSI
		case app.TerminalColor256:
			profile = colorprofile.ANSI256
		case app.TerminalColorTrueColor:
			profile = colorprofile.TrueColor
		}
	}

	programOpts := append([]tea.ProgramOption{
		tea.WithInput(fromHost),
		tea.WithoutSignalHandler(),
		tea.WithWindowSize(int(currentSize.w), int(currentSize.h)),
		tea.WithColorProfile(profile),
	}, opts...)
	p := tea.NewProgram(model, programOpts...)

	go func() {
		buffer := make([]byte, 4096)

		for {
			n, err := readTerminal(buffer)
			if err != nil {
				p.Quit()
				return
			}
			if n > 0 {
				fromHost.Write(buffer[:n])
				runtime.Gosched()
			}

			if n == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	sentQuit := false
	go func() {
		for {
			var newSize dimensions
			if err := loadTerminalSize(&newSize); err != nil {
				p.Quit()
				return
			}
			if newSize != currentSize {
				p.Send(tea.WindowSizeMsg{Width: int(newSize.w), Height: int(newSize.h)})
				currentSize = newSize
			}

			gracefulShutdown, err := app.GracefulShutdownPoll()
			if err != nil {
				p.Quit()
				return
			}
			if gracefulShutdown && !sentQuit {
				p.Quit()
				sentQuit = true
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	return p, nil
}
