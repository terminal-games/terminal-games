// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bubblewrap

import (
	"bytes"
	"image/color"
	"io"
	"os"
	"runtime"
	"strings"
	"time"
	"unsafe"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"github.com/charmbracelet/x/input"
	"github.com/lucasb-eyer/go-colorful"
	"github.com/muesli/termenv"

	"github.com/terminal-games/terminal-games/pkg/app"
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

//go:wasmimport terminal_games terminal_read
//go:noescape
func terminal_read(address_ptr unsafe.Pointer, addressLen uint32) int32

//go:wasmimport terminal_games terminal_size
//go:noescape
func terminal_size(width_ptr unsafe.Pointer, height_ptr unsafe.Pointer)

type dimensions struct {
	w uint16
	h uint16
}

var fromHost = &yieldingReadWriter{buf: bytes.NewBuffer(nil)}

func init() {
	go func() {
		buffer := make([]byte, 64)

		for {
			n := terminal_read(unsafe.Pointer(&buffer[0]), uint32(len(buffer)))
			if n < 0 {
				os.Exit(1)
			}
			if n > 0 {
				fromHost.Write(buffer[:n])
			}

			time.Sleep(1 * time.Millisecond)
		}
	}()
}

func NewProgram(model tea.Model, opts ...tea.ProgramOption) *tea.Program {
	p := tea.NewProgram(model, append([]tea.ProgramOption{tea.WithInput(fromHost), tea.WithoutSignalHandler()}, opts...)...)

	var currentSize dimensions
	sentQuit := false
	go func() {
		for {
			var newSize dimensions
			terminal_size(unsafe.Pointer(&newSize.w), unsafe.Pointer(&newSize.h))
			if newSize != currentSize {
				p.Send(tea.WindowSizeMsg{Width: int(newSize.w), Height: int(newSize.h)})
				currentSize = newSize
			}

			if app.GracefulShutdownPoll() && !sentQuit {
				p.Quit()
				sentQuit = true
			}

			time.Sleep(1 * time.Millisecond)
		}
	}()

	return p
}

func MakeRenderer() *lipgloss.Renderer {
	env := sshEnviron(os.Environ())
	r := lipgloss.NewRenderer(os.Stdout, termenv.WithEnvironment(env), termenv.WithUnsafe(), termenv.WithColorCache(true))
	bg := querySessionBackgroundColor(fromHost, os.Stdout)
	if bg != nil {
		c, ok := colorful.MakeColor(bg)
		if ok {
			_, _, l := c.Hsl()
			r.SetHasDarkBackground(l < 0.5)
		}
	}

	return r
}

type sshEnviron []string

var _ termenv.Environ = sshEnviron(nil)

// Environ implements termenv.Environ.
func (e sshEnviron) Environ() []string {
	return e
}

// Getenv implements termenv.Environ.
func (e sshEnviron) Getenv(k string) string {
	for _, v := range e {
		if strings.HasPrefix(v, k+"=") {
			return v[len(k)+1:]
		}
	}
	return ""
}

// copied from x/term@v0.1.3.
func querySessionBackgroundColor(in io.Reader, out io.Writer) (bg color.Color) {
	_ = queryTerminal(in, out, time.Second, func(events []input.Event) bool {
		for _, e := range events {
			switch e := e.(type) {
			case input.BackgroundColorEvent:
				bg = e.Color
				continue // we need to consume the next DA1 event
			case input.PrimaryDeviceAttributesEvent:
				return false
			}
		}
		return true
	}, ansi.RequestBackgroundColor+ansi.RequestPrimaryDeviceAttributes)
	return
}

// QueryTerminalFilter is a function that filters input events using a type
// switch. If false is returned, the QueryTerminal function will stop reading
// input.
type QueryTerminalFilter func(events []input.Event) bool

// queryTerminal queries the terminal for support of various features and
// returns a list of response events.
// Most of the time, you will need to set stdin to raw mode before calling this
// function.
// Note: This function will block until the terminal responds or the timeout
// is reached.
// copied from x/term@v0.1.3.
func queryTerminal(
	in io.Reader,
	out io.Writer,
	timeout time.Duration,
	filter QueryTerminalFilter,
	query string,
) error {
	rd, err := input.NewReader(in, "", 0)
	if err != nil {
		return err
	}

	defer rd.Close() // nolint: errcheck

	done := make(chan struct{}, 1)
	defer close(done)
	go func() {
		select {
		case <-done:
		case <-time.After(timeout):
			rd.Cancel()
		}
	}()

	if _, err := io.WriteString(out, query); err != nil {
		return err
	}

	for {
		runtime.Gosched()

		events, err := rd.ReadEvents()
		if err != nil {
			return err
		}

		if !filter(events) {
			break
		}
	}

	return nil
}
