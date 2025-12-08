package bubblewrap

import (
	"bytes"
	"os"
	"time"
	"unsafe"

	tea "github.com/charmbracelet/bubbletea"
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

func NewProgram(model tea.Model, opts ...tea.ProgramOption) *tea.Program {
	fromHost := &yieldingReadWriter{buf: bytes.NewBuffer(nil)}

	p := tea.NewProgram(model, append([]tea.ProgramOption{tea.WithInput(fromHost), tea.WithoutSignalHandler()}, opts...)...)

	buffer := make([]byte, 64)

	var currentSize dimensions
	go func() {
		for {
			n := terminal_read(unsafe.Pointer(&buffer[0]), uint32(len(buffer)))
			if n < 0 {
				os.Exit(1)
			}
			if n > 0 {
				fromHost.Write(buffer[:n])
			}

			var newSize dimensions
			terminal_size(unsafe.Pointer(&newSize.w), unsafe.Pointer(&newSize.h))
			if newSize != currentSize {
				p.Send(tea.WindowSizeMsg{Width: int(newSize.w), Height: int(newSize.h)})
				currentSize = newSize
			}

			time.Sleep(1 * time.Millisecond)
		}
	}()

	return p
}
