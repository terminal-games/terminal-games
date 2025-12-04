package bubblewrap

import (
	"bytes"
	"log"
	"runtime"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/terminal-games/terminal-games/internal/gen/terminal-games/app/terminal"
	"github.com/terminal-games/terminal-games/internal/gen/wasi/cli/stdin"
)

type yieldingReadWriter struct {
	buf *bytes.Buffer
}

func (b *yieldingReadWriter) Read(p []byte) (n int, err error) {
	for b.buf.Len() == 0 {
		runtime.Gosched()
	}
	return b.buf.Read(p)
}

func (b *yieldingReadWriter) Write(p []byte) (n int, err error) {
	return b.buf.Write(p)
}

func NewProgram(model tea.Model, opts ...tea.ProgramOption) *tea.Program {
	fromHost := &yieldingReadWriter{buf: bytes.NewBuffer(nil)}

	p := tea.NewProgram(model, append([]tea.ProgramOption{tea.WithInput(fromHost), tea.WithoutSignalHandler()}, opts...)...)

	stdinStream := stdin.GetStdin()

	var currentSize terminal.Dimensions
	go func() {
		for {
			list, err, isErr := stdinStream.Read(64).Result()
			if isErr {
				log.Fatalf("err=%v", err)
			}
			if list.Len() > 0 {
				fromHost.Write(list.Slice())
			}

			result := terminal.Size()

			if result != currentSize {
				p.Send(tea.WindowSizeMsg{Width: int(result.Width), Height: int(result.Height)})
				currentSize = result
			}

			runtime.Gosched()
		}
	}()

	return p
}
