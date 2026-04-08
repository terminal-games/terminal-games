// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bubblewrap

import (
	"io"
	"runtime"
	"runtime/debug"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"

	"github.com/terminal-games/terminal-games/pkg/app"
	"github.com/terminal-games/terminal-games/pkg/term"
)

// NewProgram assumes callers will not run multiple active host-input readers at
// once. Starting more than one program concurrently will cause terminal input
// to be split unpredictably between them.
func NewProgram(model tea.Model, opts ...tea.ProgramOption) (*tea.Program, error) {
	debug.SetMemoryLimit(24 * 1024 * 1024)

	width, height, err := term.Size()
	if err != nil {
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

	reader, writer := io.Pipe()
	programOpts := append([]tea.ProgramOption{
		tea.WithInput(reader),
		tea.WithoutSignalHandler(),
		tea.WithWindowSize(width, height),
		tea.WithColorProfile(profile),
	}, opts...)
	p := tea.NewProgram(model, programOpts...)
	done := make(chan struct{})

	go func() {
		p.Wait()
		close(done)
		_ = writer.Close()
	}()

	go func() {
		buffer := make([]byte, 4096)

		for {
			select {
			case <-done:
				return
			default:
			}

			n, err := term.Read(buffer)
			if err != nil {
				p.Kill()
				return
			}
			if n > 0 {
				if _, err := writer.Write(buffer[:n]); err != nil {
					return
				}
				runtime.Gosched()
			}

			newWidth, newHeight, err := term.Size()
			if err != nil {
				p.Kill()
				return
			}
			if newWidth != width || newHeight != height {
				width = newWidth
				height = newHeight
				p.Send(tea.WindowSizeMsg{Width: width, Height: height})
			}

			gracefulShutdown, err := app.GracefulShutdownPoll()
			if err != nil {
				p.Kill()
				return
			}
			if gracefulShutdown {
				p.Quit()
				return
			}

			if n == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	return p, nil
}
