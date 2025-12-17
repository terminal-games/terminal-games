// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/terminal-games/terminal-games/pkg/app"
	"github.com/terminal-games/terminal-games/pkg/bubblewrap"
)

type model struct {
	w         int
	h         int
	mainStyle lipgloss.Style
	loading   bool
}

func main() {
	p := bubblewrap.NewProgram(model{
		mainStyle: lipgloss.NewStyle().Padding(1).Border(lipgloss.NormalBorder()),
	}, tea.WithAltScreen(), tea.WithMouseAllMotion())
	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := message.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
		case "n":
			app.Change("kitchen-sink")
			m.loading = true
			return m, func() tea.Msg {
				for {
					if app.Ready() {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				return tea.Quit()
			}
		}
	case tea.WindowSizeMsg:
		m.w = msg.Width
		m.h = msg.Height
		return m, nil
	}

	return m, nil
}

func (m model) View() string {
	var loading string
	if m.loading {
		loading = "loading next app..."
	}
	content := m.mainStyle.Render(fmt.Sprintf("Menu. Size: %vx%v\n%v\n\n%+v", m.w, m.h, loading, os.Environ()))
	return lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Center, content)
}
