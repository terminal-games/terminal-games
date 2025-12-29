// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"
	"github.com/terminal-games/terminal-games/pkg/bubblewrap"
	_ "github.com/terminal-games/terminal-games/pkg/net/http"
)

type model struct {
	timeLeft          int
	lastChar          string
	w                 int
	h                 int
	mainStyle         lipgloss.Style
	httpStyle         lipgloss.Style
	x                 int
	y                 int
	isHoveringZone    bool
	httpBody          string
	hasDarkBackground bool
}

type httpBodyMsg string

type tickMsg time.Time

func main() {
	r := bubblewrap.MakeRenderer()

	zone.NewGlobal()
	p := bubblewrap.NewProgram(model{
		timeLeft:          30,
		mainStyle:         lipgloss.NewStyle().Padding(1).Border(lipgloss.NormalBorder()),
		httpStyle:         lipgloss.NewStyle().Width(100),
		hasDarkBackground: r.HasDarkBackground(),
	}, tea.WithAltScreen(), tea.WithMouseAllMotion())
	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}

func (m model) Init() tea.Cmd {
	return tick()
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := message.(type) {
	case tea.MouseMsg:
		m.x = msg.X
		m.y = msg.Y
		m.isHoveringZone = zone.Get("myId").InBounds(msg)
		return m, nil
	case tea.KeyMsg:
		m.lastChar = msg.String()
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
		case "r":
			url := fmt.Sprintf("https://pokeapi.co/api/v2/pokemon?limit=1&offset=%v", rand.Int32N(800))

			m.httpBody = fmt.Sprintf("making request... %v", url)
			return m, func() tea.Msg {
				resp, err := http.Get(url)
				if err != nil {
					panic(err)
				}
				defer resp.Body.Close()

				body, err := io.ReadAll(resp.Body)
				if err != nil {
					panic(err)
				}

				return httpBodyMsg(body)
			}
		case "f":
			fmt.Fprintf(os.Stdout, "\x1b[%d;%dH%c", m.h+2, 2, 'A')
			return m, nil
		}
	case tickMsg:
		m.timeLeft--
		if m.timeLeft <= 0 {
			return m, tea.Quit
		}
		return m, tick()
	case httpBodyMsg:
		m.httpBody = string(msg)
		return m, nil
	case tea.WindowSizeMsg:
		m.w = msg.Width
		m.h = msg.Height
		return m, nil
	}

	return m, nil
}

func (m model) View() string {
	hoverString := "[hover me]"
	if m.isHoveringZone {
		hoverString = "[hello there]"
	}
	markedZone := zone.Mark("myId", hoverString)
	content := m.mainStyle.Render(fmt.Sprintf(
		"Hi. Last char: %v. Size: %vx%v Mouse: %v %v %v %s This program will exit in %d seconds...\n\n%v\n\n%+v\nhasDarkBackground=%v",
		m.lastChar, m.w, m.h, m.x, m.y, markedZone, TerminalOSC8Link("https://example.com", "example"), m.timeLeft, m.httpStyle.Render(m.httpBody), os.Environ(), m.hasDarkBackground,
	))
	return zone.Scan(lipgloss.Place(m.w, m.h, lipgloss.Left, lipgloss.Top, content))
}

func TerminalOSC8Link(link, text string) string {
	return fmt.Sprintf("\x1b]8;;%s\x1b\\%s\x1b]8;;\x1b\\", link, text)
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}
