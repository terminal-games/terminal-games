package main

import (
	"fmt"
	"log"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"
	"github.com/terminal-games/terminal-games/pkg/bubblewrap"
)

type model struct {
	timeLeft       int
	lastChar       string
	w              int
	h              int
	mainStyle      lipgloss.Style
	x              int
	y              int
	isHoveringZone bool
}

type tickMsg time.Time

func main() {
	zone.NewGlobal()
	p := bubblewrap.NewProgram(model{
		timeLeft:  30,
		mainStyle: lipgloss.NewStyle().Padding(1).Border(lipgloss.NormalBorder()),
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
		}
	case tickMsg:
		m.timeLeft--
		if m.timeLeft <= 0 {
			return m, tea.Quit
		}
		return m, tick()
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
	return zone.Scan(m.mainStyle.Render(fmt.Sprintf("Hi. Last char: %v. Size: %vx%v Mouse: %v %v %v This program will exit in %d seconds...", m.lastChar, m.w, m.h, m.x, m.y, markedZone, m.timeLeft)))
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}
