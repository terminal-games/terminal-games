// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"log"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"
	"github.com/terminal-games/terminal-games/cmd/menu/tabs"
	"github.com/terminal-games/terminal-games/pkg/bubblewrap"
)

const maxWidth = 80

type model struct {
	w           int
	h           int
	zone        *zone.Manager
	tabs        tabs.Model
	contentArea lipgloss.Style
	barStyle    lipgloss.Style
}

func main() {
	zoneManager := zone.New()

	menuTabs := []tabs.Tab{
		{ID: "games", Title: "Games"},
		{ID: "profile", Title: "Profile"},
		{ID: "about", Title: "About"},
	}

	p := bubblewrap.NewProgram(model{
		zone:        zoneManager,
		tabs:        tabs.New(menuTabs, zoneManager, "menu-tab-"),
		contentArea: lipgloss.NewStyle().Padding(1, 2),
		barStyle:    lipgloss.NewStyle().Foreground(lipgloss.Color("#888888")),
	}, tea.WithAltScreen(), tea.WithMouseAllMotion())

	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}

func (m model) Init() tea.Cmd {
	return m.tabs.Init()
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := message.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
		case "tab", "right", "l":
			next := (m.tabs.Active + 1) % len(m.tabs.Tabs)
			cmd := m.tabs.SetActive(next)
			return m, cmd
		case "shift+tab", "left", "h":
			prev := m.tabs.Active - 1
			if prev < 0 {
				prev = len(m.tabs.Tabs) - 1
			}
			cmd := m.tabs.SetActive(prev)
			return m, cmd
		}

	case tea.WindowSizeMsg:
		m.w = msg.Width
		m.h = msg.Height
		return m, nil

	case tabs.TabChangedMsg:
		return m, nil
	}

	var cmd tea.Cmd
	m.tabs, cmd = m.tabs.Update(message)
	if cmd != nil {
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	tabsView := m.tabs.View()
	tabsWidth := m.tabs.TotalWidth()

	targetWidth := maxWidth
	if m.w < targetWidth {
		targetWidth = m.w
	}

	paddingTotal := targetWidth - tabsWidth
	if paddingTotal < 0 {
		paddingTotal = 0
	}
	leftPad := paddingTotal / 2
	rightPad := paddingTotal - leftPad

	tabLines := strings.Split(tabsView, "\n")
	var centeredTabs strings.Builder
	for i, line := range tabLines {
		if i == len(tabLines)-1 {
			centeredTabs.WriteString(m.barStyle.Render(strings.Repeat(tabs.HeavyHorizontal, leftPad)))
			centeredTabs.WriteString(line)
			centeredTabs.WriteString(m.barStyle.Render(strings.Repeat(tabs.HeavyHorizontal, rightPad)))
		} else {
			centeredTabs.WriteString(strings.Repeat(" ", leftPad))
			centeredTabs.WriteString(line)
			centeredTabs.WriteString("\n")
		}
	}

	centeredTabsView := centeredTabs.String()

	var content string
	switch m.tabs.ActiveTab().ID {
	case "games":
		content = m.renderGamesTab()
	case "profile":
		content = m.renderProfileTab()
	case "about":
		content = m.renderAboutTab()
	default:
		content = "Unknown tab"
	}

	contentHeight := m.h - lipgloss.Height(centeredTabsView)
	if contentHeight < 0 {
		contentHeight = 0
	}

	styledContent := m.contentArea.
		Width(m.w).
		Height(contentHeight).
		Render(content)

	fullView := lipgloss.JoinVertical(lipgloss.Left, centeredTabsView, styledContent)
	return m.zone.Scan(lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Top, fullView))
}

func (m model) renderGamesTab() string {
	return "Games"
}

func (m model) renderProfileTab() string {
	return "Profile"
}

func (m model) renderAboutTab() string {
	return "About"
}
