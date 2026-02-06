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

const (
	maxWidth          = 120
	minContentHeight  = 5
	minViewportWidth  = 40
	minViewportHeight = 11
)

type model struct {
	w           int
	h           int
	zone        *zone.Manager
	tabs        tabs.Model
	contentArea lipgloss.Style
	barStyle    lipgloss.Style
	games       gamesModel
	profile     profileModel
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
		games:       newGamesModel(),
		profile:     newProfileModel(),
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
		case "q", "ctrl+c":
			return m, tea.Quit
		case "tab":
			next := (m.tabs.Active + 1) % len(m.tabs.Tabs)
			cmd := m.tabs.SetActive(next)
			return m, cmd
		case "shift+tab":
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
	if m.tabs.ActiveTab().ID == "games" {
		m.games, cmd = m.games.Update(message)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	} else if m.tabs.ActiveTab().ID == "profile" {
		m.profile, cmd = m.profile.Update(message)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}
	m.tabs, cmd = m.tabs.Update(message)
	if cmd != nil {
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	tabsView := m.tabs.View()
	tabsWidth := m.tabs.TotalWidth()
	title := " Terminal Games"
	titleWidth := lipgloss.Width(title)

	viewportWidth := maxWidth
	if m.w < viewportWidth {
		viewportWidth = m.w
	}
	requiredWidth := minViewportWidth
	if titleWidth+tabsWidth+1 > requiredWidth {
		requiredWidth = titleWidth + tabsWidth + 1
	}
	if viewportWidth < requiredWidth || m.h < minViewportHeight {
		return lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Center, "Window must be larger")
	}

	paddingTotal := viewportWidth - tabsWidth
	if paddingTotal < 0 {
		paddingTotal = 0
	}
	leftPad := paddingTotal / 2
	minLeftPad := titleWidth + 1
	if leftPad < minLeftPad {
		leftPad = minLeftPad
	}
	rightPad := viewportWidth - leftPad - tabsWidth
	if rightPad < 0 {
		rightPad = 0
	}

	tabLines := strings.Split(tabsView, "\n")
	var centeredTabs strings.Builder
	centeredTabs.WriteString(strings.Repeat(" ", viewportWidth))
	centeredTabs.WriteString("\n")
	if len(tabLines) > 0 {
		line := title + strings.Repeat(" ", leftPad-titleWidth) + tabLines[0] + strings.Repeat(" ", rightPad)
		centeredTabs.WriteString(lipgloss.NewStyle().Width(viewportWidth).Render(line))
	}
	if len(tabLines) > 1 {
		centeredTabs.WriteString("\n")
		barLine := m.barStyle.Render(strings.Repeat(tabs.HeavyHorizontal, leftPad)) + tabLines[1] +
			m.barStyle.Render(strings.Repeat(tabs.HeavyHorizontal, rightPad))
		centeredTabs.WriteString(lipgloss.NewStyle().Width(viewportWidth).Render(barLine))
	}

	centeredTabsView := lipgloss.NewStyle().Width(viewportWidth).Render(centeredTabs.String())

	contentHeight := m.h - lipgloss.Height(centeredTabsView)
	if contentHeight < 0 {
		contentHeight = 0
	}

	contentPaddingY := 1
	contentPaddingX := 2
	if contentHeight <= minContentHeight+2 {
		contentPaddingY = 0
		contentPaddingX = 1
	}

	contentWidth := viewportWidth - 2*contentPaddingX
	contentHeightInner := contentHeight - 2*contentPaddingY
	if contentWidth < 0 {
		contentWidth = 0
	}
	if contentHeightInner < 0 {
		contentHeightInner = 0
	}

	var content string
	switch m.tabs.ActiveTab().ID {
	case "games":
		content = m.renderGamesTab(contentWidth, contentHeightInner)
	case "profile":
		content = m.renderProfileTab(contentWidth, contentHeightInner)
	case "about":
		content = m.renderAboutTab(contentWidth, contentHeightInner)
	default:
		content = "Unknown tab"
	}

	styledContent := m.contentArea.
		Padding(contentPaddingY, contentPaddingX).
		Width(viewportWidth).
		Height(contentHeight).
		Render(content)

	fullView := lipgloss.JoinVertical(lipgloss.Left, centeredTabsView, styledContent)
	return m.zone.Scan(lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Top, fullView))
}
