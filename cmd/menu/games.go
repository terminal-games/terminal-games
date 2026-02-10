// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"
	"github.com/terminal-games/terminal-games/cmd/menu/carousel"
	"github.com/terminal-games/terminal-games/cmd/menu/gamelist"
	"github.com/terminal-games/terminal-games/cmd/menu/theme"
)

type gameItem struct {
	Name        string
	Description string
	Details     string
	Screenshots []carousel.Screenshot
}

type gamesModel struct {
	zone     *zone.Manager
	items    []gameItem
	list     gamelist.Model
	carousel carousel.Model
	styles   gamesDetailsStyles
	termW    int
	termH    int
}

type gamesDetailsStyles struct {
	Title  lipgloss.Style
	Body   lipgloss.Style
	Subtle lipgloss.Style
}

func defaultDetailsStyles() gamesDetailsStyles {
	return gamesDetailsStyles{
		Title:  lipgloss.NewStyle().Bold(true),
		Body:   lipgloss.NewStyle(),
		Subtle: lipgloss.NewStyle().Foreground(theme.TextMuted),
	}
}

func newGamesModel(zoneManager *zone.Manager) gamesModel {
	items := []gameItem{
		{
			Name:        "Terminal Ninja",
			Description: "Slice fast while dodging bombs",
			Details:     "Get the high score",
			Screenshots: []carousel.Screenshot{
				{Content: carousel.PlaceholderScreenshot("TERMINAL NINJA", "203", "52"), Caption: "Title Screen"},
				{Content: carousel.PlaceholderScreenshot("Score: 9001  Combo: x42", "84", "22"), Caption: "Gameplay"},
				{Content: carousel.PlaceholderScreenshot("HIGH SCORES", "220", "58"), Caption: "High Scores"},
			},
		},
		{
			Name:        "Terminal Typer",
			Description: "Test your typing skills",
			Details:     "A singleplayer and multiplayer typing experience.",
			Screenshots: []carousel.Screenshot{
				{Content: carousel.PlaceholderScreenshot("TERMINAL TYPER", "117", "17"), Caption: "Ready to type"},
				{Content: carousel.PlaceholderScreenshot("WPM: 120  Accuracy: 98%", "212", "53"), Caption: "Results"},
			},
		},
		{
			Name:        "Placeholder 1",
			Description: "Placeholder 2",
			Details:     "Placeholder 3",
			Screenshots: []carousel.Screenshot{
				{Content: carousel.PlaceholderScreenshot("PLACEHOLDER 4", "117", "17"), Caption: "Placeholder 5"},
			},
		},
	}

	listItems := make([]gamelist.Item, len(items))
	for i, g := range items {
		listItems[i] = gamelist.Item{
			Name:        g.Name,
			Description: g.Description,
			FilterExtra: g.Details,
		}
	}

	c := carousel.New(zoneManager)
	if len(items) > 0 {
		c.Screenshots = items[0].Screenshots
	}

	return gamesModel{
		zone:     zoneManager,
		items:    items,
		list:     gamelist.New("Official Games", listItems, zoneManager, "games-item-"),
		carousel: c,
		styles:   defaultDetailsStyles(),
	}
}

func (m gamesModel) ShortHelp() []key.Binding {
	return m.list.ShortHelp()
}

func (m gamesModel) FullHelp() [][]key.Binding {
	return m.list.FullHelp()
}

func (m gamesModel) Init() tea.Cmd {
	return m.carousel.Init()
}

func (m gamesModel) Update(msg tea.Msg) (gamesModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.carousel.Modal {
			if msg.Type == tea.KeyEsc {
				m.carousel.Modal = false
			}
			return m, nil
		}
		prevIdx := m.list.SelectedIndex()
		var cmd tea.Cmd
		m.list, cmd = m.list.Update(msg)
		if m.list.SelectedIndex() != prevIdx {
			cmd = tea.Batch(cmd, m.syncCarouselToSelection())
		}
		return m, cmd

	case tea.MouseMsg:
		if m.carousel.Modal {
			result, cmd, handled := m.carousel.HandleMouse(msg)
			m.carousel = result
			if handled {
				return m, cmd
			}
			if msg.Action == tea.MouseActionRelease {
				m.carousel.Modal = false
			}
			return m, nil
		}

		carouselResult, carouselCmd, handled := m.carousel.HandleMouse(msg)
		m.carousel = carouselResult
		if handled {
			return m, carouselCmd
		}

		if msg.Action == tea.MouseActionRelease && m.carousel.BtnClicked(msg) {
			m.carousel.Modal = true
			return m, nil
		}

		prevIdx := m.list.SelectedIndex()
		var listCmd tea.Cmd
		m.list, listCmd = m.list.HandleMouse(msg)
		var cmds []tea.Cmd
		if listCmd != nil {
			cmds = append(cmds, listCmd)
		}
		if m.list.SelectedIndex() != prevIdx {
			cmds = append(cmds, m.syncCarouselToSelection())
		}
		return m, tea.Batch(cmds...)
	}

	var listCmd, carouselCmd tea.Cmd
	m.list, listCmd = m.list.Update(msg)
	m.carousel, carouselCmd = m.carousel.Update(msg)
	return m, tea.Batch(listCmd, carouselCmd)
}

func (m *gamesModel) syncCarouselToSelection() tea.Cmd {
	idx := m.list.SelectedIndex()
	if idx >= 0 {
		m.carousel.Screenshots = m.items[idx].Screenshots
	} else {
		m.carousel.Screenshots = nil
	}
	return m.carousel.Reset()
}

func (m gamesModel) selectedItem() gameItem {
	idx := m.list.SelectedIndex()
	if idx < 0 {
		return gameItem{}
	}
	return m.items[idx]
}

func (m *gamesModel) renderGamesTab(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}

	gap := 2
	leftWidth := int(float64(width) * 0.3)
	if leftWidth < 18 {
		leftWidth = 18
	}
	if leftWidth > width-gap-10 {
		leftWidth = width - gap - 10
	}
	if leftWidth < 0 {
		leftWidth = 0
	}
	rightWidth := width - leftWidth - gap
	if rightWidth < 0 {
		rightWidth = 0
	}

	listView := m.list.View(leftWidth, height)
	detailsView := m.renderGameDetails(rightWidth, height)

	return lipgloss.JoinHorizontal(
		lipgloss.Top,
		lipgloss.NewStyle().Width(leftWidth).Height(height).Render(listView),
		strings.Repeat(" ", gap),
		lipgloss.NewStyle().Width(rightWidth).Height(height).Render(detailsView),
	)
}

func (m *model) renderGamesTab(width, height int) string {
	return m.games.renderGamesTab(width, height)
}

func (m *gamesModel) renderGameDetails(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}

	item := m.selectedItem()
	if item.Name == "" {
		return lipgloss.NewStyle().Width(width).Height(height).
			Render(m.styles.Subtle.Render("No games match the current filter."))
	}

	title := m.styles.Title.Render(item.Name)
	desc := m.styles.Subtle.Render(item.Description)
	body := m.styles.Body.Render(item.Details)

	parts := []string{title, desc, "", body}
	if len(item.Screenshots) > 0 {
		if carousel.CanFitInline(width, height) {
			parts = append(parts, "\n"+m.carousel.View(width))
		} else if carousel.CanFitModal(m.termW, m.termH) {
			parts = append(parts, "", m.carousel.ViewButton())
		}
	}

	content := strings.Join(parts, "\n")
	return lipgloss.NewStyle().Width(width).Height(height).Render(content)
}
