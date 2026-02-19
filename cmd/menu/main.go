// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	_ "embed"
	"log"
	"strings"
	"time"
	_ "unsafe"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"

	"github.com/terminal-games/terminal-games/cmd/menu/tabs"
	"github.com/terminal-games/terminal-games/cmd/menu/theme"
	"github.com/terminal-games/terminal-games/pkg/bubblewrap"
	_ "github.com/terminal-games/terminal-games/pkg/net/http"
)

//go:embed "terminal-games.json"
var terminalGamesManifestJSON []byte

func init() {
	if len(terminalGamesManifestJSON) == 0 {
		panic("missing terminal-games.json")
	}
}

const (
	maxWidth          = 120
	minContentHeight  = 5
	minViewportWidth  = 40
	minViewportHeight = 11
	helpPaddingX      = 2
)

type model struct {
	w           int
	h           int
	zone        *zone.Manager
	started     bool
	localeReady bool
	showLoading bool
	tabs        tabs.Model
	localizer   localizer
	contentArea lipgloss.Style
	barStyle    lipgloss.Style
	titleStyle  lipgloss.Style
	keys        keyMap
	help        help.Model
	games       gamesModel
	profile     profileModel
}

type helpKeyMap interface {
	ShortHelp() []key.Binding
	FullHelp() [][]key.Binding
}

type startupLoadingDelayElapsedMsg struct{}

type helpBindings struct {
	short []key.Binding
}

func (h helpBindings) ShortHelp() []key.Binding {
	return h.short
}

func (h helpBindings) FullHelp() [][]key.Binding {
	return [][]key.Binding{h.short}
}

type keyMap struct {
	Quit    key.Binding
	NextTab key.Binding
	PrevTab key.Binding
}

type contentLayout struct {
	height      int
	paddingY    int
	paddingX    int
	innerWidth  int
	innerHeight int
}

func newKeyMap(localizer localizer) keyMap {
	return keyMap{
		Quit: key.NewBinding(
			key.WithKeys("q", "ctrl+c"),
			key.WithHelp("q", localizer.Text(textHelpQuit)),
		),
		NextTab: key.NewBinding(
			key.WithKeys("tab"),
			key.WithHelp("tab", localizer.Text(textHelpNextTab)),
		),
		PrevTab: key.NewBinding(
			key.WithKeys("shift+tab"),
			key.WithHelp("shift+tab", localizer.Text(textHelpPrevTab)),
		),
	}
}

func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.NextTab, k.PrevTab, k.Quit}
}

func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{{k.NextTab, k.PrevTab}, {k.Quit}}
}

func (m model) contextualKeyMap() helpKeyMap {
	switch m.tabs.ActiveTab().ID {
	case "games":
		return m.games
	case "profile":
		return m.profile
	}
	return nil
}

func (m model) inputCaptured() bool {
	switch m.tabs.ActiveTab().ID {
	case "games":
		return m.games.Capturing()
	case "profile":
		return m.profile.Capturing()
	default:
		return false
	}
}

func (m model) globalHelpKeyMap() helpKeyMap {
	if m.inputCaptured() {
		return nil
	}
	return helpBindings{
		short: []key.Binding{m.keys.NextTab, m.keys.PrevTab, m.keys.Quit},
	}
}

func main() {
	lipgloss.SetDefaultRenderer(bubblewrap.MakeRenderer())

	zoneManager := zone.New()

	menuModel := &model{
		zone:        zoneManager,
		localizer:   newLocalizer(),
		contentArea: lipgloss.NewStyle().Padding(1, 2),
		barStyle:    lipgloss.NewStyle().Foreground(theme.Line),
		titleStyle:  lipgloss.NewStyle().Bold(true),
		keys:        newKeyMap(newLocalizer()),
		help:        help.New(),
		games:       newGamesModel(zoneManager),
		profile:     newProfileModel(zoneManager),
	}
	menuModel.applyMenuLocalization()

	p := bubblewrap.NewProgram(menuModel, tea.WithAltScreen(), tea.WithMouseAllMotion())

	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}

func (m *model) Init() tea.Cmd {
	return tea.Batch(
		m.tabs.Init(),
		m.games.Init(),
		m.profile.Init(),
		startupLoadingDelayCmd(),
	)
}

func (m *model) setActiveTab(index int) tea.Cmd {
	cmd := m.tabs.SetActive(index)
	if index < 0 || index >= len(m.tabs.Tabs) {
		return cmd
	}
	tab := m.tabs.Tabs[index]
	return tea.Batch(cmd, func() tea.Msg {
		return tabs.TabChangedMsg{Index: index, Tab: tab}
	})
}

func (m *model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := message.(type) {
	case tea.KeyMsg:
		switch {
		case key.Matches(msg, m.keys.Quit):
			if msg.String() == "q" && m.inputCaptured() {
				break
			}
			return m, tea.Quit
		case key.Matches(msg, m.keys.NextTab):
			if m.inputCaptured() {
				return m, nil
			}
			next := (m.tabs.Active + 1) % len(m.tabs.Tabs)
			return m, m.setActiveTab(next)
		case key.Matches(msg, m.keys.PrevTab):
			if m.inputCaptured() {
				return m, nil
			}
			prev := m.tabs.Active - 1
			if prev < 0 {
				prev = len(m.tabs.Tabs) - 1
			}
			return m, m.setActiveTab(prev)
		default:
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
			return m, cmd
		}
	case tea.WindowSizeMsg:
		m.w = msg.Width
		m.h = msg.Height
	case localizationChangedMsg:
		if m.localizer.SetPreferred(msg.preferred) {
			m.applyMenuLocalization()
		}
		m.localeReady = true
	case startupLoadingDelayElapsedMsg:
		m.showLoading = true
	}

	var cmd tea.Cmd

	m.games, cmd = m.games.Update(message)
	cmds = append(cmds, cmd)

	m.profile, cmd = m.profile.Update(message)
	cmds = append(cmds, cmd)

	m.tabs, cmd = m.tabs.Update(message)
	cmds = append(cmds, cmd)

	if !m.started && m.startupReady() {
		m.started = true
	}

	return m, tea.Batch(cmds...)
}

func (m *model) View() string {
	if !m.started {
		if !m.showLoading {
			return ""
		}
		return m.renderLoadingView()
	}

	if m.games.carousel.Modal {
		item := m.games.selectedItem()
		return m.zone.Scan(m.games.carousel.ViewModal(item.Name, m.w, m.h))
	}

	tabsView := m.tabs.View()
	tabsWidth := m.tabs.TotalWidth()
	title := m.titleStyle.Render(" " + m.localizer.Text(textHeaderTitle))
	titleWidth := lipgloss.Width(title)

	viewportWidth := min(m.w, maxWidth)
	if !m.canRenderViewport(viewportWidth, titleWidth, tabsWidth) {
		return lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Center, m.localizer.Text(textWindowTooSmall))
	}

	centeredTabsView := m.renderCenteredTabsView(viewportWidth, tabsView, tabsWidth, title, titleWidth)
	helpView := m.renderHelpView(viewportWidth)
	layout := m.computeContentLayout(viewportWidth, lipgloss.Height(centeredTabsView), lipgloss.Height(helpView))
	if m.tabs.ActiveTab().ID == "games" && m.games.gameDetailsNeedsMoreHeight(layout.innerWidth, layout.innerHeight) {
		helpView = ""
		layout = m.computeContentLayout(viewportWidth, lipgloss.Height(centeredTabsView), 0)
	}

	content := m.renderActiveTab(layout.innerWidth, layout.innerHeight)

	styledContent := m.contentArea.
		Padding(layout.paddingY, layout.paddingX).
		Width(viewportWidth).
		Height(layout.height).
		Render(content)

	fullView := lipgloss.JoinVertical(lipgloss.Left, centeredTabsView, styledContent, helpView)
	return m.zone.Scan(lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Top, fullView))
}

func (m *model) canRenderViewport(viewportWidth, titleWidth, tabsWidth int) bool {
	requiredWidth := max(minViewportWidth, titleWidth+tabsWidth+1)
	return viewportWidth >= requiredWidth && m.h >= minViewportHeight
}

func (m *model) renderCenteredTabsView(viewportWidth int, tabsView string, tabsWidth int, title string, titleWidth int) string {
	paddingTotal := max(0, viewportWidth-tabsWidth)
	leftPad := max(paddingTotal/2, titleWidth+1)
	rightPad := max(0, viewportWidth-leftPad-tabsWidth)

	tabLines := strings.Split(tabsView, "\n")
	lines := []string{strings.Repeat(" ", viewportWidth)}
	if len(tabLines) > 0 {
		line := title + strings.Repeat(" ", leftPad-titleWidth) + tabLines[0] + strings.Repeat(" ", rightPad)
		lines = append(lines, lipgloss.NewStyle().Width(viewportWidth).Render(line))
	}
	if len(tabLines) > 1 {
		barLine := m.barStyle.Render(strings.Repeat(tabs.HeavyHorizontal, leftPad)) + tabLines[1] +
			m.barStyle.Render(strings.Repeat(tabs.HeavyHorizontal, rightPad))
		lines = append(lines, lipgloss.NewStyle().Width(viewportWidth).Render(barLine))
	}
	return lipgloss.NewStyle().Width(viewportWidth).Render(strings.Join(lines, "\n"))
}

func (m *model) renderHelpView(viewportWidth int) string {
	innerWidth := max(0, viewportWidth-2*helpPaddingX)
	m.help.Width = innerWidth

	helpLines := make([]string, 0, 2)
	if keyMap := m.contextualKeyMap(); keyMap != nil {
		helpLines = append(helpLines, lipgloss.NewStyle().Width(innerWidth).Render(m.help.View(keyMap)))
	}
	if keyMap := m.globalHelpKeyMap(); keyMap != nil {
		helpLines = append(helpLines, lipgloss.NewStyle().Width(innerWidth).Render(m.help.View(keyMap)))
	}
	return lipgloss.NewStyle().Padding(1, helpPaddingX).Render(strings.Join(helpLines, "\n"))
}

func (m *model) computeContentLayout(viewportWidth, headerHeight, helpHeight int) contentLayout {
	height := max(0, m.h-headerHeight-helpHeight)
	paddingY := 1
	paddingX := 2
	if height <= minContentHeight+2 {
		paddingY = 0
		paddingX = 1
	}

	innerWidth := max(0, viewportWidth-2*paddingX)
	innerHeight := max(0, height-2*paddingY)
	return contentLayout{
		height:      height,
		paddingY:    paddingY,
		paddingX:    paddingX,
		innerWidth:  innerWidth,
		innerHeight: innerHeight,
	}
}

func (m *model) renderActiveTab(width, height int) string {
	switch m.tabs.ActiveTab().ID {
	case "games":
		return m.renderGamesTab(width, height)
	case "profile":
		return m.renderProfileTab(width, height)
	case "about":
		return m.renderAboutTab(width, height)
	default:
		return m.localizer.Text(textUnknownTab)
	}
}

func (m model) startupReady() bool {
	if !m.games.loaded || !m.profile.loaded {
		return false
	}
	if m.profile.loadErr != nil {
		return true
	}
	return m.localeReady
}

func (m model) renderLoadingView() string {
	loading := m.localizer.Text(textProfileLoading)
	if m.w <= 0 || m.h <= 0 {
		return loading
	}
	return lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Center, loading)
}

func startupLoadingDelayCmd() tea.Cmd {
	return tea.Tick(200*time.Millisecond, func(time.Time) tea.Msg {
		return startupLoadingDelayElapsedMsg{}
	})
}

func (m *model) applyMenuLocalization() {
	activeID := m.tabs.ActiveTab().ID
	m.keys = newKeyMap(m.localizer)
	m.tabs = tabs.NewWithActive([]tabs.Tab{
		{ID: "games", Title: m.localizer.Text(textTabGames)},
		{ID: "profile", Title: m.localizer.Text(textTabProfile)},
		{ID: "about", Title: m.localizer.Text(textTabAbout)},
	}, m.zone, "menu-tab-", activeID)
}
