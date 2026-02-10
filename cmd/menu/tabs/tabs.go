// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package tabs

import (
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"
	"github.com/terminal-games/terminal-games/cmd/menu/theme"
)

const (
	HeavyHorizontal = "━"
	HeavyLeft       = "╸"
	HeavyRight      = "╺"
)

type Tab struct {
	ID    string
	Title string
}

type Styles struct {
	ActiveTab   lipgloss.Style
	InactiveTab lipgloss.Style
	HoverTab    lipgloss.Style
	ActiveBar   lipgloss.Style
	InactiveBar lipgloss.Style
}

func DefaultStyles() Styles {
	return Styles{
		ActiveTab: lipgloss.NewStyle().
			Background(theme.Primary).
			Foreground(theme.OnPrimary).
			Padding(0, 1),
		InactiveTab: lipgloss.NewStyle().
			Foreground(theme.TextMuted).
			Padding(0, 1),
		HoverTab: lipgloss.NewStyle().
			Background(theme.Surface).
			Padding(0, 1),
		ActiveBar: lipgloss.NewStyle().
			Foreground(theme.Primary),
		InactiveBar: lipgloss.NewStyle().
			Foreground(theme.Line),
	}
}

type Model struct {
	Tabs         []Tab
	Active       int
	Hovered      int
	Styles       Styles
	Duration     time.Duration
	zone         *zone.Manager
	zonePrefix   string
	tabPositions []int
	tabWidths    []int
	startLeft    float64
	startRight   float64
	targetLeft   float64
	targetRight  float64
	animStart    time.Time
	animating    bool
	initialized  bool
}

func New(tabs []Tab, zoneManager *zone.Manager, zonePrefix string) Model {
	m := Model{
		Tabs:       tabs,
		Active:     0,
		Hovered:    -1,
		Styles:     DefaultStyles(),
		Duration:   200 * time.Millisecond,
		zone:       zoneManager,
		zonePrefix: zonePrefix,
	}
	m.calculatePositions()
	return m
}

func NewWithActive(tabs []Tab, zoneManager *zone.Manager, zonePrefix, activeID string) Model {
	m := New(tabs, zoneManager, zonePrefix)
	if len(m.Tabs) == 0 {
		return m
	}
	active := 0
	for i, tab := range m.Tabs {
		if tab.ID == activeID {
			active = i
			break
		}
	}
	m.Active = active
	m.calculatePositions()
	left, right := m.edgesFor(m.Active)
	m.startLeft = left
	m.startRight = right
	m.targetLeft = left
	m.targetRight = right
	m.animating = false
	return m
}

func (m Model) TotalWidth() int {
	total := 0
	for _, w := range m.tabWidths {
		total += w
	}
	return total
}

func (m *Model) calculatePositions() {
	m.tabPositions = make([]int, len(m.Tabs))
	m.tabWidths = make([]int, len(m.Tabs))
	currentPos := 0
	for i, tab := range m.Tabs {
		m.tabPositions[i] = currentPos
		tabContent := m.Styles.ActiveTab.Render(tab.Title)
		m.tabWidths[i] = lipgloss.Width(tabContent)
		currentPos += m.tabWidths[i]
	}
	if !m.initialized && len(m.tabPositions) > m.Active {
		l, r := m.edgesFor(m.Active)
		m.startLeft = l
		m.startRight = r
		m.targetLeft = l
		m.targetRight = r
		m.initialized = true
	}
}

func (m Model) edgesFor(index int) (float64, float64) {
	left := float64(m.tabPositions[index]) + 1
	right := float64(m.tabPositions[index]+m.tabWidths[index]) - 1
	return left, right
}

func (m *Model) startAnim(index int) {
	curL, curR := m.currentEdges()
	m.startLeft = curL
	m.startRight = curR
	m.targetLeft, m.targetRight = m.edgesFor(index)
	m.animStart = time.Now()
	m.animating = true
}

func easeOutCubic(t float64) float64 {
	return 1 - (1-t)*(1-t)*(1-t)
}

func (m Model) animProgress() float64 {
	if !m.animating {
		return 1.0
	}
	p := float64(time.Since(m.animStart)) / float64(m.Duration)
	if p > 1.0 {
		return 1.0
	}
	return p
}

func (m Model) currentEdges() (float64, float64) {
	t := easeOutCubic(m.animProgress())
	l := m.startLeft + (m.targetLeft-m.startLeft)*t
	r := m.startRight + (m.targetRight-m.startRight)*t
	return l, r
}

func (m *Model) SetActive(index int) tea.Cmd {
	if index >= 0 && index < len(m.Tabs) {
		m.Active = index
		m.calculatePositions()
		m.startAnim(index)
		return tickCmd()
	}
	return nil
}

func (m *Model) SetActiveByID(id string) {
	for i, tab := range m.Tabs {
		if tab.ID == id {
			m.SetActive(i)
			return
		}
	}
}

func (m Model) ActiveTab() Tab {
	if m.Active >= 0 && m.Active < len(m.Tabs) {
		return m.Tabs[m.Active]
	}
	return Tab{}
}

type TickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(16*time.Millisecond, func(t time.Time) tea.Msg {
		return TickMsg(t)
	})
}

type TabChangedMsg struct {
	Index int
	Tab   Tab
}

func (m Model) Init() tea.Cmd {
	return nil
}

func (m Model) Update(msg tea.Msg) (Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.MouseMsg:
		if msg.Action == tea.MouseActionMotion {
			m.Hovered = -1
			for i, tab := range m.Tabs {
				if m.zone.Get(m.zonePrefix + tab.ID).InBounds(msg) {
					m.Hovered = i
					break
				}
			}
			return m, nil
		}

		if msg.Action != tea.MouseActionRelease {
			return m, nil
		}
		for i, tab := range m.Tabs {
			if m.zone.Get(m.zonePrefix + tab.ID).InBounds(msg) {
				if m.Active != i {
					m.Active = i
					m.calculatePositions()
					m.startAnim(i)
					return m, tea.Batch(tickCmd(), func() tea.Msg {
						return TabChangedMsg{Index: i, Tab: tab}
					})
				}
				return m, nil
			}
		}

	case TickMsg:
		if !m.animating {
			return m, nil
		}
		if m.animProgress() >= 1.0 {
			m.startLeft = m.targetLeft
			m.startRight = m.targetRight
			m.animating = false
			return m, nil
		}
		return m, tickCmd()
	}

	return m, nil
}

func (m Model) View() string {
	if len(m.Tabs) == 0 {
		return ""
	}

	var tabsRow strings.Builder
	totalWidth := 0
	for i, tab := range m.Tabs {
		var tabContent string
		if i == m.Active {
			tabContent = m.Styles.ActiveTab.Render(tab.Title)
		} else if i == m.Hovered {
			tabContent = m.Styles.HoverTab.Render(tab.Title)
		} else {
			tabContent = m.Styles.InactiveTab.Render(tab.Title)
		}
		totalWidth += lipgloss.Width(tabContent)
		tabsRow.WriteString(m.zone.Mark(m.zonePrefix+tab.ID, tabContent))
	}

	barLine := m.renderBar(totalWidth)

	return lipgloss.JoinVertical(lipgloss.Left,
		tabsRow.String(),
		barLine,
	)
}

func (m Model) renderBar(totalWidth int) string {
	if totalWidth <= 0 {
		return ""
	}

	left, right := m.currentEdges()
	l := int(left + 0.5)
	r := int(right + 0.5)
	if l < 0 {
		l = 0
	}
	if r > totalWidth {
		r = totalWidth
	}

	var bar strings.Builder
	for i := 0; i < totalWidth; i++ {
		switch {
		case i == l-1 && l > 0:
			bar.WriteString(m.Styles.InactiveBar.Render(HeavyLeft))
		case i == r && r < totalWidth:
			bar.WriteString(m.Styles.InactiveBar.Render(HeavyRight))
		case i >= l && i < r:
			bar.WriteString(m.Styles.ActiveBar.Render(HeavyHorizontal))
		default:
			bar.WriteString(m.Styles.InactiveBar.Render(HeavyHorizontal))
		}
	}

	return bar.String()
}
