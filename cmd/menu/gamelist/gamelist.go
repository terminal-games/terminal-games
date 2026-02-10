// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package gamelist

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"
	"github.com/terminal-games/terminal-games/cmd/menu/theme"
)

const (
	barVertical    = "┃"
	barTop         = "╻"
	barBottom      = "╹"
	defaultItemGap = 1
)

type Item struct {
	Name        string
	Description string
	FilterExtra string
}

type Styles struct {
	Title        lipgloss.Style
	Count        lipgloss.Style
	Name         lipgloss.Style
	Description  lipgloss.Style
	NameSelected lipgloss.Style
	DescSelected lipgloss.Style
	NameHover    lipgloss.Style
	DescHover    lipgloss.Style
	FilterPrompt lipgloss.Style
	FilterText   lipgloss.Style
	Bar          lipgloss.Style
	BarHover     lipgloss.Style
}

func DefaultStyles() Styles {
	return Styles{
		Title:        lipgloss.NewStyle().Foreground(theme.Primary).Bold(true),
		Count:        lipgloss.NewStyle().Foreground(theme.TextMuted),
		Name:         lipgloss.NewStyle().Bold(true),
		Description:  lipgloss.NewStyle().Foreground(theme.TextMuted),
		NameSelected: lipgloss.NewStyle().Foreground(theme.Primary).Bold(true),
		DescSelected: lipgloss.NewStyle().Foreground(theme.TextMuted),
		NameHover:    lipgloss.NewStyle().Bold(true),
		DescHover:    lipgloss.NewStyle().Foreground(theme.TextMuted),
		FilterPrompt: lipgloss.NewStyle().Foreground(theme.TextMuted),
		FilterText:   lipgloss.NewStyle(),
		Bar:          lipgloss.NewStyle().Foreground(theme.Primary),
		BarHover:     lipgloss.NewStyle(),
	}
}

type KeyMap struct {
	Up     key.Binding
	Down   key.Binding
	Filter key.Binding
	Clear  key.Binding
}

func DefaultKeyMap() KeyMap {
	return KeyMap{
		Up: key.NewBinding(
			key.WithKeys("up", "k"),
			key.WithHelp("↑/k", "up"),
		),
		Down: key.NewBinding(
			key.WithKeys("down", "j"),
			key.WithHelp("↓/j", "down"),
		),
		Filter: key.NewBinding(
			key.WithKeys("/"),
			key.WithHelp("/", "filter"),
		),
		Clear: key.NewBinding(
			key.WithKeys("esc"),
			key.WithHelp("esc", "clear filter"),
		),
	}
}

func (k KeyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Up, k.Down, k.Filter, k.Clear}
}

func (k KeyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{{k.Up, k.Down}, {k.Filter, k.Clear}}
}

type listLayout struct {
	contentWidth int
	itemStarts   []int
	itemHeights  []int
	nameLines    [][]string
	descLines    [][]string
	totalHeight  int
}

type Model struct {
	Title    string
	Items    []Item
	Selected int
	Hovered  int
	Filtered []int
	Styles   Styles
	Keys     KeyMap
	Duration time.Duration

	zone         *zone.Manager
	zonePrefix   string
	filterValue  string
	filterDraft  string
	filtering    bool
	layout       listLayout
	startTop     float64
	startBottom  float64
	targetTop    float64
	targetBottom float64
	animStart    time.Time
	animating    bool
	initialized  bool
}

func New(title string, items []Item, zoneManager *zone.Manager, zonePrefix string) Model {
	filtered := make([]int, 0, len(items))
	for i := range items {
		filtered = append(filtered, i)
	}
	m := Model{
		Title:      title,
		Items:      items,
		Selected:   0,
		Hovered:    -1,
		Filtered:   filtered,
		Styles:     DefaultStyles(),
		Keys:       DefaultKeyMap(),
		Duration:   120 * time.Millisecond,
		zone:       zoneManager,
		zonePrefix: zonePrefix,
	}
	m.syncBarToSelection()
	return m
}

func (m Model) SelectedIndex() int {
	if m.Selected < 0 || m.Selected >= len(m.Filtered) {
		return -1
	}
	return m.Filtered[m.Selected]
}

func (m Model) Filtering() bool {
	return m.filtering
}

func (m Model) ShortHelp() []key.Binding {
	return m.Keys.ShortHelp()
}

func (m Model) FullHelp() [][]key.Binding {
	return m.Keys.FullHelp()
}

type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(16*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m Model) Update(msg tea.Msg) (Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.filtering {
			switch msg.Type {
			case tea.KeyEnter:
				m.filtering = false
				m.filterValue = m.filterDraft
				m.applyFilter()
				return m, nil
			case tea.KeyEsc:
				m.filtering = false
				m.filterDraft = m.filterValue
				return m, nil
			case tea.KeyBackspace, tea.KeyDelete:
				if len(m.filterDraft) > 0 {
					m.filterDraft = m.filterDraft[:len(m.filterDraft)-1]
				}
				return m, nil
			case tea.KeyRunes:
				if len(m.filterDraft) < 32 {
					m.filterDraft += string(msg.Runes)
				}
				return m, nil
			}
			return m, nil
		}

		switch {
		case key.Matches(msg, m.Keys.Up):
			return m.moveSelection(-1)
		case key.Matches(msg, m.Keys.Down):
			return m.moveSelection(1)
		case key.Matches(msg, m.Keys.Filter):
			m.filtering = true
			m.filterDraft = m.filterValue
			return m, nil
		case key.Matches(msg, m.Keys.Clear):
			if m.filterValue != "" {
				m.filterValue = ""
				m.filterDraft = ""
				m.applyFilter()
			}
			return m, nil
		}

	case tickMsg:
		if m.animating {
			if m.animProgress() >= 1.0 {
				m.startTop = m.targetTop
				m.startBottom = m.targetBottom
				m.animating = false
			} else {
				return m, tickCmd()
			}
		}
		return m, nil
	}

	return m, nil
}

func (m Model) HandleMouse(msg tea.MouseMsg) (Model, tea.Cmd) {
	switch msg.Type {
	case tea.MouseWheelUp:
		return m.moveSelection(-1)
	case tea.MouseWheelDown:
		return m.moveSelection(1)
	}

	switch msg.Action {
	case tea.MouseActionMotion:
		m.Hovered = m.indexAtMouse(msg)
		return m, nil
	case tea.MouseActionRelease:
		index := m.indexAtMouse(msg)
		if index >= 0 && index < len(m.Filtered) && index != m.Selected {
			m.Selected = index
			return m, m.startAnim(index)
		}
		return m, nil
	}
	return m, nil
}

func (m *Model) View(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}

	var headerLines []string
	headerLines = append(headerLines, m.Styles.Title.Render(m.Title))

	count := len(m.Filtered)
	total := len(m.Items)
	countLabel := fmt.Sprintf("%d games", count)
	if count != total {
		countLabel = fmt.Sprintf("%d of %d games", count, total)
	}
	headerLines = append(headerLines, m.Styles.Count.Render(countLabel))

	filterLine := ""
	if m.filtering {
		cursor := ""
		if len(m.filterDraft) < 32 {
			cursor = "_"
		}
		filterLine = m.Styles.FilterPrompt.Render("/ " + m.filterDraft + cursor)
	} else if m.filterValue != "" {
		filterLine = m.Styles.FilterText.Render("Filter: " + m.filterValue)
	}
	if filterLine != "" {
		headerLines = append(headerLines, filterLine)
	}
	headerLines = append(headerLines, "")

	listHeight := height - len(headerLines)
	if listHeight < 0 {
		listHeight = 0
	}

	lines := make([]string, 0, height)
	lines = append(lines, headerLines...)
	lines = append(lines, m.renderItems(width, listHeight)...)

	return strings.Join(lines, "\n")
}

func (m Model) moveSelection(delta int) (Model, tea.Cmd) {
	if len(m.Filtered) == 0 {
		return m, nil
	}
	next := m.Selected + delta
	if next < 0 {
		next = 0
	}
	if next >= len(m.Filtered) {
		next = len(m.Filtered) - 1
	}
	if next == m.Selected {
		return m, nil
	}
	m.Selected = next
	return m, m.startAnim(next)
}

func (m *Model) applyFilter() {
	query := strings.TrimSpace(m.filterValue)
	m.Filtered = m.Filtered[:0]
	if query == "" {
		for i := range m.Items {
			m.Filtered = append(m.Filtered, i)
		}
	} else {
		query = strings.ToLower(query)
		for i, item := range m.Items {
			if strings.Contains(strings.ToLower(item.Name), query) ||
				strings.Contains(strings.ToLower(item.Description), query) ||
				strings.Contains(strings.ToLower(item.FilterExtra), query) {
				m.Filtered = append(m.Filtered, i)
			}
		}
	}

	if len(m.Filtered) == 0 {
		m.Selected = -1
		m.Hovered = -1
		m.animating = false
		m.initialized = false
		return
	}

	if m.Selected < 0 || m.Selected >= len(m.Filtered) {
		m.Selected = 0
	}
	m.Hovered = -1
	m.syncBarToSelection()
}

func (m *Model) syncBarToSelection() {
	if m.Selected < 0 || m.Selected >= len(m.Filtered) {
		m.animating = false
		m.initialized = false
		return
	}
	top, bottom := m.itemBarEdges(m.Selected)
	m.startTop = top
	m.startBottom = bottom
	m.targetTop = top
	m.targetBottom = bottom
	m.animating = false
	m.initialized = true
}

func (m *Model) startAnim(index int) tea.Cmd {
	if index < 0 || index >= len(m.Filtered) {
		return nil
	}
	curTop, curBottom := m.currentBarEdges()
	if !m.initialized {
		curTop, curBottom = m.itemBarEdges(index)
	}
	m.startTop = curTop
	m.startBottom = curBottom
	m.targetTop, m.targetBottom = m.itemBarEdges(index)
	m.animStart = time.Now()
	m.animating = true
	m.initialized = true
	return tickCmd()
}

func (m Model) itemBarEdges(index int) (float64, float64) {
	if index >= 0 && index < len(m.layout.itemStarts) {
		height := m.layout.itemHeights[index]
		if height <= 0 {
			height = 1
		}
		top := float64(m.layout.itemStarts[index])
		bottom := top + float64(height)
		return top, bottom
	}
	blockHeight := 2 + defaultItemGap
	top := float64(index * blockHeight)
	bottom := top + 2
	return top, bottom
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

func (m Model) currentBarEdges() (float64, float64) {
	t := easeOutCubic(m.animProgress())
	top := m.startTop + (m.targetTop-m.startTop)*t
	bottom := m.startBottom + (m.targetBottom-m.startBottom)*t
	return top, bottom
}

func (m *Model) renderItems(width, height int) []string {
	if height <= 0 {
		return nil
	}
	barWidth := 1
	contentWidth := width - barWidth - 1
	if contentWidth < 0 {
		contentWidth = 0
	}

	m.layout = m.buildLayout(contentWidth)
	totalHeight := m.layout.totalHeight
	if !m.animating && m.Selected >= 0 {
		m.syncBarToSelection()
	}

	offset := 0
	if totalHeight > height && m.Selected >= 0 && m.Selected < len(m.layout.itemStarts) {
		selStart := m.layout.itemStarts[m.Selected]
		selHeight := m.layout.itemHeights[m.Selected]
		center := selStart + selHeight/2
		offset = center - height/2
		maxOffset := totalHeight - height
		if offset < 0 {
			offset = 0
		}
		if offset > maxOffset {
			offset = maxOffset
		}
	}

	barTop, barBottom := m.currentBarEdges()
	showBar := m.Selected >= 0 && m.initialized

	lines := make([]string, 0, height)
	itemIndex := 0
	itemEnd := 0
	itemBlockEnd := 0
	if len(m.layout.itemStarts) > 0 {
		itemEnd = m.layout.itemStarts[0] + m.layout.itemHeights[0]
		itemBlockEnd = itemEnd + defaultItemGap
		for itemIndex < len(m.layout.itemStarts)-1 && offset >= itemBlockEnd {
			itemIndex++
			itemEnd = m.layout.itemStarts[itemIndex] + m.layout.itemHeights[itemIndex]
			itemBlockEnd = itemEnd + defaultItemGap
		}
	}
	for row := 0; row < height; row++ {
		lineIndex := offset + row
		var line string
		if lineIndex >= totalHeight || len(m.Filtered) == 0 {
			line = strings.Repeat(" ", width)
			lines = append(lines, line)
			continue
		}
		for itemIndex < len(m.layout.itemStarts)-1 && lineIndex >= itemBlockEnd {
			itemIndex++
			itemEnd = m.layout.itemStarts[itemIndex] + m.layout.itemHeights[itemIndex]
			itemBlockEnd = itemEnd + defaultItemGap
		}
		lineInItem := lineIndex - m.layout.itemStarts[itemIndex]

		bar := " "
		isSelected := itemIndex == m.Selected
		isHovered := itemIndex == m.Hovered
		itemHeight := m.layout.itemHeights[itemIndex]
		if isHovered && lineInItem >= 0 && lineInItem < itemHeight {
			bar = m.Styles.BarHover.Render(barVertical)
		}
		if showBar {
			if ch := barCharForRow(lineIndex, barTop, barBottom); ch != "" {
				bar = m.Styles.Bar.Render(ch)
			}
		}

		content := ""
		if itemIndex >= 0 && itemIndex < len(m.Filtered) && lineInItem >= 0 && lineInItem < itemHeight {
			nameLines := m.layout.nameLines[itemIndex]
			descLines := m.layout.descLines[itemIndex]
			if lineInItem < len(nameLines) {
				content = nameLines[lineInItem]
				switch {
				case isSelected:
					content = m.Styles.NameSelected.Render(content)
				case isHovered:
					content = m.Styles.NameHover.Render(content)
				default:
					content = m.Styles.Name.Render(content)
				}
			} else {
				descIndex := lineInItem - len(nameLines)
				if descIndex >= 0 && descIndex < len(descLines) {
					content = descLines[descIndex]
				}
				switch {
				case isSelected:
					content = m.Styles.DescSelected.Render(content)
				case isHovered:
					content = m.Styles.DescHover.Render(content)
				default:
					content = m.Styles.Description.Render(content)
				}
			}
		}

		content = lipgloss.NewStyle().Width(contentWidth).Render(content)
		line = lipgloss.NewStyle().Width(width).Render(bar + " " + content)
		if m.zone != nil && lineInItem >= 0 && lineInItem < itemHeight {
			zoneID := fmt.Sprintf("%s%d-%d", m.zonePrefix, itemIndex, lineInItem)
			line = m.zone.Mark(zoneID, line)
		}
		lines = append(lines, line)
	}

	return lines
}

func (m Model) buildLayout(contentWidth int) listLayout {
	count := len(m.Filtered)
	layout := listLayout{
		contentWidth: contentWidth,
		itemStarts:   make([]int, count),
		itemHeights:  make([]int, count),
		nameLines:    make([][]string, count),
		descLines:    make([][]string, count),
	}

	total := 0
	for i, idx := range m.Filtered {
		nameLines := wrapLines(m.Items[idx].Name, contentWidth)
		descLines := wrapLines(m.Items[idx].Description, contentWidth)
		layout.nameLines[i] = nameLines
		layout.descLines[i] = descLines

		height := len(nameLines) + len(descLines)
		if height <= 0 {
			height = 1
		}
		layout.itemStarts[i] = total
		layout.itemHeights[i] = height
		total += height + defaultItemGap
	}

	if count > 0 {
		total -= defaultItemGap
	}
	layout.totalHeight = total
	return layout
}

func (m Model) indexAtMouse(msg tea.MouseMsg) int {
	if m.zone == nil {
		return -1
	}
	for i := 0; i < len(m.Filtered); i++ {
		height := 2
		if i < len(m.layout.itemHeights) && m.layout.itemHeights[i] > 0 {
			height = m.layout.itemHeights[i]
		}
		for line := 0; line < height; line++ {
			zoneID := fmt.Sprintf("%s%d-%d", m.zonePrefix, i, line)
			if m.zone.Get(zoneID).InBounds(msg) {
				return i
			}
		}
	}
	return -1
}

func barCharForRow(row int, top, bottom float64) string {
	fRow := float64(row)
	if fRow+1 <= top || fRow >= bottom {
		return ""
	}
	topPartial := fRow < top
	bottomPartial := fRow+1 > bottom
	if topPartial && bottomPartial {
		return barVertical
	}
	if topPartial && top-fRow >= 0.5 {
		return barTop
	}
	if bottomPartial && bottom-fRow <= 0.5 {
		return barBottom
	}
	return barVertical
}

func easeOutCubic(t float64) float64 {
	return 1 - (1-t)*(1-t)*(1-t)
}

func wrapLines(text string, width int) []string {
	if text == "" {
		return nil
	}
	if width <= 0 {
		return []string{""}
	}
	rendered := lipgloss.NewStyle().Width(width).Render(text)
	lines := strings.Split(rendered, "\n")
	if len(lines) == 0 {
		return []string{""}
	}
	return lines
}
