// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	zone "github.com/lrstanley/bubblezone"
)

type gamesModel struct {
	keys         gamesKeyMap
	zone         *zone.Manager
	items        []gameItem
	filtered     []int
	selected     int
	hovered      int
	filterValue  string
	filterDraft  string
	filtering    bool
	styles       gamesStyles
	duration     time.Duration
	layout       gamesListLayout
	startTop     float64
	startBottom  float64
	targetTop    float64
	targetBottom float64
	animStart    time.Time
	animating    bool
	initialized  bool

	carouselScrollX     float64
	carouselVelocity    float64
	carouselDragging    bool
	carouselDragStartMX int
	carouselDragStartSX float64
	carouselLastMX      int
	carouselLastMT      time.Time
	carouselMomentum    bool
	carouselAnimating   bool
	carouselAnimStartX  float64
	carouselAnimTargetX float64
	carouselAnimStart   time.Time
	carouselAutoGen     int
}

type gamesKeyMap struct {
	Up     key.Binding
	Down   key.Binding
	Filter key.Binding
	Clear  key.Binding
}

type gameItem struct {
	Name        string
	Description string
	Details     string
	Screenshots []gameScreenshot
}

type gameScreenshot struct {
	Content string
	Caption string
}

type gamesStyles struct {
	Title         lipgloss.Style
	Count         lipgloss.Style
	Name          lipgloss.Style
	Description   lipgloss.Style
	NameSelected  lipgloss.Style
	DescSelected  lipgloss.Style
	NameHover     lipgloss.Style
	DescHover     lipgloss.Style
	FilterPrompt  lipgloss.Style
	FilterText    lipgloss.Style
	Bar           lipgloss.Style
	BarHover      lipgloss.Style
	DetailsTitle  lipgloss.Style
	DetailsBody   lipgloss.Style
	DetailsSubtle lipgloss.Style
	DotActive     lipgloss.Style
	DotInactive   lipgloss.Style
	Caption       lipgloss.Style
}

type gamesListLayout struct {
	contentWidth int
	itemStarts   []int
	itemHeights  []int
	nameLines    [][]string
	descLines    [][]string
	totalHeight  int
}

const (
	gamesBarVertical = "┃"
	gamesBarTop      = "╻"
	gamesBarBottom   = "╹"
	gamesItemHeight  = 2
	gamesItemGap     = 1

	screenshotWidth      = 80
	screenshotHeight     = 24
	carouselAutoInterval = 10 * time.Second
	carouselAnimDuration = 400 * time.Millisecond
	carouselSnapDuration = 150 * time.Millisecond
	carouselDragElastic  = 0.3
	carouselFrameZoneID  = "carousel-frame"
)

func defaultGamesStyles() gamesStyles {
	return gamesStyles{
		Title:         lipgloss.NewStyle().Foreground(lipgloss.Color("#2bb673")).Bold(true),
		Count:         lipgloss.NewStyle().Foreground(lipgloss.Color("#666666")),
		Name:          lipgloss.NewStyle().Foreground(lipgloss.Color("#dddddd")).Bold(true),
		Description:   lipgloss.NewStyle().Foreground(lipgloss.Color("#777777")),
		NameSelected:  lipgloss.NewStyle().Foreground(lipgloss.Color("#d766ff")).Bold(true),
		DescSelected:  lipgloss.NewStyle().Foreground(lipgloss.Color("#b777ff")),
		NameHover:     lipgloss.NewStyle().Foreground(lipgloss.Color("#ffffff")).Bold(true),
		DescHover:     lipgloss.NewStyle().Foreground(lipgloss.Color("#aaaaaa")),
		FilterPrompt:  lipgloss.NewStyle().Foreground(lipgloss.Color("#888888")),
		FilterText:    lipgloss.NewStyle().Foreground(lipgloss.Color("#bbbbbb")),
		Bar:           lipgloss.NewStyle().Foreground(lipgloss.Color("#d766ff")),
		BarHover:      lipgloss.NewStyle().Foreground(lipgloss.Color("#ffffff")),
		DetailsTitle:  lipgloss.NewStyle().Foreground(lipgloss.Color("#ffffff")).Bold(true),
		DetailsBody:   lipgloss.NewStyle().Foreground(lipgloss.Color("#cccccc")),
		DetailsSubtle: lipgloss.NewStyle().Foreground(lipgloss.Color("#888888")),
		DotActive:     lipgloss.NewStyle().Foreground(lipgloss.Color("#d766ff")),
		DotInactive:   lipgloss.NewStyle().Foreground(lipgloss.Color("#555555")),
		Caption:       lipgloss.NewStyle().Foreground(lipgloss.Color("#999999")),
	}
}

func newGamesModel(zoneManager *zone.Manager) gamesModel {
	items := []gameItem{
		{
			Name:        "Terminal Ninja",
			Description: "Slice fast while dodging bombs",
			Details:     "Get the high score",
			Screenshots: []gameScreenshot{
				{Content: placeholderScreenshot("TERMINAL NINJA", "#ff6644", "#1a0a08"), Caption: "Title Screen"},
				{Content: placeholderScreenshot("Score: 9001  Combo: x42", "#44ff66", "#081a08"), Caption: "Gameplay"},
				{Content: placeholderScreenshot("HIGH SCORES", "#ffcc00", "#1a1a08"), Caption: "High Scores"},
			},
		},
		{
			Name:        "Terminal Typer",
			Description: "Test your typing skills",
			Details:     "A singleplayer and multiplayer typing experience.",
			Screenshots: []gameScreenshot{
				{Content: placeholderScreenshot("TERMINAL TYPER", "#66aaff", "#080e1a"), Caption: "Ready to type"},
				{Content: placeholderScreenshot("WPM: 120  Accuracy: 98%", "#ff66aa", "#1a0812"), Caption: "Results"},
			},
		},
		{
			Name:        "Placeholder 1",
			Description: "Placeholder 2",
			Details:     "Placeholder 3",
			Screenshots: []gameScreenshot{
				{Content: placeholderScreenshot("PLACEHOLDER 4", "#66aaff", "#080e1a"), Caption: "Placeholder 5"},
			},
		},
	}

	filtered := make([]int, 0, len(items))
	for i := range items {
		filtered = append(filtered, i)
	}

	model := gamesModel{
		keys:     newGamesKeyMap(),
		zone:     zoneManager,
		items:    items,
		filtered: filtered,
		selected: 0,
		hovered:  -1,
		styles:   defaultGamesStyles(),
		duration: 120 * time.Millisecond,
	}
	model.syncBarToSelection()
	return model
}

func newGamesKeyMap() gamesKeyMap {
	return gamesKeyMap{
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

func (m gamesModel) ShortHelp() []key.Binding {
	return m.keys.ShortHelp()
}

func (m gamesModel) FullHelp() [][]key.Binding {
	return m.keys.FullHelp()
}

func (k gamesKeyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Up, k.Down, k.Filter, k.Clear}
}

func (k gamesKeyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{{k.Up, k.Down}, {k.Filter, k.Clear}}
}

func (m gamesModel) Update(msg tea.Msg) (gamesModel, tea.Cmd) {
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
		case key.Matches(msg, m.keys.Up):
			return m.moveSelection(-1)
		case key.Matches(msg, m.keys.Down):
			return m.moveSelection(1)
		case key.Matches(msg, m.keys.Filter):
			m.filtering = true
			m.filterDraft = m.filterValue
			return m, nil
		case key.Matches(msg, m.keys.Clear):
			if m.filterValue != "" {
				m.filterValue = ""
				m.filterDraft = ""
				m.applyFilter()
			}
			return m, nil
		}

	case tea.MouseMsg:
		switch msg.Type {
		case tea.MouseWheelUp:
			return m.moveSelection(-1)
		case tea.MouseWheelDown:
			return m.moveSelection(1)
		}
		switch msg.Action {
		case tea.MouseActionPress:
			if m.zone != nil && m.zone.Get(carouselFrameZoneID).InBounds(msg) {
				m.carouselDragging = true
				m.carouselDragStartMX = msg.X
				m.carouselDragStartSX = m.carouselScrollX
				m.carouselLastMX = msg.X
				m.carouselLastMT = time.Now()
				m.carouselVelocity = 0
				m.carouselMomentum = false
				m.carouselAnimating = false
				m.carouselAutoGen++
			}
			return m, nil
		case tea.MouseActionMotion:
			if m.carouselDragging {
				n := len(m.selectedScreenshots())
				maxX := m.carouselMaxScrollX(n)
				rawX := m.carouselDragStartSX + float64(m.carouselDragStartMX-msg.X)
				m.carouselScrollX = clampWithResistance(rawX, 0, maxX, carouselDragElastic)

				dt := time.Since(m.carouselLastMT).Seconds()
				if dt > 0 && dt < 0.1 {
					instantV := float64(m.carouselLastMX-msg.X) / dt
					m.carouselVelocity = 0.6*m.carouselVelocity + 0.4*instantV
				}
				m.carouselLastMX = msg.X
				m.carouselLastMT = time.Now()
				return m, nil
			}
			m.hovered = m.indexAtMouse(msg)
			return m, nil
		case tea.MouseActionRelease:
			if m.carouselDragging {
				m.carouselDragging = false
				n := len(m.selectedScreenshots())

				if time.Since(m.carouselLastMT) > 80*time.Millisecond {
					m.carouselVelocity = 0
				}

				maxX := m.carouselMaxScrollX(n)
				projected := math.Max(0, math.Min(m.carouselScrollX+m.carouselVelocity*0.15, maxX))
				targetPage := int(math.Round(projected / screenshotWidth))
				if targetPage < 0 {
					targetPage = 0
				}
				if targetPage >= n {
					targetPage = n - 1
				}
				targetX := float64(targetPage * screenshotWidth)
				m.carouselAnimStartX = m.carouselScrollX
				m.carouselAnimTargetX = targetX
				m.carouselAnimStart = time.Now()
				m.carouselAnimating = true
				m.carouselAutoGen++
				return m, gamesTickCmd()
			}
			if m.zone != nil {
				screenshots := m.selectedScreenshots()
				n := len(screenshots)
				currentPage := m.carouselCurrentPage(n)
				for i := range screenshots {
					if m.zone.Get(m.carouselDotZoneID(i)).InBounds(msg) {
						if i != currentPage {
							return m.goToCarouselPage(i)
						}
						return m, nil
					}
				}
			}
			index := m.indexAtMouse(msg)
			if index >= 0 && index < len(m.filtered) && index != m.selected {
				m.selected = index
				cmd := m.resetCarousel()
				return m, tea.Batch(m.startAnim(index), cmd)
			}
			return m, nil
		}

	case gamesTickMsg:
		var cmds []tea.Cmd
		needsTick := false

		if m.animating {
			if m.animProgress() >= 1.0 {
				m.startTop = m.targetTop
				m.startBottom = m.targetBottom
				m.animating = false
			} else {
				needsTick = true
			}
		}

		if m.carouselAnimating {
			p := m.carouselAnimProgress()
			if p >= 1.0 {
				m.carouselScrollX = m.carouselAnimTargetX
				m.carouselAnimating = false
				if n := len(m.selectedScreenshots()); n > 1 {
					m.carouselAutoGen++
					cmds = append(cmds, carouselAutoAdvanceCmd(m.carouselAutoGen))
				}
			} else {
				t := easeOutCubic(p)
				m.carouselScrollX = m.carouselAnimStartX + (m.carouselAnimTargetX-m.carouselAnimStartX)*t
				needsTick = true
			}
		}

		if needsTick {
			cmds = append(cmds, gamesTickCmd())
		}
		if len(cmds) > 0 {
			return m, tea.Batch(cmds...)
		}
		return m, nil

	case carouselAutoAdvanceMsg:
		if msg.gen != m.carouselAutoGen {
			return m, nil
		}
		if m.carouselDragging || m.carouselMomentum {
			return m, nil
		}
		screenshots := m.selectedScreenshots()
		n := len(screenshots)
		if n <= 1 {
			return m, nil
		}
		currentPage := m.carouselCurrentPage(n)
		nextPage := (currentPage + 1) % n
		return m.goToCarouselPage(nextPage)
	}
	return m, nil
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

	listView := m.renderGamesList(leftWidth, height)
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

type gamesTickMsg time.Time

func gamesTickCmd() tea.Cmd {
	return tea.Tick(16*time.Millisecond, func(t time.Time) tea.Msg {
		return gamesTickMsg(t)
	})
}

type carouselAutoAdvanceMsg struct{ gen int }

func carouselAutoAdvanceCmd(gen int) tea.Cmd {
	return tea.Tick(carouselAutoInterval, func(time.Time) tea.Msg {
		return carouselAutoAdvanceMsg{gen}
	})
}

func easeOutCubic(t float64) float64 {
	return 1 - (1-t)*(1-t)*(1-t)
}

func (m gamesModel) moveSelection(delta int) (gamesModel, tea.Cmd) {
	if len(m.filtered) == 0 {
		return m, nil
	}
	next := m.selected + delta
	if next < 0 {
		next = 0
	}
	if next >= len(m.filtered) {
		next = len(m.filtered) - 1
	}
	if next == m.selected {
		return m, nil
	}
	m.selected = next
	cmd := m.resetCarousel()
	return m, tea.Batch(m.startAnim(next), cmd)
}

func (m *gamesModel) applyFilter() {
	query := strings.TrimSpace(m.filterValue)
	m.filtered = m.filtered[:0]
	if query == "" {
		for i := range m.items {
			m.filtered = append(m.filtered, i)
		}
	} else {
		query = strings.ToLower(query)
		for i, item := range m.items {
			if strings.Contains(strings.ToLower(item.Name), query) ||
				strings.Contains(strings.ToLower(item.Description), query) ||
				strings.Contains(strings.ToLower(item.Details), query) {
				m.filtered = append(m.filtered, i)
			}
		}
	}

	if len(m.filtered) == 0 {
		m.selected = -1
		m.hovered = -1
		m.animating = false
		m.initialized = false
		m.carouselScrollX = 0
		m.carouselVelocity = 0
		m.carouselDragging = false
		m.carouselMomentum = false
		m.carouselAnimating = false
		return
	}

	if m.selected < 0 || m.selected >= len(m.filtered) {
		m.selected = 0
	}
	m.hovered = -1
	m.carouselScrollX = 0
	m.carouselVelocity = 0
	m.carouselDragging = false
	m.carouselMomentum = false
	m.carouselAnimating = false
	m.syncBarToSelection()
}

func (m *gamesModel) syncBarToSelection() {
	if m.selected < 0 || m.selected >= len(m.filtered) {
		m.animating = false
		m.initialized = false
		return
	}
	top, bottom := m.itemBarEdges(m.selected)
	m.startTop = top
	m.startBottom = bottom
	m.targetTop = top
	m.targetBottom = bottom
	m.animating = false
	m.initialized = true
}

func (m *gamesModel) startAnim(index int) tea.Cmd {
	if index < 0 || index >= len(m.filtered) {
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
	return gamesTickCmd()
}

func (m gamesModel) itemBarEdges(index int) (float64, float64) {
	if index >= 0 && index < len(m.layout.itemStarts) {
		height := m.layout.itemHeights[index]
		if height <= 0 {
			height = 1
		}
		top := float64(m.layout.itemStarts[index])
		bottom := top + float64(height)
		return top, bottom
	}
	blockHeight := gamesItemHeight + gamesItemGap
	top := float64(index * blockHeight)
	bottom := top + float64(gamesItemHeight)
	return top, bottom
}

func (m gamesModel) animProgress() float64 {
	if !m.animating {
		return 1.0
	}
	p := float64(time.Since(m.animStart)) / float64(m.duration)
	if p > 1.0 {
		return 1.0
	}
	return p
}

func (m gamesModel) currentBarEdges() (float64, float64) {
	t := easeOutCubic(m.animProgress())
	top := m.startTop + (m.targetTop-m.startTop)*t
	bottom := m.startBottom + (m.targetBottom-m.startBottom)*t
	return top, bottom
}

func barCharForRow(row int, top, bottom float64) string {
	fRow := float64(row)
	if fRow+1 <= top || fRow >= bottom {
		return ""
	}
	topPartial := fRow < top
	bottomPartial := fRow+1 > bottom
	if topPartial && bottomPartial {
		return gamesBarVertical
	}
	if topPartial && top-fRow >= 0.5 {
		return gamesBarTop
	}
	if bottomPartial && bottom-fRow <= 0.5 {
		return gamesBarBottom
	}
	return gamesBarVertical
}

func (m gamesModel) wrapLines(text string, width int) []string {
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

func (m gamesModel) buildListLayout(contentWidth int) gamesListLayout {
	count := len(m.filtered)
	layout := gamesListLayout{
		contentWidth: contentWidth,
		itemStarts:   make([]int, count),
		itemHeights:  make([]int, count),
		nameLines:    make([][]string, count),
		descLines:    make([][]string, count),
	}

	total := 0
	for i, idx := range m.filtered {
		nameLines := m.wrapLines(m.items[idx].Name, contentWidth)
		descLines := m.wrapLines(m.items[idx].Description, contentWidth)
		layout.nameLines[i] = nameLines
		layout.descLines[i] = descLines

		height := len(nameLines) + len(descLines)
		if height <= 0 {
			height = 1
		}
		layout.itemStarts[i] = total
		layout.itemHeights[i] = height
		total += height + gamesItemGap
	}

	if count > 0 {
		total -= gamesItemGap
	}
	layout.totalHeight = total
	return layout
}

func (m *gamesModel) renderGamesList(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}

	var headerLines []string
	headerLines = append(headerLines, m.styles.Title.Render("Games"))

	count := len(m.filtered)
	total := len(m.items)
	countLabel := fmt.Sprintf("%d games", count)
	if count != total {
		countLabel = fmt.Sprintf("%d of %d games", count, total)
	}
	headerLines = append(headerLines, m.styles.Count.Render(countLabel))

	filterLine := ""
	if m.filtering {
		cursor := ""
		if len(m.filterDraft) < 32 {
			cursor = "_"
		}
		filterLine = m.styles.FilterPrompt.Render("/ " + m.filterDraft + cursor)
	} else if m.filterValue != "" {
		filterLine = m.styles.FilterText.Render("Filter: " + m.filterValue)
	}
	if filterLine != "" {
		headerLines = append(headerLines, filterLine)
	}
	headerLines = append(headerLines, "")

	listHeight := height - len(headerLines)
	if listHeight < 0 {
		listHeight = 0
	}

	listLines := make([]string, 0, listHeight)
	listLines = append(listLines, headerLines...)
	listLines = append(listLines, m.renderGamesListItems(width, listHeight)...)

	return strings.Join(listLines, "\n")
}

func (m *gamesModel) renderGamesListItems(width, height int) []string {
	if height <= 0 {
		return nil
	}
	barWidth := 1
	contentWidth := width - barWidth - 1
	if contentWidth < 0 {
		contentWidth = 0
	}

	m.layout = m.buildListLayout(contentWidth)
	totalHeight := m.layout.totalHeight
	if !m.animating && m.selected >= 0 {
		m.syncBarToSelection()
	}

	offset := 0
	if totalHeight > height && m.selected >= 0 && m.selected < len(m.layout.itemStarts) {
		selStart := m.layout.itemStarts[m.selected]
		selHeight := m.layout.itemHeights[m.selected]
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
	showBar := m.selected >= 0 && m.initialized

	lines := make([]string, 0, height)
	itemIndex := 0
	itemEnd := 0
	itemBlockEnd := 0
	if len(m.layout.itemStarts) > 0 {
		itemEnd = m.layout.itemStarts[0] + m.layout.itemHeights[0]
		itemBlockEnd = itemEnd + gamesItemGap
		for itemIndex < len(m.layout.itemStarts)-1 && offset >= itemBlockEnd {
			itemIndex++
			itemEnd = m.layout.itemStarts[itemIndex] + m.layout.itemHeights[itemIndex]
			itemBlockEnd = itemEnd + gamesItemGap
		}
	}
	for row := 0; row < height; row++ {
		lineIndex := offset + row
		var line string
		if lineIndex >= totalHeight || len(m.filtered) == 0 {
			line = strings.Repeat(" ", width)
			lines = append(lines, line)
			continue
		}
		for itemIndex < len(m.layout.itemStarts)-1 && lineIndex >= itemBlockEnd {
			itemIndex++
			itemEnd = m.layout.itemStarts[itemIndex] + m.layout.itemHeights[itemIndex]
			itemBlockEnd = itemEnd + gamesItemGap
		}
		lineInItem := lineIndex - m.layout.itemStarts[itemIndex]

		bar := " "
		isSelected := itemIndex == m.selected
		isHovered := itemIndex == m.hovered
		itemHeight := m.layout.itemHeights[itemIndex]
		if isHovered && lineInItem >= 0 && lineInItem < itemHeight {
			bar = m.styles.BarHover.Render(gamesBarVertical)
		}
		if showBar {
			if ch := barCharForRow(lineIndex, barTop, barBottom); ch != "" {
				bar = m.styles.Bar.Render(ch)
			}
		}

		content := ""
		if itemIndex >= 0 && itemIndex < len(m.filtered) && lineInItem >= 0 && lineInItem < itemHeight {
			nameLines := m.layout.nameLines[itemIndex]
			descLines := m.layout.descLines[itemIndex]
			if lineInItem < len(nameLines) {
				content = nameLines[lineInItem]
				switch {
				case isSelected:
					content = m.styles.NameSelected.Render(content)
				case isHovered:
					content = m.styles.NameHover.Render(content)
				default:
					content = m.styles.Name.Render(content)
				}
			} else {
				descIndex := lineInItem - len(nameLines)
				if descIndex >= 0 && descIndex < len(descLines) {
					content = descLines[descIndex]
				}
				switch {
				case isSelected:
					content = m.styles.DescSelected.Render(content)
				case isHovered:
					content = m.styles.DescHover.Render(content)
				default:
					content = m.styles.Description.Render(content)
				}
			}
		}

		content = lipgloss.NewStyle().Width(contentWidth).Render(content)
		line = lipgloss.NewStyle().Width(width).Render(bar + " " + content)
		if m.zone != nil && lineInItem >= 0 && lineInItem < itemHeight {
			zoneID := m.zoneID(itemIndex, lineInItem)
			line = m.zone.Mark(zoneID, line)
		}
		lines = append(lines, line)
	}

	return lines
}

func (m *gamesModel) renderGameDetails(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}

	if len(m.filtered) == 0 || m.selected < 0 || m.selected >= len(m.filtered) {
		return lipgloss.NewStyle().Width(width).Height(height).
			Render(m.styles.DetailsSubtle.Render("No games match the current filter."))
	}

	item := m.items[m.filtered[m.selected]]
	title := m.styles.DetailsTitle.Render(item.Name)
	desc := m.styles.DetailsSubtle.Render(item.Description)
	body := m.styles.DetailsBody.Render(item.Details)

	parts := []string{title, desc, "", body}
	if len(item.Screenshots) > 0 {
		parts = append(parts, "\n"+m.renderCarousel(item.Screenshots, width))
	}

	content := strings.Join(parts, "\n")
	return lipgloss.NewStyle().Width(width).Height(height).Render(content)
}

func (m gamesModel) Init() tea.Cmd {
	if screenshots := m.selectedScreenshots(); len(screenshots) > 1 {
		return carouselAutoAdvanceCmd(m.carouselAutoGen)
	}
	return nil
}

func (m gamesModel) selectedScreenshots() []gameScreenshot {
	if len(m.filtered) == 0 || m.selected < 0 || m.selected >= len(m.filtered) {
		return nil
	}
	return m.items[m.filtered[m.selected]].Screenshots
}

func (m *gamesModel) resetCarousel() tea.Cmd {
	m.carouselScrollX = 0
	m.carouselVelocity = 0
	m.carouselDragging = false
	m.carouselMomentum = false
	m.carouselAnimating = false
	m.carouselAutoGen++
	if screenshots := m.selectedScreenshots(); len(screenshots) > 1 {
		return carouselAutoAdvanceCmd(m.carouselAutoGen)
	}
	return nil
}

func (m *gamesModel) goToCarouselPage(page int) (gamesModel, tea.Cmd) {
	screenshots := m.selectedScreenshots()
	n := len(screenshots)
	if n <= 1 {
		return *m, nil
	}
	targetX := float64(page * screenshotWidth)
	m.carouselAnimStartX = m.carouselScrollX
	m.carouselAnimTargetX = targetX
	m.carouselAnimStart = time.Now()
	m.carouselAnimating = true
	m.carouselMomentum = false
	m.carouselDragging = false
	m.carouselAutoGen++
	return *m, tea.Batch(gamesTickCmd(), carouselAutoAdvanceCmd(m.carouselAutoGen))
}

func (m gamesModel) carouselAnimProgress() float64 {
	if !m.carouselAnimating {
		return 1.0
	}
	dist := math.Abs(m.carouselAnimTargetX - m.carouselAnimStartX)
	dur := carouselSnapDuration
	if dist > float64(screenshotWidth)/2 {
		dur = carouselAnimDuration
	}
	p := float64(time.Since(m.carouselAnimStart)) / float64(dur)
	if p > 1.0 {
		return 1.0
	}
	return p
}

func (m gamesModel) carouselCurrentPage(count int) int {
	if count <= 0 {
		return 0
	}
	page := int(math.Round(m.carouselScrollX / screenshotWidth))
	if page < 0 {
		page = 0
	}
	if page >= count {
		page = count - 1
	}
	return page
}

func (m gamesModel) carouselMaxScrollX(count int) float64 {
	if count <= 1 {
		return 0
	}
	return float64((count - 1) * screenshotWidth)
}

func (m *gamesModel) renderCarousel(screenshots []gameScreenshot, viewWidth int) string {
	n := len(screenshots)
	if n == 0 {
		return ""
	}

	sw := screenshotWidth
	if sw > viewWidth {
		sw = viewWidth
	}

	scrollX := m.carouselScrollX
	leftIdx := int(math.Floor(scrollX / screenshotWidth))
	pixelOff := scrollX - float64(leftIdx*screenshotWidth)
	intOff := int(math.Round(pixelOff))

	if leftIdx < 0 {
		leftIdx = 0
		intOff = 0
	}
	if leftIdx >= n {
		leftIdx = n - 1
		intOff = 0
	}
	rightIdx := leftIdx + 1
	if rightIdx >= n {
		rightIdx = leftIdx
	}

	var frame string
	if intOff > 0 && leftIdx != rightIdx {
		fromLines := strings.Split(screenshots[leftIdx].Content, "\n")
		toLines := strings.Split(screenshots[rightIdx].Content, "\n")
		rows := make([]string, screenshotHeight)
		for i := range rows {
			from := padToScreenWidth(getLineOrEmpty(fromLines, i))
			to := padToScreenWidth(getLineOrEmpty(toLines, i))
			combined := from + "\x1b[0m" + to
			rows[i] = ansi.Cut(combined, intOff, intOff+sw)
		}
		frame = strings.Join(rows, "\n")
	} else {
		lines := strings.Split(screenshots[leftIdx].Content, "\n")
		rows := make([]string, screenshotHeight)
		for i := range rows {
			rows[i] = ansi.Truncate(padToScreenWidth(getLineOrEmpty(lines, i)), sw, "")
		}
		frame = strings.Join(rows, "\n")
	}

	if m.zone != nil {
		frame = m.zone.Mark(carouselFrameZoneID, frame)
	}

	currentPage := m.carouselCurrentPage(n)
	parts := []string{frame}
	if caption := screenshots[currentPage].Caption; caption != "" {
		parts = append(parts, m.styles.Caption.Render(caption))
	}
	if n > 1 {
		parts = append(parts, m.renderPaginatorDots(n, currentPage))
	}
	return strings.Join(parts, "\n")
}

func (m *gamesModel) renderPaginatorDots(count, currentPage int) string {
	dots := make([]string, count)
	for i := range dots {
		var dot string
		if i == currentPage {
			dot = m.styles.DotActive.Render("●")
		} else {
			dot = m.styles.DotInactive.Render("○")
		}
		if m.zone != nil {
			dot = m.zone.Mark(m.carouselDotZoneID(i), dot)
		}
		dots[i] = dot
	}
	return strings.Join(dots, " ")
}

func (m gamesModel) carouselDotZoneID(page int) string {
	return fmt.Sprintf("carousel-dot-%d", page)
}

func (m gamesModel) indexAtMouse(msg tea.MouseMsg) int {
	if m.zone == nil {
		return -1
	}
	for i := 0; i < len(m.filtered); i++ {
		height := gamesItemHeight
		if i < len(m.layout.itemHeights) && m.layout.itemHeights[i] > 0 {
			height = m.layout.itemHeights[i]
		}
		for line := 0; line < height; line++ {
			if m.zone.Get(m.zoneID(i, line)).InBounds(msg) {
				return i
			}
		}
	}
	return -1
}

func (m gamesModel) zoneID(itemIndex, line int) string {
	return fmt.Sprintf("games-item-%d-%d", itemIndex, line)
}

func getLineOrEmpty(lines []string, i int) string {
	if i < len(lines) {
		return lines[i]
	}
	return ""
}

func padToScreenWidth(line string) string {
	w := lipgloss.Width(line)
	if w >= screenshotWidth {
		return ansi.Truncate(line, screenshotWidth, "")
	}
	return line + strings.Repeat(" ", screenshotWidth-w)
}

func clampWithResistance(x, min, max, resistance float64) float64 {
	if x < min {
		return min + (x-min)*resistance
	}
	if x > max {
		return max + (x-max)*resistance
	}
	return x
}

func placeholderScreenshot(title, fg, bg string) string {
	titleStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(fg)).Background(lipgloss.Color(bg)).Bold(true)
	bgStyle := lipgloss.NewStyle().Background(lipgloss.Color(bg))
	lines := make([]string, screenshotHeight)
	for i := range lines {
		if i == screenshotHeight/2 {
			tw := lipgloss.Width(title)
			pad := (screenshotWidth - tw) / 2
			if pad < 0 {
				pad = 0
			}
			right := screenshotWidth - tw - pad
			if right < 0 {
				right = 0
			}
			lines[i] = bgStyle.Render(strings.Repeat(" ", pad)) + titleStyle.Render(title) + bgStyle.Render(strings.Repeat(" ", right))
		} else {
			lines[i] = bgStyle.Render(strings.Repeat(" ", screenshotWidth))
		}
	}
	return strings.Join(lines, "\n")
}
