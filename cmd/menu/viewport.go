// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	zone "github.com/lrstanley/bubblezone/v2"

	"github.com/terminal-games/terminal-games/cmd/menu/theme"
)

type viewportLineRenderer func(contentWidth int) []string

type viewportLayout struct {
	lines         []string
	contentWidth  int
	showScrollbar bool
	maxScroll     int
}

type scrollViewport struct {
	zone        *zone.Manager
	zoneID      string
	contentZone *zone.Manager
	scroll      int
	width       int
	height      int
	dragging    bool
	dragOffset  int
	cacheValid  bool
	cacheWidth  int
	cacheHeight int
	cacheLayout viewportLayout
	trackStyle  lipgloss.Style
	thumbStyle  lipgloss.Style
}

func newScrollViewport(zoneManager *zone.Manager, zoneID string) scrollViewport {
	return scrollViewport{
		zone:       zoneManager,
		zoneID:     zoneID,
		trackStyle: lipgloss.NewStyle().Foreground(theme.Line),
		thumbStyle: lipgloss.NewStyle().Foreground(theme.Primary),
	}
}

func newScrollViewportWithContentZone(zoneManager *zone.Manager, zoneID string, contentZone *zone.Manager) scrollViewport {
	v := newScrollViewport(zoneManager, zoneID)
	v.contentZone = contentZone
	return v
}

func viewportWrappedLines(content string, width int) []string {
	if width <= 0 {
		return []string{""}
	}
	lines := strings.Split(lipgloss.NewStyle().Width(width).Render(content), "\n")
	if len(lines) == 0 {
		return []string{""}
	}
	return lines
}

func viewportPlainLines(content string) []string {
	lines := strings.Split(content, "\n")
	if len(lines) == 0 {
		return []string{""}
	}
	return lines
}

func (v *scrollViewport) layout(width, height int, render viewportLineRenderer) viewportLayout {
	v.width = width
	v.height = height
	if v.cacheValid && v.cacheWidth == width && v.cacheHeight == height {
		return v.cacheLayout
	}
	if width <= 0 || height <= 0 {
		layout := viewportLayout{contentWidth: max(0, width)}
		v.cache(width, height, layout)
		return layout
	}
	contentWidth := max(1, width)
	lines := render(contentWidth)
	lines = v.scanLines(lines)
	if len(lines) == 0 {
		lines = []string{""}
	}
	layout := viewportLayout{
		lines:         lines,
		contentWidth:  contentWidth,
		showScrollbar: false,
		maxScroll:     max(0, len(lines)-height),
	}
	if len(lines) <= height || width < 3 {
		v.cache(width, height, layout)
		return layout
	}
	contentWidth = max(1, width-2)
	lines = render(contentWidth)
	lines = v.scanLines(lines)
	if len(lines) == 0 {
		lines = []string{""}
	}
	layout = viewportLayout{
		lines:         lines,
		contentWidth:  contentWidth,
		showScrollbar: true,
		maxScroll:     max(0, len(lines)-height),
	}
	v.cache(width, height, layout)
	return layout
}

func (v *scrollViewport) View(width, height int, render viewportLineRenderer) string {
	layout := v.layout(width, height, render)
	if width <= 0 || height <= 0 {
		return ""
	}
	v.scroll = clampInt(v.scroll, 0, layout.maxScroll)
	start := min(v.scroll, layout.maxScroll)
	end := min(start+height, len(layout.lines))
	visible := viewportSliceLines(layout.lines, start, end)
	for len(visible) < height {
		visible = append(visible, "")
	}

	rows := make([]string, 0, height)
	if !layout.showScrollbar {
		for _, line := range visible {
			rows = append(rows, padStyled(line, layout.contentWidth))
		}
		return v.mark(strings.Join(rows, "\n"))
	}

	thumbSize := max(1, height*height/max(1, len(layout.lines)))
	thumbStart := 0
	if layout.maxScroll > 0 && height > thumbSize {
		thumbStart = (start * (height - thumbSize)) / layout.maxScroll
	}
	for i, line := range visible {
		bar := v.trackStyle.Render("│")
		if i >= thumbStart && i < thumbStart+thumbSize {
			bar = v.thumbStyle.Render("█")
		}
		rows = append(rows, padStyled(line, layout.contentWidth)+" "+bar)
	}
	return v.mark(strings.Join(rows, "\n"))
}

func (v *scrollViewport) HandleMouse(msg tea.MouseMsg, render viewportLineRenderer) bool {
	layout := v.layout(v.width, v.height, render)
	if !layout.showScrollbar {
		v.dragging = false
		return false
	}
	switch event := msg.(type) {
	case tea.MouseClickMsg:
		if event.Button != tea.MouseLeft {
			return false
		}
		x, y, ok := v.mousePos(msg, true)
		if !ok || x != v.width-1 {
			return false
		}
		thumbStart, thumbSize := v.thumbMetrics(layout)
		if y >= thumbStart && y < thumbStart+thumbSize {
			v.dragging = true
			v.dragOffset = y - thumbStart
			return true
		}
		v.jumpThumbTo(y, layout)
		return true
	case tea.MouseMotionMsg:
		if !v.dragging {
			return false
		}
		_, y, ok := v.mousePos(msg, false)
		if !ok {
			return false
		}
		v.dragThumbTo(y, layout)
		return true
	case tea.MouseReleaseMsg:
		if !v.dragging {
			return false
		}
		v.dragging = false
		return true
	}
	return false
}

func (v *scrollViewport) MaxScroll(render viewportLineRenderer) int {
	if v.width <= 0 || v.height <= 0 {
		return 0
	}
	return v.layout(v.width, v.height, render).maxScroll
}

func (v *scrollViewport) CanScroll(render viewportLineRenderer) bool {
	return v.MaxScroll(render) > 0
}

func (v *scrollViewport) ScrollBy(delta int, render viewportLineRenderer) {
	v.scroll = clampInt(v.scroll+delta, 0, v.MaxScroll(render))
}

func (v *scrollViewport) ScrollToTop() {
	v.scroll = 0
}

func (v *scrollViewport) ScrollToBottom(render viewportLineRenderer) {
	v.scroll = v.MaxScroll(render)
}

func (v *scrollViewport) Reset() {
	v.scroll = 0
	v.dragging = false
}

func (v *scrollViewport) Invalidate() {
	v.cacheValid = false
}

func (v *scrollViewport) InBounds(msg tea.MouseMsg) bool {
	return v.zone != nil && v.zoneID != "" && v.zone.Get(v.zoneID).InBounds(msg)
}

func (v *scrollViewport) ContentMouse(msg tea.MouseMsg) (tea.MouseMsg, bool) {
	if v.contentZone == nil || !v.cacheValid {
		return nil, false
	}
	x, y, ok := v.mousePos(msg, true)
	if !ok || x < 0 || y < 0 || x >= v.cacheLayout.contentWidth {
		return nil, false
	}
	mouse := msg.Mouse()
	mouse.X = x
	mouse.Y = y + v.scroll
	switch msg.(type) {
	case tea.MouseClickMsg:
		return tea.MouseClickMsg(mouse), true
	case tea.MouseReleaseMsg:
		return tea.MouseReleaseMsg(mouse), true
	case tea.MouseWheelMsg:
		return tea.MouseWheelMsg(mouse), true
	case tea.MouseMotionMsg:
		return tea.MouseMotionMsg(mouse), true
	default:
		return nil, false
	}
}

func (v *scrollViewport) mark(view string) string {
	if v.zone == nil || v.zoneID == "" {
		return view
	}
	return v.zone.Mark(v.zoneID, view)
}

func (v *scrollViewport) cache(width, height int, layout viewportLayout) {
	v.cacheValid = true
	v.cacheWidth = width
	v.cacheHeight = height
	v.cacheLayout = layout
}

func (v *scrollViewport) thumbMetrics(layout viewportLayout) (start, size int) {
	size = max(1, v.height*v.height/max(1, len(layout.lines)))
	start = 0
	if layout.maxScroll > 0 && v.height > size {
		start = (v.scroll * (v.height - size)) / layout.maxScroll
	}
	return start, size
}

func (v *scrollViewport) jumpThumbTo(y int, layout viewportLayout) {
	_, size := v.thumbMetrics(layout)
	targetStart := clampInt(y-size/2, 0, max(0, v.height-size))
	v.scroll = v.scrollForThumbStart(targetStart, size, layout)
}

func (v *scrollViewport) dragThumbTo(y int, layout viewportLayout) {
	_, size := v.thumbMetrics(layout)
	targetStart := clampInt(y-v.dragOffset, 0, max(0, v.height-size))
	v.scroll = v.scrollForThumbStart(targetStart, size, layout)
}

func (v *scrollViewport) scrollForThumbStart(start, size int, layout viewportLayout) int {
	if layout.maxScroll <= 0 || v.height <= size {
		return 0
	}
	return clampInt((start*layout.maxScroll)/(v.height-size), 0, layout.maxScroll)
}

func (v *scrollViewport) mousePos(msg tea.MouseMsg, requireInBounds bool) (x, y int, ok bool) {
	if v.zone == nil || v.zoneID == "" {
		return 0, 0, false
	}
	info := v.zone.Get(v.zoneID)
	if info.IsZero() {
		return 0, 0, false
	}
	if requireInBounds && !info.InBounds(msg) {
		return 0, 0, false
	}
	mouse := msg.Mouse()
	return mouse.X - info.StartX, mouse.Y - info.StartY, true
}

func viewportSliceLines(lines []string, start, end int) []string {
	return append([]string{}, lines[start:end]...)
}

func (v *scrollViewport) scanLines(lines []string) []string {
	if v.contentZone == nil {
		return lines
	}
	return viewportPlainLines(v.contentZone.Scan(strings.Join(lines, "\n")))
}
