// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package carousel

import (
	"fmt"
	"math"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	zone "github.com/lrstanley/bubblezone/v2"
	"github.com/terminal-games/terminal-games/cmd/menu/theme"
)

const (
	ScreenshotWidth  = 80
	ScreenshotHeight = 24
	MinInlineHeight  = ScreenshotHeight + 6
	ModalMinHeight   = ScreenshotHeight + 3

	autoInterval = 10 * time.Second
	animDuration = 400 * time.Millisecond
	snapDuration = 150 * time.Millisecond
	frameZoneID  = "carousel-frame"
	btnZoneID    = "carousel-btn"
)

type Screenshot struct {
	Content string
	Caption string
}

type Labels struct {
	Screenshots string
	EscToClose  string
}

type Styles struct {
	DotActive   lipgloss.Style
	DotInactive lipgloss.Style
	Caption     lipgloss.Style
	Button      lipgloss.Style
	Title       lipgloss.Style
	Hint        lipgloss.Style
}

func DefaultStyles() Styles {
	return Styles{
		DotActive:   lipgloss.NewStyle().Foreground(theme.Primary),
		DotInactive: lipgloss.NewStyle().Foreground(theme.TextSubtle),
		Caption:     lipgloss.NewStyle().Foreground(theme.TextMuted),
		Button:      lipgloss.NewStyle().Foreground(theme.Primary).Bold(true),
		Title:       lipgloss.NewStyle().Bold(true),
		Hint:        lipgloss.NewStyle().Foreground(theme.TextMuted),
	}
}

type Model struct {
	Screenshots []Screenshot
	Modal       bool
	Styles      Styles
	Labels      Labels
	zone        *zone.Manager

	scrollX     float64
	momentum    bool
	animating   bool
	animStartX  float64
	animTargetX float64
	animStart   time.Time
	autoGen     int
}

func New(zoneManager *zone.Manager) Model {
	return Model{
		Styles: DefaultStyles(),
		Labels: Labels{
			Screenshots: "Screenshots",
			EscToClose:  "ESC to close",
		},
		zone: zoneManager,
	}
}

type tickMsg struct{ gen int }

func tickCmd(gen int) tea.Cmd {
	return tea.Tick(16*time.Millisecond, func(time.Time) tea.Msg {
		return tickMsg{gen}
	})
}

type autoAdvanceMsg struct{ gen int }

func autoAdvanceCmd(gen int) tea.Cmd {
	return tea.Tick(autoInterval, func(time.Time) tea.Msg {
		return autoAdvanceMsg{gen}
	})
}

func (m Model) Init() tea.Cmd {
	if len(m.Screenshots) > 1 {
		return autoAdvanceCmd(m.autoGen)
	}
	return nil
}

func (m Model) Update(msg tea.Msg) (Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tickMsg:
		if msg.gen != m.autoGen {
			return m, nil
		}
		if m.animating {
			p := m.animProgress()
			if p >= 1.0 {
				m.scrollX = m.animTargetX
				m.animating = false
				if len(m.Screenshots) > 1 {
					m.autoGen++
					return m, autoAdvanceCmd(m.autoGen)
				}
			} else {
				t := easeOutCubic(p)
				m.scrollX = m.animStartX + (m.animTargetX-m.animStartX)*t
				return m, tickCmd(m.autoGen)
			}
		}
		return m, nil

	case autoAdvanceMsg:
		if msg.gen != m.autoGen {
			return m, nil
		}
		if m.momentum {
			return m, nil
		}
		n := len(m.Screenshots)
		if n <= 1 {
			return m, nil
		}
		nextPage := (m.currentPage() + 1) % n
		return m.GoToPage(nextPage)
	}

	return m, nil
}

func (m *Model) HandleMouse(msg tea.MouseMsg) (Model, tea.Cmd, bool) {
	return m.handleMouse(msg, m.zone)
}

func (m *Model) HandleMouseInZone(msg tea.MouseMsg, zoneManager *zone.Manager) (Model, tea.Cmd, bool) {
	return m.handleMouse(msg, zoneManager)
}

func (m *Model) handleMouse(msg tea.MouseMsg, zoneManager *zone.Manager) (Model, tea.Cmd, bool) {
	switch msg := msg.(type) {
	case tea.MouseReleaseMsg:
		if zoneManager != nil {
			n := len(m.Screenshots)
			fz := zoneManager.Get(frameZoneID)
			if n > 1 && fz.InBounds(msg) {
				x, _ := fz.Pos(msg)
				zoneWidth := fz.EndX - fz.StartX + 1
				if x < zoneWidth/2 {
					result, cmd := m.Prev()
					return result, cmd, true
				}
				result, cmd := m.Next()
				return result, cmd, true
			}
			cur := m.currentPage()
			for i := range m.Screenshots {
				if zoneManager.Get(dotZoneID(i)).InBounds(msg) {
					if i != cur && n > 1 {
						result, cmd := m.GoToPage(i)
						return result, cmd, true
					}
					return *m, nil, true
				}
			}
			if fz.InBounds(msg) {
				return *m, nil, true
			}
		}
	case tea.MouseClickMsg:
		if zoneManager != nil && zoneManager.Get(frameZoneID).InBounds(msg) {
			return *m, nil, true
		}
	}
	return *m, nil, false
}

func (m Model) BtnClicked(msg tea.MouseMsg) bool {
	return m.zone != nil && m.zone.Get(btnZoneID).InBounds(msg)
}

func (m Model) BtnClickedInZone(msg tea.MouseMsg, zoneManager *zone.Manager) bool {
	return zoneManager != nil && zoneManager.Get(btnZoneID).InBounds(msg)
}

func (m Model) IsDragging() bool { return false }

func (m *Model) Reset() tea.Cmd {
	m.scrollX = 0
	m.momentum = false
	m.animating = false
	m.Modal = false
	m.autoGen++
	if len(m.Screenshots) > 1 {
		return autoAdvanceCmd(m.autoGen)
	}
	return nil
}

func (m *Model) RestartAuto() tea.Cmd {
	m.autoGen++
	if len(m.Screenshots) > 1 {
		return autoAdvanceCmd(m.autoGen)
	}
	return nil
}

func (m Model) GoToPage(page int) (Model, tea.Cmd) {
	n := len(m.Screenshots)
	if n <= 1 {
		return m, nil
	}
	if page < 0 {
		page = 0
	}
	if page >= n {
		page = n - 1
	}
	targetX := float64(page * ScreenshotWidth)
	if m.animating && math.Abs(m.animTargetX-targetX) < 0.001 {
		return m, nil
	}
	if math.Abs(m.scrollX-targetX) < 0.001 {
		return m, nil
	}
	m.animStartX = m.scrollX
	m.animTargetX = targetX
	m.animStart = time.Now()
	m.animating = true
	m.momentum = false
	m.autoGen++
	return m, tea.Batch(tickCmd(m.autoGen), autoAdvanceCmd(m.autoGen))
}

func (m Model) Prev() (Model, tea.Cmd) {
	n := len(m.Screenshots)
	if n <= 1 {
		return m, nil
	}
	cur := m.currentPage()
	if cur == 0 {
		return m.GoToPage(n - 1)
	}
	return m.GoToPage(cur - 1)
}

func (m Model) Next() (Model, tea.Cmd) {
	n := len(m.Screenshots)
	if n <= 1 {
		return m, nil
	}
	cur := m.currentPage()
	if cur == n-1 {
		return m.GoToPage(0)
	}
	return m.GoToPage(cur + 1)
}

// View renders the carousel inline (screenshot + caption + dots).
func (m Model) View(viewWidth int) string {
	return m.view(viewWidth, m.zone)
}

func (m Model) ViewInZone(viewWidth int, zoneManager *zone.Manager) string {
	return m.view(viewWidth, zoneManager)
}

func (m Model) view(viewWidth int, zoneManager *zone.Manager) string {
	n := len(m.Screenshots)
	if n == 0 {
		return ""
	}

	sw := ScreenshotWidth
	if sw > viewWidth {
		sw = viewWidth
	}

	leftIdx := int(math.Floor(m.scrollX / ScreenshotWidth))
	pixelOff := m.scrollX - float64(leftIdx*ScreenshotWidth)
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
		fromLines := strings.Split(m.Screenshots[leftIdx].Content, "\n")
		toLines := strings.Split(m.Screenshots[rightIdx].Content, "\n")
		rows := make([]string, ScreenshotHeight)
		fromVisible := ScreenshotWidth - intOff
		if fromVisible > sw {
			fromVisible = sw
		}
		toVisible := sw - fromVisible
		for i := range rows {
			from := padToScreenWidth(getLineOrEmpty(fromLines, i))
			left := ansi.Cut(from, intOff, intOff+fromVisible)
			if toVisible > 0 {
				to := padToScreenWidth(getLineOrEmpty(toLines, i))
				rows[i] = left + "\x1b[0m" + ansi.Truncate(to, toVisible, "")
			} else {
				rows[i] = left
			}
		}
		frame = strings.Join(rows, "\n")
	} else {
		lines := strings.Split(m.Screenshots[leftIdx].Content, "\n")
		rows := make([]string, ScreenshotHeight)
		for i := range rows {
			rows[i] = ansi.Truncate(padToScreenWidth(getLineOrEmpty(lines, i)), sw, "")
		}
		frame = strings.Join(rows, "\n")
	}

	if zoneManager != nil {
		frame = zoneManager.Mark(frameZoneID, frame)
	}

	cur := m.currentPage()
	parts := []string{frame}
	if caption := m.Screenshots[cur].Caption; caption != "" {
		parts = append(parts, m.Styles.Caption.Render(caption))
	}
	if n > 1 {
		parts = append(parts, m.renderDotsWithZone(n, cur, zoneManager))
	}
	return strings.Join(parts, "\n")
}

// ViewButton renders the clickable button shown when the carousel can't fit inline.
func (m Model) ViewButton() string {
	return m.viewButton(m.zone)
}

func (m Model) ViewButtonInZone(zoneManager *zone.Manager) string {
	return m.viewButton(zoneManager)
}

func (m Model) viewButton(zoneManager *zone.Manager) string {
	btn := m.Styles.Button.Render(fmt.Sprintf("▶ %s (%d)", m.Labels.Screenshots, len(m.Screenshots)))
	if zoneManager != nil {
		btn = zoneManager.Mark(btnZoneID, btn)
	}
	return btn
}

// ViewModal renders the fullscreen modal with the carousel centered.
func (m Model) ViewModal(title string, width, height int) string {
	if len(m.Screenshots) == 0 {
		return ""
	}

	cw := ScreenshotWidth
	if cw > width {
		cw = width
	}

	titleStr := m.Styles.Title.Render(title)
	hintStr := m.Styles.Hint.Render(m.Labels.EscToClose)
	gap := cw - lipgloss.Width(titleStr) - lipgloss.Width(hintStr)
	if gap < 1 {
		gap = 1
	}
	header := titleStr + strings.Repeat(" ", gap) + hintStr

	content := header + "\n" + m.View(cw)
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, content)
}

func (m *Model) SetLabels(labels Labels) {
	m.Labels = labels
}

func CanFitInline(width, height int) bool {
	return width >= ScreenshotWidth
}

func CanFitModal(termW, termH int) bool {
	return termW >= ScreenshotWidth && termH >= ModalMinHeight
}

func (m *Model) HandleWindowSize(termW, termH int) {
	if m.Modal && !CanFitModal(termW, termH) {
		m.Modal = false
	}
}

func (m Model) animProgress() float64 {
	if !m.animating {
		return 1.0
	}
	dist := math.Abs(m.animTargetX - m.animStartX)
	dur := snapDuration
	if dist > float64(ScreenshotWidth)/2 {
		dur = animDuration
	}
	p := float64(time.Since(m.animStart)) / float64(dur)
	if p > 1.0 {
		return 1.0
	}
	return p
}

func (m Model) currentPage() int {
	n := len(m.Screenshots)
	if n <= 0 {
		return 0
	}
	page := int(math.Round(m.scrollX / ScreenshotWidth))
	if page < 0 {
		page = 0
	}
	if page >= n {
		page = n - 1
	}
	return page
}

func (m Model) maxScrollX(count int) float64 {
	if count <= 1 {
		return 0
	}
	return float64((count - 1) * ScreenshotWidth)
}

func (m Model) renderDots(count, currentPage int) string {
	return m.renderDotsWithZone(count, currentPage, m.zone)
}

func (m Model) renderDotsWithZone(count, currentPage int, zoneManager *zone.Manager) string {
	dots := make([]string, count)
	for i := range dots {
		var dot string
		if i == currentPage {
			dot = m.Styles.DotActive.Render("●")
		} else {
			dot = m.Styles.DotInactive.Render("○")
		}
		if zoneManager != nil {
			dot = zoneManager.Mark(dotZoneID(i), dot)
		}
		dots[i] = dot
	}
	return strings.Join(dots, " ")
}

func dotZoneID(page int) string {
	return fmt.Sprintf("carousel-dot-%d", page)
}

func easeOutCubic(t float64) float64 {
	return 1 - (1-t)*(1-t)*(1-t)
}

func getLineOrEmpty(lines []string, i int) string {
	if i < len(lines) {
		return lines[i]
	}
	return ""
}

func padToScreenWidth(line string) string {
	w := lipgloss.Width(line)
	if w >= ScreenshotWidth {
		return ansi.Truncate(line, ScreenshotWidth, "")
	}
	return line + strings.Repeat(" ", ScreenshotWidth-w)
}

func PlaceholderScreenshot(title, fg, bg string) string {
	titleStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(fg)).Background(lipgloss.Color(bg)).Bold(true)
	bgStyle := lipgloss.NewStyle().Background(lipgloss.Color(bg))
	lines := make([]string, ScreenshotHeight)
	for i := range lines {
		if i == ScreenshotHeight/2 {
			tw := lipgloss.Width(title)
			pad := (ScreenshotWidth - tw) / 2
			if pad < 0 {
				pad = 0
			}
			right := ScreenshotWidth - tw - pad
			if right < 0 {
				right = 0
			}
			lines[i] = bgStyle.Render(strings.Repeat(" ", pad)) + titleStyle.Render(title) + bgStyle.Render(strings.Repeat(" ", right))
		} else {
			lines[i] = bgStyle.Render(strings.Repeat(" ", ScreenshotWidth))
		}
	}
	return strings.Join(lines, "\n")
}
