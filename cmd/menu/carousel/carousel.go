// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package carousel

import (
	"fmt"
	"math"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	zone "github.com/lrstanley/bubblezone"
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
	dragElastic  = 0.3
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
	velocity    float64
	dragging    bool
	dragStartMX int
	dragStartSX float64
	lastMX      int
	lastMT      time.Time
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

type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(16*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
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
				return m, tickCmd()
			}
		}
		return m, nil

	case autoAdvanceMsg:
		if msg.gen != m.autoGen {
			return m, nil
		}
		if m.dragging || m.momentum {
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
	switch msg.Action {
	case tea.MouseActionPress:
		if m.zone != nil && m.zone.Get(frameZoneID).InBounds(msg) {
			m.dragging = true
			m.dragStartMX = msg.X
			m.dragStartSX = m.scrollX
			m.lastMX = msg.X
			m.lastMT = time.Now()
			m.velocity = 0
			m.momentum = false
			m.animating = false
			m.autoGen++
			return *m, nil, true
		}
	case tea.MouseActionMotion:
		if m.dragging {
			n := len(m.Screenshots)
			maxX := m.maxScrollX(n)
			rawX := m.dragStartSX + float64(m.dragStartMX-msg.X)
			m.scrollX = clampWithResistance(rawX, 0, maxX, dragElastic)
			dt := time.Since(m.lastMT).Seconds()
			if dt > 0 && dt < 0.1 {
				instantV := float64(m.lastMX-msg.X) / dt
				m.velocity = 0.6*m.velocity + 0.4*instantV
			}
			m.lastMX = msg.X
			m.lastMT = time.Now()
			return *m, nil, true
		}
	case tea.MouseActionRelease:
		if m.dragging {
			m.dragging = false
			n := len(m.Screenshots)
			if time.Since(m.lastMT) > 80*time.Millisecond {
				m.velocity = 0
			}
			maxX := m.maxScrollX(n)
			projected := math.Max(0, math.Min(m.scrollX+m.velocity*0.15, maxX))
			targetPage := int(math.Round(projected / ScreenshotWidth))
			if targetPage < 0 {
				targetPage = 0
			}
			if targetPage >= n {
				targetPage = n - 1
			}
			targetX := float64(targetPage * ScreenshotWidth)
			m.animStartX = m.scrollX
			m.animTargetX = targetX
			m.animStart = time.Now()
			m.animating = true
			m.autoGen++
			return *m, tickCmd(), true
		}
		if m.zone != nil {
			n := len(m.Screenshots)
			cur := m.currentPage()
			for i := range m.Screenshots {
				if m.zone.Get(dotZoneID(i)).InBounds(msg) {
					if i != cur && n > 1 {
						result, cmd := m.GoToPage(i)
						return result, cmd, true
					}
					return *m, nil, true
				}
			}
		}
	}
	return *m, nil, false
}

func (m Model) BtnClicked(msg tea.MouseMsg) bool {
	return m.zone != nil && m.zone.Get(btnZoneID).InBounds(msg)
}

func (m *Model) Reset() tea.Cmd {
	m.scrollX = 0
	m.velocity = 0
	m.dragging = false
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
	m.animStartX = m.scrollX
	m.animTargetX = float64(page * ScreenshotWidth)
	m.animStart = time.Now()
	m.animating = true
	m.momentum = false
	m.dragging = false
	m.autoGen++
	return m, tea.Batch(tickCmd(), autoAdvanceCmd(m.autoGen))
}

// View renders the carousel inline (screenshot + caption + dots).
func (m Model) View(viewWidth int) string {
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
		for i := range rows {
			from := padToScreenWidth(getLineOrEmpty(fromLines, i))
			to := padToScreenWidth(getLineOrEmpty(toLines, i))
			combined := from + "\x1b[0m" + to
			rows[i] = ansi.Cut(combined, intOff, intOff+sw)
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

	if m.zone != nil {
		frame = m.zone.Mark(frameZoneID, frame)
	}

	cur := m.currentPage()
	parts := []string{frame}
	if caption := m.Screenshots[cur].Caption; caption != "" {
		parts = append(parts, m.Styles.Caption.Render(caption))
	}
	if n > 1 {
		parts = append(parts, m.renderDots(n, cur))
	}
	return strings.Join(parts, "\n")
}

// ViewButton renders the clickable button shown when the carousel can't fit inline.
func (m Model) ViewButton() string {
	btn := m.Styles.Button.Render(fmt.Sprintf("▶ %s (%d)", m.Labels.Screenshots, len(m.Screenshots)))
	if m.zone != nil {
		btn = m.zone.Mark(btnZoneID, btn)
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
	return width >= ScreenshotWidth && height >= MinInlineHeight
}

func CanFitModal(termW, termH int) bool {
	return termW >= ScreenshotWidth && termH >= ModalMinHeight
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
	dots := make([]string, count)
	for i := range dots {
		var dot string
		if i == currentPage {
			dot = m.Styles.DotActive.Render("●")
		} else {
			dot = m.Styles.DotInactive.Render("○")
		}
		if m.zone != nil {
			dot = m.zone.Mark(dotZoneID(i), dot)
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

func clampWithResistance(x, min, max, resistance float64) float64 {
	if x < min {
		return min + (x-min)*resistance
	}
	if x > max {
		return max + (x-max)*resistance
	}
	return x
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
