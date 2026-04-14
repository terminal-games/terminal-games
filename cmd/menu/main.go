// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	_ "embed"
	"log"
	"os"
	"strings"
	"time"
	_ "unsafe"

	"charm.land/bubbles/v2/help"
	"charm.land/bubbles/v2/key"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	zone "github.com/lrstanley/bubblezone/v2"

	"github.com/terminal-games/terminal-games/cmd/menu/carousel"
	"github.com/terminal-games/terminal-games/cmd/menu/gamelist"
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
	maxWidth               = 120
	minContentHeight       = 5
	minViewportWidth       = 40
	minViewportHeight      = 11
	helpPaddingX           = 2
	sshAudioDialogMinWidth = 54
	sshAudioDialogMaxWidth = 120
	sshAudioCopyBashTarget = "bash"
	sshAudioCopyFishTarget = "fish"
	sshAudioCalloutZoneID  = "menu-ssh-audio-callout"
	sshAudioDismissZoneID  = "menu-ssh-audio-dismiss"
	sshAudioDialogZoneID   = "menu-ssh-audio-dialog"
	sshAudioDialogCloseID  = "menu-ssh-audio-dialog-close"
	sshAudioCopyBashID     = "menu-ssh-audio-copy-bash"
	sshAudioCopyFishID     = "menu-ssh-audio-copy-fish"
)

const (
	sshAudioBashCommand = `ssh -C -q terminalgames.net -t audio 2> >(ffplay -f ogg -nodisp -v quiet -autoexit -)`
	sshAudioFishCommand = `ssh -C -q terminalgames.net -t audio 2>| ffplay -f ogg -nodisp -v quiet -autoexit -`
)

type model struct {
	w                   int
	h                   int
	zone                *zone.Manager
	started             bool
	localeReady         bool
	showLoading         bool
	showSSHAudioCallout bool
	showSSHAudioHelp    bool
	tabs                tabs.Model
	localizer           localizer
	contentArea         lipgloss.Style
	barStyle            lipgloss.Style
	titleStyle          lipgloss.Style
	keys                keyMap
	help                help.Model
	sshAudioCopied      string
	sshAudioCopyGen     int
	sshAudioHovered     string
	games               gamesModel
	profile             profileModel
	about               aboutModel
	dirty               bool
	viewCache           string
}

type helpKeyMap interface {
	ShortHelp() []key.Binding
	FullHelp() [][]key.Binding
}

type startupLoadingDelayElapsedMsg struct{}
type sshAudioCopyResetMsg struct {
	gen int
}

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
	Quit             key.Binding
	NextTab          key.Binding
	PrevTab          key.Binding
	AudioHelp        key.Binding
	DismissAudioHelp key.Binding
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
			key.WithHelp("q", localizer.HelpQuit()),
		),
		NextTab: key.NewBinding(
			key.WithKeys("tab"),
			key.WithHelp("tab", localizer.HelpNextTab()),
		),
		PrevTab: key.NewBinding(
			key.WithKeys("shift+tab"),
			key.WithHelp("shift+tab", localizer.HelpPrevTab()),
		),
		AudioHelp: key.NewBinding(
			key.WithKeys("a"),
			key.WithHelp("a", localizer.HelpAudio()),
		),
		DismissAudioHelp: key.NewBinding(
			key.WithKeys("x"),
			key.WithHelp("x", localizer.HelpDismiss()),
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
	case "about":
		return m.about
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
	short := []key.Binding{m.keys.NextTab, m.keys.PrevTab}
	if m.showSSHAudioCallout {
		short = append(short, m.keys.AudioHelp, m.keys.DismissAudioHelp)
	}
	short = append(short, m.keys.Quit)
	return helpBindings{
		short: short,
	}
}

func main() {
	theme.ConfigureFromTerminalInfo()

	zoneManager := zone.New()

	menuModel := &model{
		zone:                zoneManager,
		localizer:           newLocalizer(),
		contentArea:         lipgloss.NewStyle().Padding(1, 2),
		barStyle:            lipgloss.NewStyle().Foreground(theme.Line),
		titleStyle:          lipgloss.NewStyle().Bold(true),
		showSSHAudioCallout: shouldShowSSHAudioHelp(),
		keys:                newKeyMap(newLocalizer()),
		help:                help.New(),
		games:               newGamesModel(zoneManager),
		profile:             newProfileModel(zoneManager),
		about:               newAboutModel(zoneManager),
	}
	menuModel.applyMenuLocalization()

	p, err := bubblewrap.NewProgram(menuModel)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}

func (m *model) Init() tea.Cmd {
	return tea.Batch(
		m.tabs.Init(),
		m.games.Init(),
		m.profile.Init(),
		m.about.Init(),
		startupLoadingDelayCmd(),
	)
}

func shouldShowSSHAudioHelp() bool {
	return os.Getenv("AUDIO_ENABLED") != "1" && os.Getenv("REMOTE_SSHID") != "cli"
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
	switch msg := message.(type) {
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	case tea.MouseMsg:
		return m.handleMouse(msg)
	case tea.WindowSizeMsg:
		m.dirty = true
		m.w = msg.Width
		m.h = msg.Height
		var cmd tea.Cmd
		m.games, cmd = m.games.Update(message)
		return m, cmd
	case tea.BackgroundColorMsg:
		m.dirty = true
		theme.SetHasDarkBackground(msg.IsDark())
		m.applyThemeStyles()
		return m, nil
	case localizationChangedMsg:
		m.dirty = true
		if m.localizer.SetPreferred(msg.preferred) {
			m.applyMenuLocalization()
		}
		m.localeReady = true
		var cmds []tea.Cmd
		var cmd tea.Cmd
		m.games, cmd = m.games.Update(message)
		cmds = append(cmds, cmd)
		m.profile, cmd = m.profile.Update(message)
		cmds = append(cmds, cmd)
		m.about, cmd = m.about.Update(message)
		cmds = append(cmds, cmd)
		if !m.started && m.startupReady() {
			m.started = true
		}
		return m, tea.Batch(cmds...)
	case startupLoadingDelayElapsedMsg:
		m.dirty = true
		m.showLoading = true
		return m, nil
	case sshAudioCopyResetMsg:
		if msg.gen == m.sshAudioCopyGen {
			m.dirty = true
			m.sshAudioCopied = ""
		}
		return m, nil
	default:
		m.dirty = true
		var cmds []tea.Cmd
		var cmd tea.Cmd
		m.games, cmd = m.games.Update(message)
		cmds = append(cmds, cmd)
		m.profile, cmd = m.profile.Update(message)
		cmds = append(cmds, cmd)
		m.about, cmd = m.about.Update(message)
		cmds = append(cmds, cmd)
		m.tabs, cmd = m.tabs.Update(message)
		cmds = append(cmds, cmd)
		if !m.started && m.startupReady() {
			m.started = true
		}
		return m, tea.Batch(cmds...)
	}
}

func (m *model) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	m.dirty = true
	if m.showSSHAudioHelp {
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
		if key.Matches(msg, m.keys.AudioHelp) {
			m.showSSHAudioHelp = false
			return m, nil
		}
		switch {
		case msg.String() == "b":
			return m, m.copySSHAudioCommand(sshAudioCopyBashTarget, sshAudioBashCommand)
		case msg.String() == "f":
			return m, m.copySSHAudioCommand(sshAudioCopyFishTarget, sshAudioFishCommand)
		}
		switch msg.String() {
		case "esc", "enter", " ", "q":
			m.showSSHAudioHelp = false
		}
		return m, nil
	}
	if m.showSSHAudioCallout && key.Matches(msg, m.keys.DismissAudioHelp) && !m.inputCaptured() {
		m.showSSHAudioCallout = false
		m.showSSHAudioHelp = false
		return m, nil
	}
	if m.showSSHAudioCallout && key.Matches(msg, m.keys.AudioHelp) && !m.inputCaptured() {
		m.showSSHAudioHelp = true
		return m, nil
	}
	if key.Matches(msg, m.keys.Quit) {
		if msg.String() != "q" || !m.inputCaptured() {
			return m, tea.Quit
		}
	} else if key.Matches(msg, m.keys.NextTab) {
		if m.inputCaptured() {
			return m, nil
		}
		next := (m.tabs.Active + 1) % len(m.tabs.Tabs)
		return m, m.setActiveTab(next)
	} else if key.Matches(msg, m.keys.PrevTab) {
		if m.inputCaptured() {
			return m, nil
		}
		prev := m.tabs.Active - 1
		if prev < 0 {
			prev = len(m.tabs.Tabs) - 1
		}
		return m, m.setActiveTab(prev)
	}
	var cmd tea.Cmd
	switch m.tabs.ActiveTab().ID {
	case "games":
		m.games, cmd = m.games.Update(msg)
	case "profile":
		m.profile, cmd = m.profile.Update(msg)
	case "about":
		m.about, cmd = m.about.Update(msg)
	}
	return m, cmd
}

func (m *model) handleMouse(msg tea.MouseMsg) (tea.Model, tea.Cmd) {
	if _, ok := msg.(tea.MouseMotionMsg); ok {
		return m.handleMouseMotion(msg)
	}
	m.dirty = true
	if handled, cmd := m.handleSSHAudioMouse(msg); handled {
		return m, cmd
	}
	var cmds []tea.Cmd
	var cmd tea.Cmd
	m.tabs, cmd = m.tabs.Update(msg)
	cmds = append(cmds, cmd)
	switch m.tabs.ActiveTab().ID {
	case "games":
		m.games, cmd = m.games.Update(msg)
		cmds = append(cmds, cmd)
	case "profile":
		m.profile, cmd = m.profile.Update(msg)
		cmds = append(cmds, cmd)
	case "about":
		m.about, cmd = m.about.Update(msg)
		cmds = append(cmds, cmd)
	}
	return m, tea.Batch(cmds...)
}

func (m *model) handleSSHAudioMouse(msg tea.MouseMsg) (bool, tea.Cmd) {
	if m.showSSHAudioHelp {
		return m.handleSSHAudioDialogMouse(msg)
	}
	if m.showSSHAudioCallout {
		return m.handleSSHAudioCalloutMouse(msg)
	}
	return false, nil
}

func (m *model) handleSSHAudioDialogMouse(msg tea.MouseMsg) (bool, tea.Cmd) {
	if !isPrimaryMouseRelease(msg) {
		return true, nil
	}
	switch {
	case m.zone.Get(sshAudioCopyBashID).InBounds(msg):
		return true, m.copySSHAudioCommand(sshAudioCopyBashTarget, sshAudioBashCommand)
	case m.zone.Get(sshAudioCopyFishID).InBounds(msg):
		return true, m.copySSHAudioCommand(sshAudioCopyFishTarget, sshAudioFishCommand)
	case m.zone.Get(sshAudioDialogCloseID).InBounds(msg):
		m.showSSHAudioHelp = false
		m.sshAudioHovered = ""
		return true, nil
	case !m.zone.Get(sshAudioDialogZoneID).InBounds(msg):
		m.showSSHAudioHelp = false
		m.sshAudioHovered = ""
		return true, nil
	default:
		return true, nil
	}
}

func (m *model) handleSSHAudioCalloutMouse(msg tea.MouseMsg) (bool, tea.Cmd) {
	if !isPrimaryMouseRelease(msg) {
		return false, nil
	}
	if m.zone.Get(sshAudioDismissZoneID).InBounds(msg) {
		m.showSSHAudioCallout = false
		m.showSSHAudioHelp = false
		m.sshAudioHovered = ""
		return true, nil
	}
	if m.zone.Get(sshAudioCalloutZoneID).InBounds(msg) {
		m.showSSHAudioHelp = true
		return true, nil
	}
	return false, nil
}

func isPrimaryMouseRelease(msg tea.MouseMsg) bool {
	event, ok := msg.(tea.MouseReleaseMsg)
	return ok && event.Button == tea.MouseLeft
}

func (m *model) copySSHAudioCommand(target string, command string) tea.Cmd {
	m.dirty = true
	m.sshAudioCopied = target
	m.sshAudioCopyGen++
	gen := m.sshAudioCopyGen
	return tea.Batch(
		tea.SetClipboard(command),
		tea.Tick(time.Second, func(time.Time) tea.Msg {
			return sshAudioCopyResetMsg{gen: gen}
		}),
	)
}

func (m *model) handleMouseMotion(msg tea.MouseMsg) (tea.Model, tea.Cmd) {
	if m.showSSHAudioHelp {
		hovered := m.sshAudioHoveredZone(msg)
		if hovered != m.sshAudioHovered {
			m.sshAudioHovered = hovered
			m.dirty = true
		}
		return m, nil
	}
	oldTabHover := m.tabs.Hovered
	m.tabs, _ = m.tabs.Update(msg)

	var cmd tea.Cmd
	switch m.tabs.ActiveTab().ID {
	case "games":
		oldListHover := m.games.list.Hovered
		wasDragging := m.games.carousel.IsDragging()
		oldDetailsScroll := m.games.detailsViewport.scroll
		oldDetailsDragging := m.games.detailsViewport.dragging
		m.games, cmd = m.games.Update(msg)
		if m.games.list.Hovered != oldListHover ||
			m.games.carousel.IsDragging() ||
			wasDragging ||
			m.games.detailsViewport.scroll != oldDetailsScroll ||
			m.games.detailsViewport.dragging != oldDetailsDragging {
			m.dirty = true
		}
	case "profile":
		oldHover := m.profile.hovered
		m.profile, cmd = m.profile.Update(msg)
		if m.profile.hovered != oldHover {
			m.dirty = true
		}
	case "about":
		oldScroll := m.about.viewport.scroll
		oldDragging := m.about.viewport.dragging
		m.about, cmd = m.about.Update(msg)
		if m.about.viewport.scroll != oldScroll || m.about.viewport.dragging != oldDragging {
			m.dirty = true
		}
	}
	if m.tabs.Hovered != oldTabHover {
		m.dirty = true
	}
	return m, cmd
}

func (m *model) View() tea.View {
	var v tea.View
	v.AltScreen = true
	v.MouseMode = tea.MouseModeAllMotion
	if !m.dirty && m.viewCache != "" {
		v.SetContent(m.viewCache)
		return v
	}
	m.dirty = false
	m.viewCache = m.renderView()
	v.SetContent(m.viewCache)
	return v
}

func (m *model) renderView() string {
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
	title := m.titleStyle.Render(" " + m.localizer.HeaderTitle())
	titleWidth := lipgloss.Width(title)

	viewportWidth := min(m.w, maxWidth)
	if !m.canRenderViewport(viewportWidth, titleWidth, tabsWidth) {
		return lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Center, m.localizer.WindowTooSmall())
	}

	centeredTabsView := m.renderCenteredTabsView(viewportWidth, tabsView, tabsWidth, title, titleWidth)
	sshAudioCalloutView := m.renderSSHAudioCallout(viewportWidth)
	helpView := m.renderHelpView(viewportWidth)
	layout := m.computeContentLayout(
		viewportWidth,
		lipgloss.Height(centeredTabsView)+lipgloss.Height(sshAudioCalloutView),
		lipgloss.Height(helpView),
	)

	content := m.renderActiveTab(layout.innerWidth, layout.innerHeight)

	styledContent := m.contentArea.
		Padding(layout.paddingY, layout.paddingX).
		Width(viewportWidth).
		Height(layout.height).
		Render(content)

	parts := []string{centeredTabsView}
	if sshAudioCalloutView != "" {
		parts = append(parts, sshAudioCalloutView)
	}
	parts = append(parts, styledContent, helpView)
	fullView := lipgloss.JoinVertical(lipgloss.Left, parts...)
	placed := lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Top, fullView)
	if m.showSSHAudioHelp {
		placed = m.renderSSHAudioDialog(placed)
	}
	return m.zone.Scan(placed)
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
	m.help.SetWidth(innerWidth)

	helpLines := make([]string, 0, 2)
	if keyMap := m.contextualKeyMap(); keyMap != nil {
		helpLines = append(helpLines, lipgloss.NewStyle().Width(innerWidth).Render(m.help.View(keyMap)))
	}
	if m.showSSHAudioHelp {
		helpLines = append(helpLines, lipgloss.NewStyle().Width(innerWidth).Render(m.renderSSHAudioDialogHelp(innerWidth)))
	} else if keyMap := m.globalHelpKeyMap(); keyMap != nil {
		helpLines = append(helpLines, lipgloss.NewStyle().Width(innerWidth).Render(m.help.View(keyMap)))
	}
	for len(helpLines) < 2 {
		helpLines = append(helpLines, lipgloss.NewStyle().Width(innerWidth).Render(""))
	}
	return lipgloss.NewStyle().Padding(1, helpPaddingX).Render(strings.Join(helpLines, "\n"))
}

func (m *model) renderSSHAudioCallout(viewportWidth int) string {
	if !m.showSSHAudioCallout {
		return ""
	}
	calloutStyle := lipgloss.NewStyle().
		Width(viewportWidth).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(theme.Line).
		Padding(0, 1)
	contentWidth := max(1, viewportWidth-calloutStyle.GetHorizontalFrameSize())

	dismiss := m.zone.Mark(
		sshAudioDismissZoneID,
		lipgloss.NewStyle().
			Foreground(theme.TextSubtle).
			Bold(true).
			Render("×"),
	)
	callout := calloutStyle.Render(renderPinnedRow(
		contentWidth,
		lipgloss.NewStyle().
			Foreground(theme.Text).
			Bold(true).
			Render(m.localizer.SSHAudioCallout()),
		dismiss,
	))
	return m.zone.Mark(sshAudioCalloutZoneID, callout)
}

func (m *model) renderSSHAudioDialog(_ string) string {
	dialogWidth := min(sshAudioDialogMaxWidth, max(sshAudioDialogMinWidth, m.w-8))
	dialogWidth = min(dialogWidth, max(36, m.w-4))
	dialogStyle := lipgloss.NewStyle().
		Width(dialogWidth).
		Foreground(theme.Text).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(theme.Line).
		Padding(1, 2).
		MaxWidth(m.w - 4)
	contentWidth := max(1, dialogWidth-dialogStyle.GetHorizontalFrameSize())

	titleStyle := lipgloss.NewStyle().
		Foreground(theme.Text).
		Bold(true)
	bodyStyle := lipgloss.NewStyle().
		Foreground(theme.TextMuted).
		Width(contentWidth)
	commandLabelStyle := lipgloss.NewStyle().
		Foreground(theme.TextMuted).
		Bold(true)
	closeStyle := lipgloss.NewStyle().
		Foreground(theme.TextSubtle).
		Bold(true)
	closeButton := m.zone.Mark(
		sshAudioDialogCloseID,
		closeStyle.Render("×"),
	)

	dialog := dialogStyle.Render(lipgloss.JoinVertical(
		lipgloss.Left,
		renderPinnedRow(contentWidth, titleStyle.Render(m.localizer.SSHAudioDialogTitle()), closeButton),
		"",
		bodyStyle.Render(m.localizer.SSHAudioDialogInstall()),
		"",
		bodyStyle.Render(m.localizer.SSHAudioDialogBody()),
		"",
		m.renderSSHAudioCommandBlock(
			contentWidth,
			commandLabelStyle.Render(m.localizer.SSHAudioDialogBashLabel()),
			sshAudioBashCommand,
			m.zone.Mark(sshAudioCopyBashID, m.renderSSHAudioCopyButton(sshAudioCopyBashTarget)),
		),
		m.renderSSHAudioCommandBlock(
			contentWidth,
			commandLabelStyle.Render(m.localizer.SSHAudioDialogFishLabel()),
			sshAudioFishCommand,
			m.zone.Mark(sshAudioCopyFishID, m.renderSSHAudioCopyButton(sshAudioCopyFishTarget)),
		),
	))

	return lipgloss.Place(m.w, m.h, lipgloss.Center, lipgloss.Center, m.zone.Mark(sshAudioDialogZoneID, dialog))
}

func (m *model) renderSSHAudioCommandBlock(width int, label string, command string, action string) string {
	blockStyle := lipgloss.NewStyle().
		Width(width).
		Border(lipgloss.NormalBorder()).
		BorderForeground(theme.Line).
		Padding(0, 1)
	contentWidth := max(1, width-blockStyle.GetHorizontalFrameSize())
	commandStyle := lipgloss.NewStyle().
		Foreground(theme.Text).
		Width(contentWidth)

	return blockStyle.Render(lipgloss.JoinVertical(
		lipgloss.Left,
		renderPinnedRow(contentWidth, label, action),
		"",
		commandStyle.Render(command),
	))
}

func (m model) sshAudioCopyLabel(target string) string {
	if m.sshAudioCopied == target {
		return m.localizer.SSHAudioDialogCopied()
	}
	return m.localizer.SSHAudioDialogCopy()
}

func (m model) renderSSHAudioDialogHelp(width int) string {
	dialogHelp := help.New()
	dialogHelp.SetWidth(width)
	return dialogHelp.View(helpBindings{
		short: []key.Binding{
			key.NewBinding(
				key.WithKeys("b"),
				key.WithHelp("b", m.localizer.SSHAudioDialogCopyBashHelp()),
			),
			key.NewBinding(
				key.WithKeys("f"),
				key.WithHelp("f", m.localizer.SSHAudioDialogCopyFishHelp()),
			),
			key.NewBinding(
				key.WithKeys("esc"),
				key.WithHelp("esc", m.localizer.SSHAudioDialogCloseHelp()),
			),
		},
	})
}

func (m model) renderSSHAudioCopyButton(target string) string {
	style := lipgloss.NewStyle().
		Foreground(theme.Text).
		Bold(true).
		Padding(0, 2)
	switch {
	case m.sshAudioCopied == target:
		style = style.Foreground(theme.OnPrimary).Background(theme.Primary)
	case m.sshAudioHovered == target:
		style = style.Background(theme.Line)
	default:
		style = style.Background(theme.Surface)
	}
	return style.Render(m.sshAudioCopyLabel(target))
}

func (m model) sshAudioHoveredZone(msg tea.MouseMsg) string {
	switch {
	case m.zone.Get(sshAudioCopyBashID).InBounds(msg):
		return sshAudioCopyBashTarget
	case m.zone.Get(sshAudioCopyFishID).InBounds(msg):
		return sshAudioCopyFishTarget
	case m.zone.Get(sshAudioDialogCloseID).InBounds(msg):
		return sshAudioDialogCloseID
	default:
		return ""
	}
}

func renderPinnedRow(width int, left string, right string) string {
	if right == "" {
		return lipgloss.NewStyle().Width(width).Render(left)
	}
	leftWidth := lipgloss.Width(left)
	rightWidth := lipgloss.Width(right)
	if leftWidth+rightWidth > width {
		return lipgloss.JoinVertical(
			lipgloss.Left,
			lipgloss.NewStyle().Width(width).Render(left),
			lipgloss.PlaceHorizontal(width, lipgloss.Right, right),
		)
	}
	gap := width - leftWidth - rightWidth
	return left + strings.Repeat(" ", gap) + right
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
		return m.localizer.UnknownTab()
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
	loading := m.localizer.ProfileLoading()
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
	m.about.applyLocalization(m.localizer)
	m.tabs = tabs.NewWithActive([]tabs.Tab{
		{ID: "games", Title: m.localizer.TabGames()},
		{ID: "profile", Title: m.localizer.TabProfile()},
		{ID: "about", Title: m.localizer.TabAbout()},
	}, m.zone, "menu-tab-", activeID)
}

func (m *model) applyThemeStyles() {
	m.barStyle = lipgloss.NewStyle().Foreground(theme.Line)
	m.games.styles = defaultDetailsStyles()
	m.games.list.Styles = gamelist.DefaultStyles()
	m.games.carousel.Styles = carousel.DefaultStyles()
	m.profile.styles = defaultProfileStyles()
	m.applyMenuLocalization()
}
