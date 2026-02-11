// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"strings"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"
	"github.com/terminal-games/terminal-games/cmd/menu/carousel"
	"github.com/terminal-games/terminal-games/cmd/menu/gamelist"
	"github.com/terminal-games/terminal-games/cmd/menu/tabs"
	"github.com/terminal-games/terminal-games/cmd/menu/theme"
	"github.com/terminal-games/terminal-games/pkg/app"
	"golang.org/x/text/language"
)

const (
	playZoneID = "games-play"
)

func resolveLocalized(preferred []language.Tag, localized map[string]string, fallback string) string {
	if locale := negotiateLocale(preferred, localized); locale != "" {
		if text, ok := localized[locale]; ok {
			return text
		}
	}
	if text, ok := localized["en"]; ok {
		return text
	}
	for _, text := range localized {
		return text
	}
	return fallback
}

type gameDetails struct {
	Name          map[string]string                `json:"name"`
	Description   map[string]string                `json:"description"`
	Details       map[string]string                `json:"details"`
	ScreenshotsBy map[string][]carousel.Screenshot `json:"screenshots"`
}

type gameData struct {
	ID        int64       `json:"id"`
	Shortname string      `json:"shortname"`
	Details   gameDetails `json:"details"`
}

type gameItem struct {
	ID          int64
	Shortname   string
	Name        string
	Description string
	Details     string
	Screenshots []carousel.Screenshot
}

type gamesDataMsg struct {
	games []gameData
	err   error
}

type gamesModel struct {
	zone      *zone.Manager
	rawGames  []gameData
	items     []gameItem
	list      gamelist.Model
	carousel  carousel.Model
	styles    gamesDetailsStyles
	termW     int
	termH     int
	tag       language.Tag
	localizer localizer
	loadErr   error
	loaded    bool
	playKey   key.Binding
	playBusy  bool
	spin      spinner.Model
	playError string
	preferred []language.Tag
}

type gamesDetailsStyles struct {
	Title    lipgloss.Style
	Body     lipgloss.Style
	Subtle   lipgloss.Style
	PlayBtn  lipgloss.Style
	PlayLoad lipgloss.Style
	Error    lipgloss.Style
}

func defaultDetailsStyles() gamesDetailsStyles {
	return gamesDetailsStyles{
		Title:    lipgloss.NewStyle().Bold(true),
		Body:     lipgloss.NewStyle(),
		Subtle:   lipgloss.NewStyle().Foreground(theme.TextMuted),
		PlayBtn:  lipgloss.NewStyle().Foreground(theme.OnPrimary).Background(theme.Primary).Bold(true),
		PlayLoad: lipgloss.NewStyle().Foreground(theme.TextMuted),
		Error:    lipgloss.NewStyle().Foreground(theme.Danger),
	}
}

func newGamesModel(zoneManager *zone.Manager) gamesModel {
	m := gamesModel{
		zone:      zoneManager,
		list:      gamelist.New("", nil, zoneManager, "games-item-"),
		carousel:  carousel.New(zoneManager),
		styles:    defaultDetailsStyles(),
		tag:       language.English,
		localizer: newLocalizer(),
		preferred: []language.Tag{language.English},
	}
	m.spin = spinner.New(spinner.WithSpinner(spinner.Dot))
	m.applyLocalization(m.localizer, m.preferred)
	return m
}

func (m gamesModel) ShortHelp() []key.Binding {
	b := m.list.ShortHelp()
	if !m.list.Filtering() && !m.playBusy && m.selectedItem().Shortname != "" {
		b = append(b, m.playKey)
	}
	return b
}

func (m gamesModel) FullHelp() [][]key.Binding { return [][]key.Binding{m.ShortHelp()} }
func (m gamesModel) Capturing() bool {
	return m.list.Filtering() || m.playBusy || m.carousel.Modal
}
func (m gamesModel) Init() tea.Cmd { return tea.Batch(m.carousel.Init(), m.fetchGamesData()) }

func (m gamesModel) fetchGamesData() tea.Cmd {
	return func() tea.Msg {
		games, err := menuFetchGames()
		if err != nil {
			return gamesDataMsg{err: err}
		}
		return gamesDataMsg{games: games}
	}
}

func (m gamesModel) Update(msg tea.Msg) (gamesModel, tea.Cmd) {
	switch msg := msg.(type) {
	case gamesDataMsg:
		m.loaded = true
		m.loadErr = msg.err
		if msg.err == nil {
			m.rawGames = msg.games
		}
		return m, m.applyLocalization(m.localizer, m.preferred)
	case localizationChangedMsg:
		localizerChanged := m.localizer.SetPreferred(msg.preferred)
		preferredChanged := !sameLanguageTags(m.preferred, msg.preferred)
		if localizerChanged || preferredChanged {
			return m, m.applyLocalization(m.localizer, msg.preferred)
		}
		return m, nil
	case spinner.TickMsg:
		if !m.playBusy {
			return m, nil
		}
		if app.Ready() {
			return m, tea.Quit
		}
		var cmd tea.Cmd
		m.spin, cmd = m.spin.Update(msg)
		return m, cmd
	case tabs.TabChangedMsg:
		if msg.Tab.ID == "games" {
			return m, m.onActivated()
		}
		return m, nil
	case tea.WindowSizeMsg:
		m.termW = msg.Width
		m.termH = msg.Height
		m.carousel.HandleWindowSize(msg.Width, msg.Height)
		return m, nil
	case tea.KeyMsg:
		if m.playBusy {
			return m, nil
		}
		if key.Matches(msg, m.playKey) && !m.list.Filtering() {
			return m.startPlaySelected()
		}
		if m.carousel.Modal {
			if msg.Type == tea.KeyEsc {
				m.carousel.Modal = false
			}
			return m, nil
		}
		prev := m.list.SelectedIndex()
		var cmd tea.Cmd
		m.list, cmd = m.list.Update(msg)
		if m.list.SelectedIndex() != prev {
			cmd = tea.Batch(cmd, m.syncCarouselToSelection())
		}
		return m, cmd
	case tea.MouseMsg:
		if m.playBusy {
			return m, nil
		}
		if msg.Action == tea.MouseActionRelease && m.zone != nil && m.zone.Get(playZoneID).InBounds(msg) {
			return m.startPlaySelected()
		}
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
		result, cc, handled := m.carousel.HandleMouse(msg)
		m.carousel = result
		if handled {
			return m, cc
		}
		if msg.Action == tea.MouseActionRelease && m.carousel.BtnClicked(msg) {
			m.carousel.Modal = true
			return m, nil
		}
		prev := m.list.SelectedIndex()
		var lc tea.Cmd
		m.list, lc = m.list.HandleMouse(msg)
		if m.list.SelectedIndex() != prev {
			return m, tea.Batch(lc, m.syncCarouselToSelection())
		}
		return m, lc
	}
	var lc, cc tea.Cmd
	m.list, lc = m.list.Update(msg)
	m.carousel, cc = m.carousel.Update(msg)
	return m, tea.Batch(lc, cc)
}

func (m gamesModel) startPlaySelected() (gamesModel, tea.Cmd) {
	item := m.selectedItem()
	if item.Shortname == "" {
		return m, nil
	}
	if err := app.Change(item.Shortname); err != nil {
		m.playError = err.Error()
		return m, nil
	}
	m.playError = ""
	m.playBusy = true
	return m, m.spin.Tick
}

func (m *gamesModel) syncCarouselToSelection() tea.Cmd {
	idx := m.list.SelectedIndex()
	if idx >= 0 && idx < len(m.items) {
		m.carousel.Screenshots = m.items[idx].Screenshots
	} else {
		m.carousel.Screenshots = nil
	}
	return m.carousel.Reset()
}

func (m *gamesModel) onActivated() tea.Cmd {
	return m.carousel.RestartAuto()
}

func (m gamesModel) selectedItem() gameItem {
	idx := m.list.SelectedIndex()
	if idx < 0 || idx >= len(m.items) {
		return gameItem{}
	}
	return m.items[idx]
}

func negotiateLocale(preferred []language.Tag, byLang map[string]string) string {
	if len(byLang) == 0 {
		return ""
	}
	if len(preferred) == 0 {
		preferred = []language.Tag{language.English}
	}
	tags := make([]language.Tag, 0, len(byLang))
	keys := make([]string, 0, len(byLang))
	for locale := range byLang {
		tag, err := language.Parse(locale)
		if err != nil {
			continue
		}
		tags = append(tags, tag)
		keys = append(keys, locale)
	}
	if len(tags) == 0 {
		if _, ok := byLang["en"]; ok {
			return "en"
		}
		for locale := range byLang {
			return locale
		}
		return ""
	}
	_, idx, _ := language.NewMatcher(tags).Match(preferred...)
	return keys[idx]
}

func resolveScreenshots(preferred []language.Tag, byLang map[string][]carousel.Screenshot) []carousel.Screenshot {
	if len(byLang) == 0 {
		return nil
	}
	m := make(map[string]string, len(byLang))
	for locale := range byLang {
		m[locale] = locale
	}
	if locale := negotiateLocale(preferred, m); locale != "" {
		if shots, ok := byLang[locale]; ok {
			return shots
		}
	}
	if shots, ok := byLang["en"]; ok {
		return shots
	}
	for _, shots := range byLang {
		return shots
	}
	return nil
}

func (m *gamesModel) applyLocalization(localizer localizer, preferred []language.Tag) tea.Cmd {
	m.localizer = localizer
	m.tag = localizer.tag
	if len(preferred) == 0 {
		m.preferred = []language.Tag{m.tag}
	} else {
		m.preferred = append([]language.Tag(nil), preferred...)
	}
	m.playKey = key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", localizer.Text(textHelpPlay)))
	m.carousel.SetLabels(localizer.CarouselLabels())

	selectedShortname := ""
	if i := m.list.SelectedIndex(); i >= 0 && i < len(m.items) {
		selectedShortname = m.items[i].Shortname
	}

	m.items = make([]gameItem, len(m.rawGames))
	listItems := make([]gamelist.Item, len(m.rawGames))
	for i := range m.rawGames {
		d := m.rawGames[i].Details
		item := gameItem{
			ID:          m.rawGames[i].ID,
			Shortname:   m.rawGames[i].Shortname,
			Name:        resolveLocalized(m.preferred, d.Name, m.rawGames[i].Shortname),
			Description: resolveLocalized(m.preferred, d.Description, ""),
			Details:     resolveLocalized(m.preferred, d.Details, ""),
			Screenshots: resolveScreenshots(m.preferred, d.ScreenshotsBy),
		}
		m.items[i] = item
		listItems[i] = gamelist.Item{Name: item.Name, Description: item.Description, FilterExtra: item.Details}
	}
	m.list = gamelist.New(localizer.Text(textGamesListTitle), listItems, m.zone, "games-item-")
	m.list.SetLabels(localizer.GameListLabels())
	for i := range m.items {
		if m.items[i].Shortname == selectedShortname {
			m.list.Selected = i
			break
		}
	}
	return m.syncCarouselToSelection()
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
	if !m.loaded {
		return lipgloss.NewStyle().Width(width).Height(height).Render(m.styles.Subtle.Render(m.localizer.Text(textProfileLoading)))
	}
	if m.loadErr != nil {
		return lipgloss.NewStyle().Width(width).Height(height).Render(m.styles.Subtle.Render(m.loadErr.Error()))
	}
	item := m.selectedItem()
	if item.Shortname == "" {
		return lipgloss.NewStyle().Width(width).Height(height).Render(m.styles.Subtle.Render(m.localizer.Text(textGamesNoMatch)))
	}
	title := m.styles.Title.Render(item.Name)
	desc := m.styles.Subtle.Render(item.Description)
	body := m.styles.Body.Render(item.Details)
	play := m.renderPlayButton(width, m.localizer.Text(textPlayButton))
	if m.playBusy {
		play = m.renderPlayButton(width, m.spin.View()+" "+m.localizer.Text(textPlayLoading))
	}
	if m.zone != nil {
		play = m.zone.Mark(playZoneID, play)
	}

	parts := []string{title, desc, "", body}
	if len(item.Screenshots) > 0 {
		if carousel.CanFitInline(width, height) {
			parts = append(parts, "\n"+m.carousel.View(width))
		} else if carousel.CanFitModal(m.termW, m.termH) {
			parts = append(parts, "", m.carousel.ViewButton())
		}
	}
	parts = append(parts, "", play)
	if m.playError != "" {
		parts = append(parts, m.styles.Error.Render(m.playError))
	}
	return lipgloss.NewStyle().Width(width).Height(height).Render(strings.Join(parts, "\n"))
}

func (m *gamesModel) renderPlayButton(width int, label string) string {
	if width <= 0 {
		return ""
	}
	return m.styles.PlayBtn.Render(lipgloss.Place(width, 3, lipgloss.Center, lipgloss.Center, label))
}
