// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"
	"github.com/terminal-games/terminal-games/cmd/menu/theme"
	"golang.org/x/text/language"
	"golang.org/x/text/language/display"
)

type profileState int

const (
	stateNormal profileState = iota
	stateEditName
	stateEditLangs
	stateLangDropdown
	stateConfirmDelete
)

type profileSection int

const (
	sectionUsername profileSection = iota
	sectionLanguages
	sectionReplays
)

var languageCodes = []string{
	"ar", "bg", "bn", "ca", "cs", "da", "de", "el", "en", "es", "et",
	"fa", "fi", "fr", "he", "hi", "hr", "hu", "id", "is", "it", "ja",
	"ko", "lt", "lv", "ms", "nl", "no", "pl", "pt", "ro", "ru", "sk",
	"sl", "sr", "sv", "sw", "ta", "te", "th", "tr", "uk", "ur", "vi", "zh",
}

var availableLanguages []language.Tag

func init() {
	for _, code := range languageCodes {
		availableLanguages = append(availableLanguages, language.Make(code))
	}
	sort.Slice(availableLanguages, func(i, j int) bool {
		return langName(availableLanguages[i]) < langName(availableLanguages[j])
	})
}

func langName(tag language.Tag) string {
	if name := display.Self.Name(tag); name != "" {
		return name
	}
	return tag.String()
}

func langLabel(tag language.Tag) string {
	return langName(tag) + " (" + tag.String() + ")"
}

func languageOptions(exclude []language.Tag) []dropdownOption {
	set := make(map[string]bool, len(exclude))
	for _, t := range exclude {
		set[t.String()] = true
	}
	var opts []dropdownOption
	for _, tag := range availableLanguages {
		if !set[tag.String()] {
			opts = append(opts, dropdownOption{tag.String(), langLabel(tag)})
		}
	}
	return opts
}

func buildAcceptLanguage(tags []language.Tag) string {
	var b strings.Builder
	for i, tag := range tags {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tag.String())
		if i > 0 {
			fmt.Fprintf(&b, ";q=%.1f", max(0.1, 1.0-float64(i)*0.1))
		}
	}
	return b.String()
}

func osc8Link(url, text string) string {
	if url == "" {
		return text
	}
	return "\033]8;;" + url + "\007" + text + "\033]8;;\007"
}

type inlineEditor struct {
	runes       []rune
	pos         int
	placeholder string
	maxLen      int
}

func (e *inlineEditor) SetValue(s string) {
	e.runes = []rune(s)
	e.pos = len(e.runes)
}

func (e inlineEditor) Value() string { return string(e.runes) }

func (e *inlineEditor) Insert(r rune) {
	if e.maxLen > 0 && len(e.runes) >= e.maxLen {
		return
	}
	e.runes = append(e.runes, 0)
	copy(e.runes[e.pos+1:], e.runes[e.pos:])
	e.runes[e.pos] = r
	e.pos++
}

func (e *inlineEditor) Backspace() {
	if e.pos > 0 {
		e.runes = append(e.runes[:e.pos-1], e.runes[e.pos:]...)
		e.pos--
	}
}

func (e *inlineEditor) Delete() {
	if e.pos < len(e.runes) {
		e.runes = append(e.runes[:e.pos], e.runes[e.pos+1:]...)
	}
}

func (e *inlineEditor) Left()  { e.pos = max(0, e.pos-1) }
func (e *inlineEditor) Right() { e.pos = min(len(e.runes), e.pos+1) }
func (e *inlineEditor) Home()  { e.pos = 0 }
func (e *inlineEditor) End()   { e.pos = len(e.runes) }

var editorCursor = lipgloss.NewStyle().Reverse(true)

func (e inlineEditor) View(valueStyle, hintStyle lipgloss.Style) string {
	if len(e.runes) == 0 {
		return editorCursor.Render(" ") + hintStyle.Render(e.placeholder)
	}
	var b strings.Builder
	b.WriteString(valueStyle.Render(string(e.runes[:e.pos])))
	if e.pos < len(e.runes) {
		b.WriteString(editorCursor.Render(string(e.runes[e.pos : e.pos+1])))
		b.WriteString(valueStyle.Render(string(e.runes[e.pos+1:])))
	} else {
		b.WriteString(editorCursor.Render(" "))
	}
	return b.String()
}

type dropdownOption struct {
	value, label string
}

type dropdown struct {
	options    []dropdownOption
	filtered   []int
	cursor     int
	scroll     int
	maxVisible int
	searchBuf  []rune
}

func newDropdown(options []dropdownOption) dropdown {
	d := dropdown{options: options, maxVisible: 5}
	d.filter()
	return d
}

func (d *dropdown) filter() {
	d.filtered = d.filtered[:0]
	q := strings.ToLower(string(d.searchBuf))
	for i, opt := range d.options {
		if q == "" || strings.Contains(strings.ToLower(opt.label), q) {
			d.filtered = append(d.filtered, i)
		}
	}
}

func (d *dropdown) ensureVisible() {
	d.scroll = max(min(d.scroll, d.cursor), d.cursor-d.maxVisible+1)
}

func (d *dropdown) TypeRune(r rune) {
	d.searchBuf = append(d.searchBuf, r)
	d.filter()
	d.cursor, d.scroll = 0, 0
}

func (d *dropdown) Backspace() {
	if len(d.searchBuf) > 0 {
		d.searchBuf = d.searchBuf[:len(d.searchBuf)-1]
		d.filter()
		d.cursor, d.scroll = 0, 0
	}
}

func (d *dropdown) MoveUp() {
	if d.cursor > 0 {
		d.cursor--
		d.ensureVisible()
	}
}

func (d *dropdown) MoveDown() {
	if d.cursor < len(d.filtered)-1 {
		d.cursor++
		d.ensureVisible()
	}
}

func (d dropdown) Selected() (dropdownOption, bool) {
	if d.cursor >= len(d.filtered) {
		return dropdownOption{}, false
	}
	return d.options[d.filtered[d.cursor]], true
}

type replay struct {
	createdAt    int64
	asciinemaURL string
	gameTitle    string
}

type profileDataMsg struct {
	username, locale string
	replays          []replay
	err              error
}

type profileSavedMsg struct{ err error }
type replayDeletedMsg struct{ err error }

type profileModel struct {
	db     *sql.DB
	zone   *zone.Manager
	keys   profileKeyMap
	styles profileStyles
	userID string
	tr     localizer

	loaded  bool
	loadErr error
	state   profileState
	section profileSection
	hovered string

	username string
	editor   inlineEditor

	languages  []language.Tag
	langCursor int
	dropdown   dropdown

	replays      []replay
	replayCursor int
}

type profileKeyMap struct {
	Up, Down, Edit, Delete, MoveUp, MoveDown key.Binding
}

func newProfileKeyMap() profileKeyMap {
	return newProfileKeyMapWithLocalizer(newLocalizer())
}

func newProfileKeyMapWithLocalizer(localizer localizer) profileKeyMap {
	return profileKeyMap{
		Up:       key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("↑/k", localizer.Text(textHelpUp))),
		Down:     key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("↓/j", localizer.Text(textHelpDown))),
		Edit:     key.NewBinding(key.WithKeys("enter", "e"), key.WithHelp("enter", localizer.Text(textHelpEdit))),
		Delete:   key.NewBinding(key.WithKeys("d", "x"), key.WithHelp("d", localizer.Text(textHelpDelete))),
		MoveUp:   key.NewBinding(key.WithKeys("K"), key.WithHelp("shift+k", localizer.Text(textHelpMoveUp))),
		MoveDown: key.NewBinding(key.WithKeys("J"), key.WithHelp("shift+j", localizer.Text(textHelpMoveDown))),
	}
}

func newProfileModel(zoneManager *zone.Manager, db *sql.DB) profileModel {
	return profileModel{
		db:     db,
		zone:   zoneManager,
		keys:   newProfileKeyMap(),
		styles: defaultProfileStyles(),
		userID: os.Getenv("USER_ID"),
		tr:     newLocalizer(),
		editor: inlineEditor{placeholder: newLocalizer().Text(textProfileNameHint), maxLen: 32},
	}
}

func (m *profileModel) applyLocalization(localizer localizer) {
	m.tr = localizer
	m.keys = newProfileKeyMapWithLocalizer(localizer)
	m.editor.placeholder = localizer.Text(textProfileNameHint)
}

func (m profileModel) Capturing() bool { return m.state != stateNormal }
func (m profileModel) Init() tea.Cmd   { return m.fetchProfileData() }

func (m profileModel) fetchProfileData() tea.Cmd {
	return func() tea.Msg {
		if m.userID == "" || m.db == nil {
			return profileDataMsg{err: fmt.Errorf("not signed in")}
		}
		var d profileDataMsg
		if err := m.db.QueryRow("SELECT username, locale FROM users WHERE id = ?", m.userID).Scan(&d.username, &d.locale); err != nil {
			return profileDataMsg{err: err}
		}
		rows, err := m.db.Query(
			"SELECT r.created_at, r.asciinema_url, COALESCE(gl.title, g.shortname) FROM replays r LEFT JOIN games g ON r.game_id = g.id LEFT JOIN game_localizations gl ON gl.game_id = g.id AND gl.locale = ? WHERE r.user_id = ? ORDER BY r.created_at DESC",
			m.tr.tag.String(),
			m.userID,
		)
		if err != nil {
			return d
		}
		defer rows.Close()
		for rows.Next() {
			var r replay
			if rows.Scan(&r.createdAt, &r.asciinemaURL, &r.gameTitle) == nil {
				d.replays = append(d.replays, r)
			}
		}
		return d
	}
}

func (m profileModel) saveUsername(username string) tea.Cmd {
	return func() tea.Msg {
		_, err := m.db.Exec("UPDATE users SET username = ? WHERE id = ?", username, m.userID)
		return profileSavedMsg{err: err}
	}
}

func (m profileModel) saveLanguages() tea.Cmd {
	locale := buildAcceptLanguage(m.languages)
	return func() tea.Msg {
		_, err := m.db.Exec("UPDATE users SET locale = ? WHERE id = ?", locale, m.userID)
		return profileSavedMsg{err: err}
	}
}

func (m profileModel) deleteReplay(createdAt int64) tea.Cmd {
	return func() tea.Msg {
		_, err := m.db.Exec("DELETE FROM replays WHERE user_id = ? AND created_at = ?", m.userID, createdAt)
		return replayDeletedMsg{err: err}
	}
}

func (m profileModel) ShortHelp() []key.Binding {
	switch m.state {
	case stateEditName:
		return []key.Binding{
			key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", m.tr.Text(textHelpSave))),
			key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", m.tr.Text(textHelpCancel))),
		}
	case stateEditLangs:
		b := []key.Binding{m.keys.Up, m.keys.Down}
		if m.langCursor < len(m.languages) {
			b = append(b, m.keys.MoveUp, m.keys.MoveDown, m.keys.Delete)
		}
		return append(b, m.keys.Edit, key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", m.tr.Text(textHelpDone))))
	case stateLangDropdown:
		return []key.Binding{
			key.NewBinding(key.WithKeys("up", "down"), key.WithHelp("↑↓", m.tr.Text(textHelpNavigate))),
			key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", m.tr.Text(textHelpSelect))),
			key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", m.tr.Text(textHelpBack))),
		}
	case stateConfirmDelete:
		return []key.Binding{
			key.NewBinding(key.WithKeys("y"), key.WithHelp("y", m.tr.Text(textHelpConfirm))),
			key.NewBinding(key.WithKeys("n"), key.WithHelp("n/esc", m.tr.Text(textHelpCancel))),
		}
	}
	switch m.section {
	case sectionUsername:
		return []key.Binding{m.keys.Down, m.keys.Edit}
	case sectionLanguages:
		return []key.Binding{m.keys.Up, m.keys.Down, m.keys.Edit}
	case sectionReplays:
		b := []key.Binding{m.keys.Up, m.keys.Down}
		if len(m.replays) > 0 {
			b = append(b, m.keys.Delete)
		}
		return b
	}
	return nil
}

func (m profileModel) FullHelp() [][]key.Binding { return [][]key.Binding{m.ShortHelp()} }

func (m profileModel) Update(msg tea.Msg) (profileModel, tea.Cmd) {
	switch msg := msg.(type) {
	case profileDataMsg:
		m.loaded = true
		if msg.err != nil {
			m.loadErr = msg.err
			return m, nil
		}
		m.loadErr = nil
		m.username = msg.username
		m.replays = msg.replays
		if msg.locale != "" {
			if tags, _, err := language.ParseAcceptLanguage(msg.locale); err == nil && len(tags) > 0 {
				m.languages = tags
			}
		}
		if len(m.languages) == 0 {
			m.languages = []language.Tag{language.English}
		}
		m.replayCursor = min(m.replayCursor, max(0, len(m.replays)-1))
		m.langCursor = min(m.langCursor, len(m.languages))
		return m, nil

	case profileSavedMsg:
		return m, nil

	case replayDeletedMsg:
		if msg.err == nil {
			return m, m.fetchProfileData()
		}
		return m, nil

	case tea.KeyMsg:
		switch m.state {
		case stateEditName:
			return m.handleEditKey(msg)
		case stateEditLangs:
			return m.handleLangsKey(msg)
		case stateLangDropdown:
			return m.handleDropdownKey(msg)
		case stateConfirmDelete:
			return m.handleDeleteConfirm(msg)
		default:
			return m.handleKey(msg)
		}

	case tea.MouseMsg:
		return m.handleMouse(msg)
	}
	return m, nil
}

func (m profileModel) handleEditKey(msg tea.KeyMsg) (profileModel, tea.Cmd) {
	switch msg.String() {
	case "enter":
		m.state = stateNormal
		m.username = m.editor.Value()
		return m, m.saveUsername(m.username)
	case "esc":
		m.state = stateNormal
	case "left":
		m.editor.Left()
	case "right":
		m.editor.Right()
	case "home", "ctrl+a":
		m.editor.Home()
	case "end", "ctrl+e":
		m.editor.End()
	case "backspace":
		m.editor.Backspace()
	case "delete":
		m.editor.Delete()
	default:
		if r := []rune(msg.String()); len(r) == 1 && unicode.IsPrint(r[0]) {
			m.editor.Insert(r[0])
		}
	}
	return m, nil
}

func (m profileModel) handleLangsKey(msg tea.KeyMsg) (profileModel, tea.Cmd) {
	if msg.String() == "esc" {
		m.state = stateNormal
		return m, nil
	}
	switch {
	case key.Matches(msg, m.keys.Down):
		m.langCursor = min(m.langCursor+1, len(m.languages))
	case key.Matches(msg, m.keys.Up):
		m.langCursor = max(m.langCursor-1, 0)
	case key.Matches(msg, m.keys.MoveUp):
		if m.langCursor > 0 && m.langCursor < len(m.languages) {
			m.languages[m.langCursor], m.languages[m.langCursor-1] = m.languages[m.langCursor-1], m.languages[m.langCursor]
			m.langCursor--
			return m, m.saveLanguages()
		}
	case key.Matches(msg, m.keys.MoveDown):
		if m.langCursor < len(m.languages)-1 {
			m.languages[m.langCursor], m.languages[m.langCursor+1] = m.languages[m.langCursor+1], m.languages[m.langCursor]
			m.langCursor++
			return m, m.saveLanguages()
		}
	case key.Matches(msg, m.keys.Edit):
		m.state = stateLangDropdown
		m.dropdown = newDropdown(languageOptions(m.languages))
	case key.Matches(msg, m.keys.Delete):
		if m.langCursor < len(m.languages) && len(m.languages) > 1 {
			m.languages = append(m.languages[:m.langCursor], m.languages[m.langCursor+1:]...)
			m.langCursor = min(m.langCursor, len(m.languages))
			return m, m.saveLanguages()
		}
	}
	return m, nil
}

func (m profileModel) handleDropdownKey(msg tea.KeyMsg) (profileModel, tea.Cmd) {
	switch msg.String() {
	case "enter":
		if opt, ok := m.dropdown.Selected(); ok {
			if tag, err := language.Parse(opt.value); err == nil {
				m.languages = append(m.languages, tag)
				m.langCursor = len(m.languages)
			}
			m.state = stateEditLangs
			return m, m.saveLanguages()
		}
		m.state = stateEditLangs
	case "esc":
		m.state = stateEditLangs
	case "up":
		m.dropdown.MoveUp()
	case "down":
		m.dropdown.MoveDown()
	case "backspace", "delete":
		m.dropdown.Backspace()
	default:
		if msg.Type == tea.KeyRunes && len(msg.Runes) == 1 && unicode.IsPrint(msg.Runes[0]) {
			m.dropdown.TypeRune(msg.Runes[0])
		}
	}
	return m, nil
}

func (m profileModel) handleDeleteConfirm(msg tea.KeyMsg) (profileModel, tea.Cmd) {
	switch msg.String() {
	case "y":
		m.state = stateNormal
		if m.replayCursor < len(m.replays) {
			r := m.replays[m.replayCursor]
			if m.replayCursor >= len(m.replays)-1 {
				m.replayCursor = max(0, m.replayCursor-1)
			}
			return m, m.deleteReplay(r.createdAt)
		}
	case "n", "esc":
		m.state = stateNormal
	}
	return m, nil
}

func (m profileModel) handleKey(msg tea.KeyMsg) (profileModel, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Down):
		switch m.section {
		case sectionUsername:
			m.section = sectionLanguages
		case sectionLanguages:
			if len(m.replays) > 0 {
				m.section = sectionReplays
			}
		case sectionReplays:
			m.replayCursor = min(m.replayCursor+1, len(m.replays)-1)
		}
	case key.Matches(msg, m.keys.Up):
		switch m.section {
		case sectionLanguages:
			m.section = sectionUsername
		case sectionReplays:
			if m.replayCursor > 0 {
				m.replayCursor--
			} else {
				m.section = sectionLanguages
			}
		}
	case key.Matches(msg, m.keys.Edit):
		switch m.section {
		case sectionUsername:
			m.state = stateEditName
			m.editor.SetValue(m.username)
		case sectionLanguages:
			m.state = stateEditLangs
			m.langCursor = 0
		}
	case key.Matches(msg, m.keys.Delete):
		if m.section == sectionReplays && len(m.replays) > 0 {
			m.state = stateConfirmDelete
		}
	}
	return m, nil
}

func (m profileModel) handleMouse(msg tea.MouseMsg) (profileModel, tea.Cmd) {
	if m.zone == nil {
		return m, nil
	}
	switch msg.Type {
	case tea.MouseWheelUp:
		if m.state == stateLangDropdown {
			m.dropdown.MoveUp()
		} else if m.replayCursor > 0 {
			m.section = sectionReplays
			m.replayCursor--
		}
		return m, nil
	case tea.MouseWheelDown:
		if m.state == stateLangDropdown {
			m.dropdown.MoveDown()
		} else if m.replayCursor < len(m.replays)-1 {
			m.section = sectionReplays
			m.replayCursor++
		}
		return m, nil
	}
	switch msg.Action {
	case tea.MouseActionMotion:
		m.hovered = m.zoneAt(msg)
	case tea.MouseActionRelease:
		return m.handleClick(m.zoneAt(msg))
	}
	return m, nil
}

func (m profileModel) handleClick(id string) (profileModel, tea.Cmd) {
	switch {
	case id == "pf-username":
		m.section = sectionUsername
		m.state = stateEditName
		m.editor.SetValue(m.username)

	case id == "pf-languages":
		m.section = sectionLanguages
		m.state = stateEditLangs
		m.langCursor = 0

	case id == "pf-lang-add":
		if m.state != stateLangDropdown {
			m.section = sectionLanguages
			m.state = stateLangDropdown
			m.langCursor = len(m.languages)
			m.dropdown = newDropdown(languageOptions(m.languages))
		}

	case strings.HasPrefix(id, "pf-lang-"):
		var i int
		fmt.Sscanf(id, "pf-lang-%d", &i)
		m.state = stateEditLangs
		m.langCursor = i

	case strings.HasPrefix(id, "pf-dd-"):
		var i int
		fmt.Sscanf(id, "pf-dd-%d", &i)
		m.dropdown.cursor = i
		m.dropdown.ensureVisible()
		if opt, ok := m.dropdown.Selected(); ok {
			if tag, err := language.Parse(opt.value); err == nil {
				m.languages = append(m.languages, tag)
				m.langCursor = len(m.languages)
			}
			m.state = stateEditLangs
			return m, m.saveLanguages()
		}

	case strings.HasPrefix(id, "pf-replay-"):
		var i int
		fmt.Sscanf(id, "pf-replay-%d", &i)
		m.section = sectionReplays
		m.replayCursor = i
		m.state = stateNormal
	}
	return m, nil
}

func (m profileModel) zoneAt(msg tea.MouseMsg) string {
	check := func(id string) bool { return m.zone.Get(id).InBounds(msg) }
	if check("pf-username") {
		return "pf-username"
	}
	if check("pf-languages") {
		return "pf-languages"
	}
	if check("pf-lang-add") {
		return "pf-lang-add"
	}
	for i := range m.languages {
		if id := fmt.Sprintf("pf-lang-%d", i); check(id) {
			return id
		}
	}
	for i := range m.replays {
		if id := fmt.Sprintf("pf-replay-%d", i); check(id) {
			return id
		}
	}
	if m.state == stateLangDropdown {
		end := min(m.dropdown.scroll+m.dropdown.maxVisible, len(m.dropdown.filtered))
		for i := m.dropdown.scroll; i < end; i++ {
			if id := fmt.Sprintf("pf-dd-%d", i); check(id) {
				return id
			}
		}
	}
	return ""
}

type profileStyles struct {
	ActiveLabel lipgloss.Style
	Label       lipgloss.Style
	Value       lipgloss.Style
	Hint        lipgloss.Style
	SelectedRow lipgloss.Style
	HoverRow    lipgloss.Style
	Row         lipgloss.Style
	Danger      lipgloss.Style
	Divider     lipgloss.Style
	LangNum     lipgloss.Style
	URL         lipgloss.Style
}

func defaultProfileStyles() profileStyles {
	profileStrong := lipgloss.AdaptiveColor{Light: "235", Dark: "255"}
	profileMuted := lipgloss.AdaptiveColor{Light: "240", Dark: "250"}
	return profileStyles{
		ActiveLabel: lipgloss.NewStyle().Foreground(profileStrong).Bold(true),
		Label:       lipgloss.NewStyle().Foreground(profileMuted),
		Value:       lipgloss.NewStyle().Foreground(profileStrong),
		Hint:        lipgloss.NewStyle().Foreground(profileMuted).Italic(true),
		SelectedRow: lipgloss.NewStyle().Foreground(profileStrong).Bold(true),
		HoverRow:    lipgloss.NewStyle().Foreground(profileStrong),
		Row:         lipgloss.NewStyle().Foreground(profileStrong),
		Danger:      lipgloss.NewStyle().Foreground(theme.Danger).Bold(true),
		Divider:     lipgloss.NewStyle().Foreground(profileMuted),
		LangNum:     lipgloss.NewStyle().Foreground(profileMuted),
		URL:         lipgloss.NewStyle().Foreground(theme.Accent),
	}
}

func (m profileModel) mark(id, s string) string {
	if m.zone != nil {
		return m.zone.Mark(id, s)
	}
	return s
}

func (m profileModel) rowStyle(zoneID string, selected bool) lipgloss.Style {
	if selected {
		return m.styles.SelectedRow
	}
	if m.hovered == zoneID {
		return m.styles.HoverRow
	}
	return m.styles.Row
}

func (m profileModel) styledLabel(text string, w int, active bool) string {
	s := padRight(text, w)
	if active {
		return m.styles.ActiveLabel.Render(s)
	}
	return m.styles.Label.Render(s)
}

func cursor(selected bool) string {
	if selected {
		return "▸ "
	}
	return "  "
}

func (m profileModel) renderProfileTab(width, height int) string {
	if m.userID == "" {
		return m.styles.Label.Render(m.tr.Text(textProfileNotSignedIn))
	}
	if !m.loaded {
		return m.styles.Label.Render(m.tr.Text(textProfileLoading))
	}
	if m.loadErr != nil {
		return m.styles.Label.Render(m.tr.Text(textProfileLoadFailed))
	}

	const labelW = 15
	indent := strings.Repeat(" ", labelW)
	var lines []string

	// Username
	active := m.state == stateEditName || (m.section == sectionUsername && m.state == stateNormal)
	lbl := m.styledLabel(m.tr.Text(textProfileUsername), labelW, active)
	if m.state == stateEditName {
		lines = append(lines, m.mark("pf-username", lbl+m.editor.View(m.styles.Value, m.styles.Hint)))
	} else {
		style := m.styles.Value
		if m.hovered == "pf-username" && !active {
			style = m.styles.HoverRow
		}
		val := style.Render(m.username)
		if active {
			val += m.styles.Hint.Render(m.tr.Text(textProfileEnterToEdit))
		}
		lines = append(lines, m.mark("pf-username", lbl+val))
	}

	// Languages
	langExpanded := m.state == stateEditLangs || m.state == stateLangDropdown
	lbl = m.styledLabel(m.tr.Text(textProfileLanguages), labelW, langExpanded || (m.section == sectionLanguages && m.state == stateNormal))

	if langExpanded {
		for i, tag := range m.languages {
			zid := fmt.Sprintf("pf-lang-%d", i)
			sel := m.langCursor == i && m.state == stateEditLangs
			style := m.rowStyle(zid, sel)
			text := style.Render(cursor(sel) + fmt.Sprintf("%d. %s", i+1, langLabel(tag)))
			if i == 0 {
				text = lbl + text
			} else {
				text = indent + text
			}
			lines = append(lines, m.mark(zid, text))
		}
		if len(m.languages) == 0 {
			lines = append(lines, lbl)
		}

		// Add/search row
		if m.state == stateLangDropdown {
			display := editorCursor.Render(" ") + m.styles.Hint.Render(m.tr.Text(textProfileSearch))
			if len(m.dropdown.searchBuf) > 0 {
				display = m.styles.Value.Render(string(m.dropdown.searchBuf)) + editorCursor.Render(" ")
			}
			lines = append(lines, m.mark("pf-lang-add", indent+m.styles.Row.Render("+ ")+display))
		} else {
			sel := m.langCursor == len(m.languages) && m.state == stateEditLangs
			style := m.rowStyle("pf-lang-add", sel)
			text := cursor(sel) + style.Render(m.tr.Text(textProfileAddLanguage))
			if sel {
				text += m.styles.Hint.Render(m.tr.Text(textProfileEnterToAdd))
			}
			lines = append(lines, m.mark("pf-lang-add", indent+text))
		}

		// Dropdown results
		if m.state == stateLangDropdown {
			if len(m.dropdown.filtered) == 0 {
				lines = append(lines, indent+"    "+m.styles.Hint.Render(m.tr.Text(textProfileNoMatches)))
			} else {
				end := min(m.dropdown.scroll+m.dropdown.maxVisible, len(m.dropdown.filtered))
				for i := m.dropdown.scroll; i < end; i++ {
					zid := fmt.Sprintf("pf-dd-%d", i)
					sel := i == m.dropdown.cursor
					style := m.rowStyle(zid, sel)
					lines = append(lines, m.mark(zid, indent+style.Render(cursor(sel)+"  "+m.dropdown.options[m.dropdown.filtered[i]].label)))
				}
			}
		}
	} else {
		// Collapsed
		names := make([]string, len(m.languages))
		for i, tag := range m.languages {
			names[i] = langName(tag)
		}
		active := m.section == sectionLanguages && m.state == stateNormal
		style := m.styles.Value
		if m.hovered == "pf-languages" && !active {
			style = m.styles.HoverRow
		}
		summary := style.Render(strings.Join(names, ", "))
		if active {
			summary += m.styles.Hint.Render(m.tr.Text(textProfileEnterToEdit))
		}
		lines = append(lines, m.mark("pf-languages", lbl+summary))
	}

	// Replays
	lines = append(lines, "")
	replaysActive := m.state == stateConfirmDelete || (m.section == sectionReplays && m.state == stateNormal)
	title := m.tr.Text(textProfileReplays)
	if len(m.replays) > 0 {
		title = fmt.Sprintf(m.tr.Text(textProfileReplaysCount), len(m.replays))
	}
	title = m.styledLabel(title, 0, replaysActive)
	divW := max(0, width-lipgloss.Width(title)-1)
	lines = append(lines, title+" "+m.styles.Divider.Render(strings.Repeat("─", divW)))

	availRows := max(1, height-len(lines))
	if len(m.replays) == 0 {
		lines = append(lines, m.styles.Label.Render(m.tr.Text(textProfileNoReplays)))
	} else {
		offset := scrollOffset(m.replayCursor, len(m.replays), availRows)
		end := min(offset+availRows, len(m.replays))
		for i := offset; i < end; i++ {
			lines = append(lines, m.mark(fmt.Sprintf("pf-replay-%d", i), m.renderReplayRow(i, width)))
		}
	}

	return lipgloss.NewStyle().Width(width).Height(height).Render(strings.Join(lines, "\n"))
}

func (m profileModel) renderReplayRow(idx, width int) string {
	r := m.replays[idx]
	zid := fmt.Sprintf("pf-replay-%d", idx)
	selected := idx == m.replayCursor && (m.section == sectionReplays || m.state == stateConfirmDelete)

	name := r.gameTitle
	if name == "" {
		name = m.tr.Text(textProfileUnknownGame)
	}
	const nameCol = 16
	if len([]rune(name)) > nameCol-1 {
		name = string([]rune(name)[:nameCol-2]) + "…"
	}
	pad := strings.Repeat(" ", max(0, nameCol-len([]rune(name))))
	ts := time.Unix(r.createdAt, 0).UTC().Format(time.RFC3339)

	nameStyle, timeStyle, urlStyle := m.styles.Row, m.styles.Label, m.styles.URL
	if selected {
		nameStyle, timeStyle, urlStyle = m.styles.SelectedRow, m.styles.Value, m.styles.Value
	} else if m.hovered == zid {
		nameStyle, timeStyle, urlStyle = m.styles.HoverRow.Bold(true), m.styles.HoverRow, m.styles.HoverRow
	}

	row := nameStyle.Render(cursor(selected)+name) + pad + timeStyle.Render(ts)
	if m.state == stateConfirmDelete && selected {
		return row + "  " + m.styles.Danger.Render(m.tr.Text(textProfileDeleteConfirm))
	}
	if r.asciinemaURL != "" {
		row += "  " + urlStyle.Render(osc8Link(r.asciinemaURL, r.asciinemaURL))
	}
	return row
}

func scrollOffset(cursor, total, visible int) int {
	if visible >= total {
		return 0
	}
	return max(0, min(cursor-visible/2, total-visible))
}

func padRight(s string, w int) string {
	if w <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) >= w {
		return string(r[:w])
	}
	return s + strings.Repeat(" ", w-len(r))
}

func (m *model) renderProfileTab(width, height int) string {
	return m.profile.renderProfileTab(width, height)
}
