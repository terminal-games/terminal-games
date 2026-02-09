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
	"golang.org/x/text/language"
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

var langNames = map[string]string{
	"ar": "Arabic", "bg": "Bulgarian", "bn": "Bengali", "ca": "Catalan",
	"cs": "Czech", "da": "Danish", "de": "German", "el": "Greek",
	"en": "English", "es": "Spanish", "et": "Estonian", "fa": "Persian",
	"fi": "Finnish", "fr": "French", "he": "Hebrew", "hi": "Hindi",
	"hr": "Croatian", "hu": "Hungarian", "id": "Indonesian", "is": "Icelandic",
	"it": "Italian", "ja": "Japanese", "ko": "Korean", "lt": "Lithuanian",
	"lv": "Latvian", "ms": "Malay", "nl": "Dutch", "no": "Norwegian",
	"pl": "Polish", "pt": "Portuguese", "ro": "Romanian", "ru": "Russian",
	"sk": "Slovak", "sl": "Slovenian", "sr": "Serbian", "sv": "Swedish",
	"sw": "Swahili", "ta": "Tamil", "te": "Telugu", "th": "Thai",
	"tr": "Turkish", "uk": "Ukrainian", "ur": "Urdu", "vi": "Vietnamese",
	"zh": "Chinese",
}

var availableLanguages []language.Tag

func init() {
	for code := range langNames {
		availableLanguages = append(availableLanguages, language.Make(code))
	}
	sort.Slice(availableLanguages, func(i, j int) bool {
		return langName(availableLanguages[i]) < langName(availableLanguages[j])
	})
}

func langName(tag language.Tag) string {
	if name, ok := langNames[tag.String()]; ok {
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

func (e inlineEditor) View() string {
	if len(e.runes) == 0 {
		return editorCursor.Render(" ") + pfHint.Render(e.placeholder)
	}
	var b strings.Builder
	b.WriteString(pfValue.Render(string(e.runes[:e.pos])))
	if e.pos < len(e.runes) {
		b.WriteString(editorCursor.Render(string(e.runes[e.pos : e.pos+1])))
		b.WriteString(pfValue.Render(string(e.runes[e.pos+1:])))
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
	userID string

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
	return profileKeyMap{
		Up:       key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("↑/k", "up")),
		Down:     key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("↓/j", "down")),
		Edit:     key.NewBinding(key.WithKeys("enter", "e"), key.WithHelp("enter", "edit")),
		Delete:   key.NewBinding(key.WithKeys("d", "x"), key.WithHelp("d", "delete")),
		MoveUp:   key.NewBinding(key.WithKeys("K"), key.WithHelp("shift+k", "move up")),
		MoveDown: key.NewBinding(key.WithKeys("J"), key.WithHelp("shift+j", "move down")),
	}
}

func newProfileModel(zoneManager *zone.Manager, db *sql.DB) profileModel {
	return profileModel{
		db:     db,
		zone:   zoneManager,
		keys:   newProfileKeyMap(),
		userID: os.Getenv("USER_ID"),
		editor: inlineEditor{placeholder: "enter username", maxLen: 32},
	}
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
			"SELECT r.created_at, r.asciinema_url, g.title FROM replays r LEFT JOIN games g ON r.game_id = g.id WHERE r.user_id = ? ORDER BY r.created_at DESC",
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
			key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "save")),
			key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "cancel")),
		}
	case stateEditLangs:
		b := []key.Binding{m.keys.Up, m.keys.Down}
		if m.langCursor < len(m.languages) {
			b = append(b, m.keys.MoveUp, m.keys.MoveDown, m.keys.Delete)
		}
		return append(b, m.keys.Edit, key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "done")))
	case stateLangDropdown:
		return []key.Binding{
			key.NewBinding(key.WithKeys("up", "down"), key.WithHelp("↑↓", "navigate")),
			key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "select")),
			key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "back")),
		}
	case stateConfirmDelete:
		return []key.Binding{
			key.NewBinding(key.WithKeys("y"), key.WithHelp("y", "confirm")),
			key.NewBinding(key.WithKeys("n"), key.WithHelp("n/esc", "cancel")),
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
	case "up", "k":
		m.dropdown.MoveUp()
	case "down", "j":
		m.dropdown.MoveDown()
	case "backspace":
		m.dropdown.Backspace()
	default:
		if r := []rune(msg.String()); len(r) == 1 && unicode.IsPrint(r[0]) {
			m.dropdown.TypeRune(r[0])
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

var (
	pfActiveLabel = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffffff")).Bold(true)
	pfLabel       = lipgloss.NewStyle().Foreground(lipgloss.Color("#666666"))
	pfValue       = lipgloss.NewStyle().Foreground(lipgloss.Color("#cccccc"))
	pfHint        = lipgloss.NewStyle().Foreground(lipgloss.Color("#555555")).Italic(true)
	pfSelectedRow = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffffff")).Bold(true)
	pfHoverRow    = lipgloss.NewStyle().Foreground(lipgloss.Color("#bbbbbb"))
	pfRow         = lipgloss.NewStyle().Foreground(lipgloss.Color("#999999"))
	pfDanger      = lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555")).Bold(true)
	pfDivider     = lipgloss.NewStyle().Foreground(lipgloss.Color("#444444"))
	pfLangNum     = lipgloss.NewStyle().Foreground(lipgloss.Color("#777777"))
	pfURL         = lipgloss.NewStyle().Foreground(lipgloss.Color("#6688aa"))
)

func (m profileModel) mark(id, s string) string {
	if m.zone != nil {
		return m.zone.Mark(id, s)
	}
	return s
}

func (m profileModel) rowStyle(zoneID string, selected bool) lipgloss.Style {
	if selected {
		return pfSelectedRow
	}
	if m.hovered == zoneID {
		return pfHoverRow
	}
	return pfRow
}

func styledLabel(text string, w int, active bool) string {
	s := padRight(text, w)
	if active {
		return pfActiveLabel.Render(s)
	}
	return pfLabel.Render(s)
}

func cursor(selected bool) string {
	if selected {
		return "▸ "
	}
	return "  "
}

func (m profileModel) renderProfileTab(width, height int) string {
	if m.userID == "" {
		return pfLabel.Render("You are not signed in.")
	}
	if !m.loaded {
		return pfLabel.Render("Loading...")
	}
	if m.loadErr != nil {
		return pfLabel.Render("Failed to load profile.")
	}

	const labelW = 12
	indent := strings.Repeat(" ", labelW)
	var lines []string

	// Username
	active := m.state == stateEditName || (m.section == sectionUsername && m.state == stateNormal)
	lbl := styledLabel("Username", labelW, active)
	if m.state == stateEditName {
		lines = append(lines, m.mark("pf-username", lbl+m.editor.View()))
	} else {
		style := pfValue
		if m.hovered == "pf-username" && !active {
			style = pfHoverRow
		}
		val := style.Render(m.username)
		if active {
			val += pfHint.Render("  enter to edit")
		}
		lines = append(lines, m.mark("pf-username", lbl+val))
	}

	// Languages
	langExpanded := m.state == stateEditLangs || m.state == stateLangDropdown
	lbl = styledLabel("Languages", labelW, langExpanded || (m.section == sectionLanguages && m.state == stateNormal))

	if langExpanded {
		for i, tag := range m.languages {
			zid := fmt.Sprintf("pf-lang-%d", i)
			sel := m.langCursor == i && m.state == stateEditLangs
			style := m.rowStyle(zid, sel)
			text := cursor(sel) + pfLangNum.Render(fmt.Sprintf("%d. ", i+1)) + style.Render(langLabel(tag))
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
			display := editorCursor.Render(" ") + pfHint.Render("search...")
			if len(m.dropdown.searchBuf) > 0 {
				display = pfValue.Render(string(m.dropdown.searchBuf)) + editorCursor.Render(" ")
			}
			lines = append(lines, m.mark("pf-lang-add", indent+pfRow.Render("+ ")+display))
		} else {
			sel := m.langCursor == len(m.languages) && m.state == stateEditLangs
			style := m.rowStyle("pf-lang-add", sel)
			text := cursor(sel) + style.Render("+ add language")
			if sel {
				text += pfHint.Render("  enter to add")
			}
			lines = append(lines, m.mark("pf-lang-add", indent+text))
		}

		// Dropdown results
		if m.state == stateLangDropdown {
			if len(m.dropdown.filtered) == 0 {
				lines = append(lines, indent+"    "+pfHint.Render("no matches"))
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
		style := pfValue
		if m.hovered == "pf-languages" && !active {
			style = pfHoverRow
		}
		summary := style.Render(strings.Join(names, ", "))
		if active {
			summary += pfHint.Render("  enter to edit")
		}
		lines = append(lines, m.mark("pf-languages", lbl+summary))
	}

	// Replays
	lines = append(lines, "")
	replaysActive := m.state == stateConfirmDelete || (m.section == sectionReplays && m.state == stateNormal)
	title := "Replays"
	if len(m.replays) > 0 {
		title = fmt.Sprintf("Replays (%d)", len(m.replays))
	}
	title = styledLabel(title, 0, replaysActive)
	divW := max(0, width-lipgloss.Width(title)-1)
	lines = append(lines, title+" "+pfDivider.Render(strings.Repeat("─", divW)))

	availRows := max(1, height-len(lines))
	if len(m.replays) == 0 {
		lines = append(lines, pfLabel.Render("  No replays yet."))
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
		name = "unknown"
	}
	const nameCol = 16
	if len([]rune(name)) > nameCol-1 {
		name = string([]rune(name)[:nameCol-2]) + "…"
	}
	pad := strings.Repeat(" ", max(0, nameCol-len([]rune(name))))
	ts := time.Unix(r.createdAt, 0).UTC().Format("Jan 2, 2006  3:04 PM")

	nameStyle, timeStyle, urlStyle := pfRow, pfLabel, pfURL
	if selected {
		nameStyle, timeStyle, urlStyle = pfSelectedRow, pfValue, pfValue
	} else if m.hovered == zid {
		nameStyle, timeStyle, urlStyle = pfHoverRow, pfHoverRow, pfHoverRow
	}

	row := nameStyle.Render(cursor(selected)+osc8Link(r.asciinemaURL, name)) + pad + timeStyle.Render(ts)
	if m.state == stateConfirmDelete && selected {
		return row + "  " + pfDanger.Render("delete? y/n")
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
