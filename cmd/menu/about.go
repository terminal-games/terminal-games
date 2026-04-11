// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/terminal-games/terminal-games/cmd/menu/tabs"
	"github.com/terminal-games/terminal-games/cmd/menu/theme"
)

const (
	aboutBlinkInterval   = 700 * time.Millisecond
	aboutRefreshInterval = 5 * time.Second
	aboutMaxMapHeight    = 24
	menuBuiltinVersion   = "builtin"
)

func menuVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return menuBuiltinVersion
	}
	var revision string
	modified := false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	if revision == "" {
		return menuBuiltinVersion
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	if modified {
		return revision + "-dirty"
	}
	return revision
}

type aboutDataMsg struct {
	payload   aboutHostPayload
	err       error
	fetchedAt time.Time
}

type aboutTickMsg time.Time

func aboutTickCmd() tea.Cmd {
	return tea.Tick(aboutBlinkInterval, func(t time.Time) tea.Msg {
		return aboutTickMsg(t)
	})
}

type aboutModel struct {
	localizer localizer
	data      aboutHostPayload
	lastFetch time.Time
	loadErr   error
	loaded    bool
	loading   bool
	active    bool
	blinkOn   bool
}

func newAboutModel() aboutModel {
	return aboutModel{
		localizer: newLocalizer(),
		blinkOn:   true,
	}
}

func (m *aboutModel) applyLocalization(localizer localizer) {
	m.localizer = localizer
}

func (m aboutModel) Init() tea.Cmd { return nil }

func (m aboutModel) fetchStatus() tea.Cmd {
	return func() tea.Msg {
		payload, err := menuFetchAboutStatus()
		return aboutDataMsg{
			payload:   payload,
			err:       err,
			fetchedAt: time.Now(),
		}
	}
}

func (m aboutModel) beginRefresh() (aboutModel, tea.Cmd) {
	if m.loading {
		return m, nil
	}
	m.loading = true
	return m, m.fetchStatus()
}

func (m aboutModel) Update(msg tea.Msg) (aboutModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tabs.TabChangedMsg:
		if msg.Tab.ID == "about" {
			m.active = true
			cmds := []tea.Cmd{aboutTickCmd()}
			if !m.loaded || time.Since(m.lastFetch) >= aboutRefreshInterval {
				var cmd tea.Cmd
				m, cmd = m.beginRefresh()
				if cmd != nil {
					cmds = append(cmds, cmd)
				}
			}
			return m, tea.Batch(cmds...)
		}
		m.active = false
		return m, nil

	case aboutTickMsg:
		if !m.active {
			return m, nil
		}
		m.blinkOn = !m.blinkOn
		cmds := []tea.Cmd{aboutTickCmd()}
		if !m.loading && (!m.loaded || time.Since(m.lastFetch) >= aboutRefreshInterval) {
			var cmd tea.Cmd
			m, cmd = m.beginRefresh()
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
		}
		return m, tea.Batch(cmds...)

	case aboutDataMsg:
		m.loading = false
		m.loaded = true
		m.lastFetch = msg.fetchedAt
		m.loadErr = msg.err
		if msg.err == nil {
			m.data = msg.payload
		}
		return m, nil

	case localizationChangedMsg:
		if m.localizer.SetPreferred(msg.preferred) {
			m.applyLocalization(m.localizer)
		}
		return m, nil
	}

	return m, nil
}

type regionMeta struct {
	Code string
	Name string
	Lon  float64
	Lat  float64
}

type regionSummary struct {
	regionMeta
	LatencyMS     int
	Active        int
	SessionsKnown bool
	IsCurrent     bool
	Nodes         []string
}

func normalizeRegionCode(nodeID string) string {
	s := strings.ToLower(strings.TrimSpace(nodeID))
	if len(s) >= 3 {
		return s[:3]
	}
	if s == "" {
		return "loc"
	}
	return s
}

func fallbackRegionForNode(nodeID string) regionMeta {
	code := normalizeRegionCode(nodeID)
	if meta, ok := regionCatalog()[code]; ok {
		return meta
	}
	return regionMeta{
		Code: strings.ToUpper(code),
		Name: strings.ToUpper(code),
		Lon:  0,
		Lat:  0,
	}
}

func regionForAboutNode(node aboutNode) regionMeta {
	fallback := fallbackRegionForNode(node.NodeID)
	meta := fallback
	if node.RegionCode != nil && strings.TrimSpace(*node.RegionCode) != "" {
		meta.Code = strings.ToUpper(strings.TrimSpace(*node.RegionCode))
	}
	if node.RegionName != nil && strings.TrimSpace(*node.RegionName) != "" {
		meta.Name = strings.TrimSpace(*node.RegionName)
	} else if meta.Name == "" {
		meta.Name = meta.Code
	}
	if node.LongitudeDeg != nil {
		meta.Lon = *node.LongitudeDeg
	}
	if node.LatitudeDeg != nil {
		meta.Lat = *node.LatitudeDeg
	}
	return meta
}

func currentRegionMeta(payload aboutHostPayload, regions []regionSummary) regionMeta {
	for _, region := range regions {
		if region.IsCurrent {
			return region.regionMeta
		}
	}
	for _, node := range payload.Nodes {
		if node.IsCurrent {
			return regionForAboutNode(node)
		}
	}
	return fallbackRegionForNode(payload.CurrentRegion)
}

func regionCatalog() map[string]regionMeta {
	return map[string]regionMeta{
		"loc": {Code: "LOC", Name: "Local", Lon: 0, Lat: 0},
		"iad": {Code: "IAD", Name: "Ashburn", Lon: -77.46, Lat: 39.01},
		"ewr": {Code: "EWR", Name: "Newark", Lon: -74.17, Lat: 40.69},
		"nyc": {Code: "NYC", Name: "New York", Lon: -74.00, Lat: 40.71},
		"mia": {Code: "MIA", Name: "Miami", Lon: -80.29, Lat: 25.79},
		"ord": {Code: "ORD", Name: "Chicago", Lon: -87.90, Lat: 41.98},
		"dfw": {Code: "DFW", Name: "Dallas", Lon: -97.04, Lat: 32.90},
		"den": {Code: "DEN", Name: "Denver", Lon: -104.67, Lat: 39.86},
		"sea": {Code: "SEA", Name: "Seattle", Lon: -122.30, Lat: 47.45},
		"sfo": {Code: "SFO", Name: "San Francisco", Lon: -122.38, Lat: 37.62},
		"lax": {Code: "LAX", Name: "Los Angeles", Lon: -118.40, Lat: 33.94},
		"gru": {Code: "GRU", Name: "Sao Paulo", Lon: -46.47, Lat: -23.43},
		"eze": {Code: "EZE", Name: "Buenos Aires", Lon: -58.54, Lat: -34.82},
		"lhr": {Code: "LHR", Name: "London", Lon: -0.45, Lat: 51.47},
		"ams": {Code: "AMS", Name: "Amsterdam", Lon: 4.76, Lat: 52.31},
		"fra": {Code: "FRA", Name: "Frankfurt", Lon: 8.57, Lat: 50.03},
		"par": {Code: "PAR", Name: "Paris", Lon: 2.35, Lat: 48.85},
		"mad": {Code: "MAD", Name: "Madrid", Lon: -3.57, Lat: 40.49},
		"waw": {Code: "WAW", Name: "Warsaw", Lon: 20.97, Lat: 52.16},
		"ist": {Code: "IST", Name: "Istanbul", Lon: 28.75, Lat: 41.27},
		"jnb": {Code: "JNB", Name: "Johannesburg", Lon: 28.24, Lat: -26.14},
		"dxb": {Code: "DXB", Name: "Dubai", Lon: 55.36, Lat: 25.25},
		"bom": {Code: "BOM", Name: "Mumbai", Lon: 72.87, Lat: 19.09},
		"sin": {Code: "SIN", Name: "Singapore", Lon: 103.99, Lat: 1.35},
		"hkg": {Code: "HKG", Name: "Hong Kong", Lon: 113.92, Lat: 22.31},
		"nrt": {Code: "NRT", Name: "Tokyo", Lon: 140.39, Lat: 35.77},
		"icn": {Code: "ICN", Name: "Seoul", Lon: 126.44, Lat: 37.46},
		"syd": {Code: "SYD", Name: "Sydney", Lon: 151.18, Lat: -33.94},
		"mel": {Code: "MEL", Name: "Melbourne", Lon: 144.84, Lat: -37.67},
		"akl": {Code: "AKL", Name: "Auckland", Lon: 174.79, Lat: -37.01},
	}
}

func summarizeRegions(payload aboutHostPayload) []regionSummary {
	grouped := map[string]*regionSummary{}
	for _, node := range payload.Nodes {
		meta := regionForAboutNode(node)
		key := meta.Code
		entry, ok := grouped[key]
		if !ok {
			entry = &regionSummary{
				regionMeta: meta,
				LatencyMS:  -1,
			}
			grouped[key] = entry
		}
		entry.IsCurrent = entry.IsCurrent || node.IsCurrent
		entry.Nodes = append(entry.Nodes, node.NodeID)
		if node.SessionsKnown {
			entry.SessionsKnown = true
			entry.Active += node.ActiveSessions
		}
		if node.LatencyMS >= 0 && (entry.LatencyMS < 0 || node.LatencyMS < entry.LatencyMS) {
			entry.LatencyMS = node.LatencyMS
		}
	}

	out := make([]regionSummary, 0, len(grouped))
	for _, summary := range grouped {
		sort.Strings(summary.Nodes)
		out = append(out, *summary)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsCurrent != out[j].IsCurrent {
			return out[i].IsCurrent
		}
		if out[i].LatencyMS >= 0 && out[j].LatencyMS >= 0 && out[i].LatencyMS != out[j].LatencyMS {
			return out[i].LatencyMS < out[j].LatencyMS
		}
		return out[i].Code < out[j].Code
	})
	return out
}

func wrapStyledLines(style lipgloss.Style, text string, width int) []string {
	if width <= 0 {
		return []string{""}
	}
	lines := strings.Split(style.Width(width).Render(text), "\n")
	if len(lines) == 0 {
		return []string{""}
	}
	return lines
}

func renderStatCardContentLines(label, value string, width int) []string {
	contentWidth := max(1, width)
	labelStyle := lipgloss.NewStyle().Foreground(theme.TextSubtle).Bold(true)
	valueStyle := lipgloss.NewStyle().Foreground(theme.Text).Bold(true)
	lines := append([]string{}, wrapStyledLines(labelStyle, label, contentWidth)...)
	lines = append(lines, wrapStyledLines(valueStyle, value, contentWidth)...)
	return lines
}

func statCardInnerWidth(width int) int {
	return max(1, width-4)
}

func renderStatCard(label, value string, width, contentHeight int) string {
	contentWidth := statCardInnerWidth(width)
	borderStyle := lipgloss.NewStyle().Foreground(theme.Line)
	lines := renderStatCardContentLines(label, value, contentWidth)
	for len(lines) < max(1, contentHeight) {
		lines = append(lines, "")
	}
	out := []string{
		borderStyle.Render("╭" + strings.Repeat("─", contentWidth+2) + "╮"),
	}
	for _, line := range lines {
		out = append(out, borderStyle.Render("│")+" "+padStyled(line, contentWidth)+" "+borderStyle.Render("│"))
	}
	out = append(out, borderStyle.Render("╰"+strings.Repeat("─", contentWidth+2)+"╯"))
	return strings.Join(out, "\n")
}

type statCardData struct {
	label string
	value string
}

func distributeWidths(totalWidth, cols, gap int) []int {
	available := max(cols, totalWidth-(cols-1)*gap)
	baseWidth := available / cols
	remainder := available % cols
	widths := make([]int, cols)
	for i := range cols {
		widths[i] = baseWidth
		if i < remainder {
			widths[i]++
		}
	}
	return widths
}

func cardGridLayout(width, cardCount int) (int, []int) {
	gap := 1
	const minFourUpCardWidth = 24
	const minTwoUpCardWidth = 28
	if cardCount >= 4 && width >= 4*minFourUpCardWidth+3*gap {
		return 4, distributeWidths(width, 4, gap)
	}
	if cardCount >= 2 && width >= 2*minTwoUpCardWidth+gap {
		return 2, distributeWidths(width, 2, gap)
	}
	if cardCount <= 0 {
		return 1, []int{width}
	}
	return 1, []int{width}
}

func renderCardGrid(cards []statCardData, width int) string {
	if len(cards) == 0 {
		return ""
	}
	cols, _ := cardGridLayout(width, len(cards))
	rows := make([]string, 0, (len(cards)+cols-1)/cols)
	maxContentHeight := 1
	rowWidthsByStart := map[int][]int{}
	for i := 0; i < len(cards); i += cols {
		rowCount := min(cols, len(cards)-i)
		rowWidths := distributeWidths(width, rowCount, 1)
		rowWidthsByStart[i] = rowWidths
		for j := 0; j < rowCount; j++ {
			spec := cards[i+j]
			contentHeight := len(renderStatCardContentLines(spec.label, spec.value, statCardInnerWidth(rowWidths[j])))
			maxContentHeight = max(maxContentHeight, contentHeight)
		}
	}
	for i := 0; i < len(cards); i += cols {
		rowCount := min(cols, len(cards)-i)
		rowWidths := rowWidthsByStart[i]
		rowCards := make([]string, 0, rowCount)
		rowHeight := 0
		for j := 0; j < rowCount; j++ {
			spec := cards[i+j]
			card := renderStatCard(spec.label, spec.value, rowWidths[j], maxContentHeight)
			rowCards = append(rowCards, card)
			rowHeight = max(rowHeight, lipgloss.Height(card))
		}
		if len(rowCards) == 1 {
			rows = append(rows, rowCards[0])
			continue
		}
		rowParts := make([]string, 0, len(rowCards)*2-1)
		for j, card := range rowCards {
			if j > 0 {
				rowParts = append(rowParts, lipgloss.NewStyle().Width(1).Height(rowHeight).Render(" "))
			}
			rowParts = append(rowParts, card)
		}
		rows = append(rows, lipgloss.JoinHorizontal(lipgloss.Top, rowParts...))
	}
	return strings.Join(rows, "\n")
}

func renderRegionTable(width int, regions []regionSummary) string {
	headerStyle := lipgloss.NewStyle().Foreground(theme.TextSubtle).Bold(true)
	valueStyle := lipgloss.NewStyle().Foreground(theme.Text)
	mutedStyle := lipgloss.NewStyle().Foreground(theme.TextMuted)
	currentStyle := lipgloss.NewStyle().Foreground(theme.Primary).Bold(true)
	innerWidth := max(1, width-4)

	latW := 8
	sessW := 8
	const columnGap = 3
	const minRegionW = 12
	const minNodesW = 5
	maxRegionW := lipgloss.Width("Region")
	maxNodesW := lipgloss.Width("Nodes")
	for _, region := range regions {
		name := region.Code + "  " + region.Name
		maxRegionW = max(maxRegionW, lipgloss.Width(name))
		maxNodesW = max(maxNodesW, lipgloss.Width(strings.Join(region.Nodes, ", ")))
	}
	fixedWidth := latW + sessW + columnGap
	availableNodesW := max(minNodesW, innerWidth-fixedWidth-minRegionW)
	nodesW := min(maxNodesW, availableNodesW)
	regionW := max(minRegionW, innerWidth-fixedWidth-nodesW)
	if regionW > maxRegionW {
		regionW = maxRegionW
		nodesW = max(minNodesW, innerWidth-fixedWidth-regionW)
	}
	lines := []string{
		headerStyle.Render(padRight("Region", regionW) + " " + padRight("Latency", latW) + " " + padRight("Sessions", sessW) + " " + padRight("Nodes", nodesW)),
	}
	for _, region := range regions {
		latency := "--"
		if region.LatencyMS >= 0 {
			latency = fmt.Sprintf("%d ms", region.LatencyMS)
		}
		sessions := "--"
		if region.SessionsKnown {
			sessions = fmt.Sprintf("%d", region.Active)
		}
		nodes := strings.Join(region.Nodes, ", ")
		if lipgloss.Width(nodes) > nodesW {
			nodes = truncateVisible(nodes, nodesW)
		}
		name := region.Code + "  " + region.Name
		if lipgloss.Width(name) > regionW {
			name = truncateVisible(name, regionW)
		}
		line := padRight(name, regionW) + " " + padRight(latency, latW) + " " + padRight(sessions, sessW) + " " + padRight(nodes, nodesW)
		switch {
		case region.IsCurrent:
			lines = append(lines, currentStyle.Render(line))
		case region.SessionsKnown:
			lines = append(lines, valueStyle.Render(line))
		default:
			lines = append(lines, mutedStyle.Render(line))
		}
	}
	return renderFramedPanel(width, lines)
}

func renderFooterPanel(width int, localizer localizer) string {
	labelStyle := lipgloss.NewStyle().Foreground(theme.TextSubtle).Bold(true)
	bodyStyle := lipgloss.NewStyle().Foreground(theme.Text)
	linkStyle := lipgloss.NewStyle().Foreground(theme.Accent).Bold(true)

	lines := []string{
		labelStyle.Render(localizer.Text(textAboutSectionCredits)),
		bodyStyle.Render(localizer.Text(textAboutDevelopedByPrefix) + linkStyle.Render(osc8Link("https://github.com/mbund", "Mark Bundschuh")) + localizer.Text(textAboutSentenceEnd)),
		bodyStyle.Render(localizer.Text(textAboutOpenSourcePrefix) + linkStyle.Render(osc8Link("https://github.com/terminal-games/terminal-games", "https://github.com/terminal-games/terminal-games")) + localizer.Text(textAboutSentenceEnd)),
	}
	return renderFramedPanel(width, lines)
}

func renderMapPanel(width int, title, summary, caption, body string) string {
	innerWidth := max(1, width-4)
	borderStyle := lipgloss.NewStyle().Foreground(theme.Line)
	titleStyle := lipgloss.NewStyle().Foreground(theme.TextSubtle).Bold(true)
	summaryStyle := lipgloss.NewStyle().Foreground(theme.TextMuted)
	captionStyle := lipgloss.NewStyle().Foreground(theme.TextSubtle)
	leftBorder := borderStyle.Render("│")
	rightBorder := borderStyle.Render("│")

	var lines []string
	lines = append(lines, borderStyle.Render("╭"+strings.Repeat("─", innerWidth+2)+"╮"))
	lines = append(lines, leftBorder+" "+padStyled(titleStyle.Render(title), innerWidth)+" "+rightBorder)
	lines = append(lines, leftBorder+" "+padStyled(summaryStyle.Render(summary), innerWidth)+" "+rightBorder)
	if strings.TrimSpace(caption) != "" {
		lines = append(lines, leftBorder+" "+padStyled(captionStyle.Render(caption), innerWidth)+" "+rightBorder)
	}
	for _, line := range strings.Split(body, "\n") {
		lines = append(lines, leftBorder+" "+padStyled(line, innerWidth)+" "+rightBorder)
	}
	lines = append(lines, borderStyle.Render("╰"+strings.Repeat("─", innerWidth+2)+"╯"))
	return strings.Join(lines, "\n")
}

func renderFramedPanel(width int, lines []string) string {
	innerWidth := max(1, width-4)
	borderStyle := lipgloss.NewStyle().Foreground(theme.Line)
	leftBorder := borderStyle.Render("│")
	rightBorder := borderStyle.Render("│")
	out := []string{
		borderStyle.Render("╭" + strings.Repeat("─", innerWidth+2) + "╮"),
	}
	for _, line := range lines {
		out = append(out, leftBorder+" "+padStyled(line, innerWidth)+" "+rightBorder)
	}
	out = append(out, borderStyle.Render("╰"+strings.Repeat("─", innerWidth+2)+"╯"))
	return strings.Join(out, "\n")
}

func fitHeight(text string, height int) string {
	if height <= 0 {
		return ""
	}
	lines := strings.Split(text, "\n")
	if len(lines) > height {
		lines = lines[:height]
	}
	if len(lines) < height {
		lines = append(lines, make([]string, height-len(lines))...)
	}
	return strings.Join(lines, "\n")
}

func truncateVisible(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= width {
		return s
	}
	runes := []rune(s)
	for len(runes) > 0 && lipgloss.Width(string(runes)+"…") > width {
		runes = runes[:len(runes)-1]
	}
	return string(runes) + "…"
}

func padStyled(s string, width int) string {
	visible := lipgloss.Width(s)
	if visible > width {
		return truncateVisible(s, width)
	}
	if visible < width {
		return s + strings.Repeat(" ", width-visible)
	}
	return s
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func clampInt(v, low, high int) int {
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}

func (m aboutModel) renderAboutTab(width, height int) string {
	titleStyle := lipgloss.NewStyle().Foreground(theme.Text).Bold(true)
	ledeStyle := lipgloss.NewStyle().Foreground(theme.TextMuted).Width(width)
	topLinkStyle := lipgloss.NewStyle().Foreground(theme.Accent).Bold(true)
	errorStyle := lipgloss.NewStyle().Foreground(theme.Danger).Bold(true)
	panelStyle := lipgloss.NewStyle().Width(width)

	head := []string{
		titleStyle.Render(m.localizer.Text(textAboutTitle)),
		ledeStyle.Render(m.localizer.Text(textAboutBody)),
		ledeStyle.Render(m.localizer.Text(textAboutDeveloperDocsPrefix) + topLinkStyle.Render(osc8Link("https://docs.terminalgames.net", "https://docs.terminalgames.net")) + m.localizer.Text(textAboutSentenceEnd)),
	}

	if !m.loaded && m.loading {
		content := strings.Join(append(head, "", m.localizer.Text(textProfileLoading)), "\n")
		return panelStyle.Height(height).Render(fitHeight(content, height))
	}

	if m.loadErr != nil && !m.loaded {
		content := strings.Join(append(head, "", errorStyle.Render(m.loadErr.Error())), "\n")
		return panelStyle.Height(height).Render(fitHeight(content, height))
	}

	regions := summarizeRegions(m.data)
	current := currentRegionMeta(m.data, regions)
	knownSessions := 0
	for _, region := range regions {
		if region.SessionsKnown {
			knownSessions += region.Active
		}
	}
	serverVersion := "local only"
	if m.data.ServerVersion != nil && strings.TrimSpace(*m.data.ServerVersion) != "" {
		serverVersion = *m.data.ServerVersion
	}
	cards := []statCardData{
		{label: "Server Version", value: serverVersion},
		{label: "Menu Version", value: menuVersion()},
		{label: "CLI API Version", value: m.data.CliAPIVersion},
		{label: "Connected Region", value: current.Code + "  " + current.Name},
	}
	cardsPanel := renderCardGrid(cards, width)

	sessionLabel := "sessions"
	if knownSessions == 1 {
		sessionLabel = "session"
	}
	mapSummary := fmt.Sprintf("%d regions, %d current %s", len(regions), knownSessions, sessionLabel)
	mapCaption := ""

	regionsPanel := renderRegionTable(width, regions)
	footerPanel := renderFooterPanel(width, m.localizer)

	headBlock := strings.Join(head, "\n")
	mapChromeHeight := 4
	nonMapHeight := lipgloss.Height(headBlock) +
		lipgloss.Height(cardsPanel) +
		lipgloss.Height(regionsPanel) +
		lipgloss.Height(footerPanel) +
		1 // blank row between the body copy and the panel stack
	mapHeight := min(aboutMaxMapHeight, max(6, height-nonMapHeight-mapChromeHeight))
	mapWidth := max(20, width-4)

	mapPanel := renderMapPanel(
		width,
		"Network Topology",
		mapSummary,
		mapCaption,
		renderWorldMap(mapWidth, mapHeight, regions, m.blinkOn),
	)

	content := strings.Join([]string{
		headBlock,
		"",
		cardsPanel,
		mapPanel,
		regionsPanel,
		footerPanel,
	}, "\n")
	return panelStyle.Height(height).Render(fitHeight(content, height))
}

func (m *model) renderAboutTab(width, height int) string {
	return m.about.renderAboutTab(width, height)
}
