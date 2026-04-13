// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package translations

import (
	"fmt"

	"github.com/terminal-games/terminal-games/cmd/menu/carousel"
	"github.com/terminal-games/terminal-games/cmd/menu/gamelist"
)

type English struct{}

func (English) HeaderTitle() string    { return "Terminal Games" }
func (English) TabGames() string       { return "Games" }
func (English) TabProfile() string     { return "Profile" }
func (English) TabAbout() string       { return "About" }
func (English) WindowTooSmall() string { return "Window must be larger" }
func (English) UnknownTab() string     { return "Unknown tab" }

func (English) HelpQuit() string        { return "quit" }
func (English) HelpNextTab() string     { return "next tab" }
func (English) HelpPrevTab() string     { return "prev tab" }
func (English) HelpUp() string          { return "up" }
func (English) HelpDown() string        { return "down" }
func (English) HelpEdit() string        { return "edit" }
func (English) HelpDelete() string      { return "delete" }
func (English) HelpMoveUp() string      { return "move up" }
func (English) HelpMoveDown() string    { return "move down" }
func (English) HelpSave() string        { return "save" }
func (English) HelpCancel() string      { return "cancel" }
func (English) HelpDone() string        { return "done" }
func (English) HelpNavigate() string    { return "navigate" }
func (English) HelpSelect() string      { return "select" }
func (English) HelpBack() string        { return "back" }
func (English) HelpConfirm() string     { return "confirm" }
func (English) HelpFilter() string      { return "filter" }
func (English) HelpClearFilter() string { return "clear filter" }
func (English) HelpApplyFilter() string { return "apply filter" }
func (English) HelpPlay() string        { return "play" }
func (English) HelpPageUp() string      { return "page up" }
func (English) HelpPageDown() string    { return "page down" }
func (English) HelpDetailsUp() string   { return "details up" }
func (English) HelpDetailsDown() string { return "details down" }

func (English) GamesListTitle() string { return "All Games" }
func (English) GamesNoMatch() string   { return "No games match the current filter." }
func (English) GamesByAuthor(author string) string {
	return "by " + author
}

func (English) GamesActiveSessions(count int, sessionsKnown bool) string {
	prefix := ""
	if !sessionsKnown && count > 0 {
		prefix = "~"
	}
	if count == 1 {
		return prefix + "1 active session"
	}
	return fmt.Sprintf("%s%d active sessions", prefix, count)
}

func (t English) GameListLabels() gamelist.Labels {
	return gamelist.Labels{
		Up:          t.HelpUp(),
		Down:        t.HelpDown(),
		Filter:      t.HelpFilter(),
		ClearFilter: t.HelpClearFilter(),
		ApplyFilter: t.HelpApplyFilter(),
		Cancel:      t.HelpCancel(),
		Games:       t.TabGames(),
		Of:          "of",
		FilterValue: t.HelpFilter(),
	}
}

func (English) CarouselLabels() carousel.Labels {
	return carousel.Labels{
		Screenshots: "Screenshots",
		EscToClose:  "ESC to close",
	}
}

func (English) PlayButton() string  { return "Play" }
func (English) PlayLoading() string { return "Loading" }

func (English) AboutTitle() string {
	return "Play games from anywhere, in the terminal."
}

func (English) AboutBody() string {
	return "Terminal Games is an open source platform built for games designed to work on modern terminals and over SSH."
}

func (English) AboutDeveloperDocs(link string) string {
	return "Become a game developer at " + link
}

func (English) AboutCreditsHeading() string { return "Credits" }

func (English) AboutDevelopedBy(name string) string {
	return "Developed by " + name
}

func (English) AboutOpenSource(link string) string {
	return "Open source at " + link
}

func (English) AboutServerVersionLabel() string   { return "Server Version" }
func (English) AboutMenuVersionLabel() string     { return "Menu Version" }
func (English) AboutCLIAPIVersionLabel() string   { return "CLI API Version" }
func (English) AboutConnectedRegionLabel() string { return "Connected Region" }
func (English) AboutLocalOnly() string            { return "local only" }
func (English) AboutRegionHeader() string         { return "Region" }
func (English) AboutLatencyHeader() string        { return "Latency" }
func (English) AboutSessionsHeader() string       { return "Sessions" }
func (English) AboutNodesHeader() string          { return "Nodes" }
func (English) AboutNetworkTopologyTitle() string { return "Network Topology" }

func (English) ProfileLoading() string         { return "Loading..." }
func (English) ProfileLoadFailed() string      { return "Failed to load profile." }
func (English) ProfileUsername() string        { return "Username" }
func (English) ProfileLanguages() string       { return "Languages" }
func (English) ProfileEnterToEditHint() string { return "  enter to edit" }
func (English) ProfileLanguageSearchPlaceholder() string {
	return "search..."
}
func (English) ProfileAddLanguage() string    { return "+ add language" }
func (English) ProfileEnterToAddHint() string { return "  enter to add" }
func (English) ProfileNoLanguageMatches() string {
	return "no matches"
}

func (English) ProfileReplaysTitle(count int) string {
	if count <= 0 {
		return "Replays"
	}
	return fmt.Sprintf("Replays (%d)", count)
}

func (English) ProfileNoReplays() string       { return "  No replays yet." }
func (English) ProfileDeleteConfirm() string   { return "delete? y/n" }
func (English) ProfileUnknownGame() string     { return "unknown" }
func (English) ProfileNamePlaceholder() string { return "enter username" }

func (English) AboutNetworkSummary(regionCount, sessionCount int) string {
	sessionLabel := "active sessions"
	if sessionCount == 1 {
		sessionLabel = "active session"
	}
	return fmt.Sprintf("%d regions, %d %s", regionCount, sessionCount, sessionLabel)
}
