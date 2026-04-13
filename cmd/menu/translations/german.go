// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package translations

import (
	"fmt"

	"github.com/terminal-games/terminal-games/cmd/menu/carousel"
	"github.com/terminal-games/terminal-games/cmd/menu/gamelist"
)

type German struct{}

func (German) HeaderTitle() string    { return "Terminal Games" }
func (German) TabGames() string       { return "Spiele" }
func (German) TabProfile() string     { return "Profil" }
func (German) TabAbout() string       { return "Über" }
func (German) WindowTooSmall() string { return "Das Fenster muss größer sein" }
func (German) UnknownTab() string     { return "Unbekannter Tab" }

func (German) HelpQuit() string        { return "beenden" }
func (German) HelpNextTab() string     { return "nächster Tab" }
func (German) HelpPrevTab() string     { return "vorheriger Tab" }
func (German) HelpUp() string          { return "hoch" }
func (German) HelpDown() string        { return "runter" }
func (German) HelpEdit() string        { return "bearbeiten" }
func (German) HelpDelete() string      { return "löschen" }
func (German) HelpMoveUp() string      { return "nach oben" }
func (German) HelpMoveDown() string    { return "nach unten" }
func (German) HelpSave() string        { return "speichern" }
func (German) HelpCancel() string      { return "abbrechen" }
func (German) HelpDone() string        { return "fertig" }
func (German) HelpNavigate() string    { return "navigieren" }
func (German) HelpSelect() string      { return "auswählen" }
func (German) HelpBack() string        { return "zurück" }
func (German) HelpConfirm() string     { return "bestätigen" }
func (German) HelpFilter() string      { return "filtern" }
func (German) HelpClearFilter() string { return "Filter löschen" }
func (German) HelpApplyFilter() string { return "Filter anwenden" }
func (German) HelpPlay() string        { return "spielen" }
func (German) HelpPageUp() string      { return "eine Seite hoch" }
func (German) HelpPageDown() string    { return "eine Seite runter" }
func (German) HelpDetailsUp() string   { return "Details hochscrollen" }
func (German) HelpDetailsDown() string { return "Details runterscrollen" }

func (German) GamesListTitle() string { return "Alle Spiele" }
func (German) GamesNoMatch() string {
	return "Keine Spiele entsprechen dem aktuellen Filter."
}
func (German) GamesByAuthor(author string) string {
	return "von " + author
}

func (German) GamesActiveSessions(count int, sessionsKnown bool) string {
	prefix := ""
	if !sessionsKnown && count > 0 {
		prefix = "~"
	}
	if count == 1 {
		return prefix + "1 aktive Sitzung"
	}
	return fmt.Sprintf("%s%d aktive Sitzungen", prefix, count)
}

func (t German) GameListLabels() gamelist.Labels {
	return gamelist.Labels{
		Up:          t.HelpUp(),
		Down:        t.HelpDown(),
		Filter:      t.HelpFilter(),
		ClearFilter: t.HelpClearFilter(),
		ApplyFilter: t.HelpApplyFilter(),
		Cancel:      t.HelpCancel(),
		Games:       t.TabGames(),
		Of:          "von",
		FilterValue: t.HelpFilter(),
	}
}

func (German) CarouselLabels() carousel.Labels {
	return carousel.Labels{
		Screenshots: "Screenshots",
		EscToClose:  "ESC zum Schließen",
	}
}

func (German) PlayButton() string  { return "Spielen" }
func (German) PlayLoading() string { return "Lade" }

func (German) AboutTitle() string {
	return "Spiele von überall aus, direkt im Terminal."
}

func (German) AboutBody() string {
	return "Terminal Games ist eine Open-Source-Plattform für Spiele, die für moderne Terminals und für SSH entwickelt wurden."
}

func (German) AboutDeveloperDocs(link string) string {
	return "Werde Spieleentwickler auf " + link
}

func (German) AboutCreditsHeading() string { return "Mitwirkende" }

func (German) AboutDevelopedBy(name string) string {
	return "Entwickelt von " + name
}

func (German) AboutOpenSource(link string) string {
	return "Open Source auf " + link
}

func (German) AboutServerVersionLabel() string   { return "Server-Version" }
func (German) AboutMenuVersionLabel() string     { return "Menü-Version" }
func (German) AboutCLIAPIVersionLabel() string   { return "CLI-API-Version" }
func (German) AboutConnectedRegionLabel() string { return "Verbundene Region" }
func (German) AboutLocalOnly() string            { return "nur lokal" }
func (German) AboutRegionHeader() string         { return "Region" }
func (German) AboutLatencyHeader() string        { return "Latenz" }
func (German) AboutSessionsHeader() string       { return "Sitzungen" }
func (German) AboutNodesHeader() string          { return "Knoten" }
func (German) AboutNetworkTopologyTitle() string { return "Netzwerk-Topologie" }

func (German) ProfileLoading() string         { return "Wird geladen..." }
func (German) ProfileLoadFailed() string      { return "Profil konnte nicht geladen werden." }
func (German) ProfileUsername() string        { return "Benutzername" }
func (German) ProfileLanguages() string       { return "Sprachen" }
func (German) ProfileEnterToEditHint() string { return "  Enter zum Bearbeiten" }
func (German) ProfileLanguageSearchPlaceholder() string {
	return "suchen..."
}
func (German) ProfileAddLanguage() string    { return "+ Sprache hinzufügen" }
func (German) ProfileEnterToAddHint() string { return "  Enter zum Hinzufügen" }
func (German) ProfileNoLanguageMatches() string {
	return "keine Treffer"
}

func (German) ProfileReplaysTitle(count int) string {
	if count <= 0 {
		return "Replays"
	}
	return fmt.Sprintf("Replays (%d)", count)
}

func (German) ProfileNoReplays() string       { return "  Noch keine Replays." }
func (German) ProfileDeleteConfirm() string   { return "löschen? y/n" }
func (German) ProfileUnknownGame() string     { return "unbekannt" }
func (German) ProfileNamePlaceholder() string { return "Benutzername eingeben" }

func (German) AboutNetworkSummary(regionCount, sessionCount int) string {
	sessionLabel := "aktive Sitzungen"
	if sessionCount == 1 {
		sessionLabel = "aktive Sitzung"
	}
	return fmt.Sprintf("%d Regionen, %d %s", regionCount, sessionCount, sessionLabel)
}
