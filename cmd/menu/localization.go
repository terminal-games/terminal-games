// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/terminal-games/terminal-games/cmd/menu/carousel"
	"github.com/terminal-games/terminal-games/cmd/menu/gamelist"
	"golang.org/x/text/language"
)

type localizationChangedMsg struct {
	preferred []language.Tag
}

func localizationChanged(preferred []language.Tag) tea.Cmd {
	next := append([]language.Tag(nil), preferred...)
	return func() tea.Msg {
		return localizationChangedMsg{preferred: next}
	}
}

func sameLanguageTags(a, b []language.Tag) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type textKey string

const (
	textHeaderTitle    textKey = "header.title"
	textTabGames       textKey = "tab.games"
	textTabProfile     textKey = "tab.profile"
	textTabAbout       textKey = "tab.about"
	textWindowTooSmall textKey = "window.tooSmall"
	textUnknownTab     textKey = "tab.unknown"
	textAboutBody      textKey = "about.body"

	textHelpQuit    textKey = "help.quit"
	textHelpNextTab textKey = "help.nextTab"
	textHelpPrevTab textKey = "help.prevTab"

	textGamesListTitle      textKey = "games.listTitle"
	textGamesNoMatch        textKey = "games.noMatch"
	textCarouselScreenshots textKey = "carousel.screenshots"
	textCarouselEscToClose  textKey = "carousel.escToClose"

	textProfileNotSignedIn   textKey = "profile.notSignedIn"
	textProfileLoading       textKey = "profile.loading"
	textProfileLoadFailed    textKey = "profile.loadFailed"
	textProfileUsername      textKey = "profile.username"
	textProfileLanguages     textKey = "profile.languages"
	textProfileEnterToEdit   textKey = "profile.enterToEdit"
	textProfileSearch        textKey = "profile.search"
	textProfileAddLanguage   textKey = "profile.addLanguage"
	textProfileEnterToAdd    textKey = "profile.enterToAdd"
	textProfileNoMatches     textKey = "profile.noMatches"
	textProfileReplays       textKey = "profile.replays"
	textProfileReplaysCount  textKey = "profile.replaysCount"
	textProfileNoReplays     textKey = "profile.noReplays"
	textProfileDeleteConfirm textKey = "profile.deleteConfirm"
	textProfileUnknownGame   textKey = "profile.unknownGame"
	textProfileNameHint      textKey = "profile.nameHint"

	textHelpUp          textKey = "help.up"
	textHelpDown        textKey = "help.down"
	textHelpEdit        textKey = "help.edit"
	textHelpDelete      textKey = "help.delete"
	textHelpMoveUp      textKey = "help.moveUp"
	textHelpMoveDown    textKey = "help.moveDown"
	textHelpSave        textKey = "help.save"
	textHelpCancel      textKey = "help.cancel"
	textHelpDone        textKey = "help.done"
	textHelpNavigate    textKey = "help.navigate"
	textHelpSelect      textKey = "help.select"
	textHelpBack        textKey = "help.back"
	textHelpConfirm     textKey = "help.confirm"
	textHelpFilter      textKey = "help.filter"
	textHelpClearFilter textKey = "help.clearFilter"
	textHelpApplyFilter textKey = "help.applyFilter"
	textHelpPlay        textKey = "help.play"
	textCountOf         textKey = "count.of"
	textPlayButton      textKey = "play.button"
	textPlayLoading     textKey = "play.loading"
)

var supportedMenuLanguages = []language.Tag{
	language.English,
	language.German,
}

type localizer struct {
	matcher language.Matcher
	tag     language.Tag
}

func newLocalizer() localizer {
	return localizer{
		matcher: language.NewMatcher(supportedMenuLanguages),
		tag:     language.English,
	}
}

func (l *localizer) SetPreferred(preferred []language.Tag) bool {
	next := language.English
	if len(preferred) > 0 {
		next, _, _ = l.matcher.Match(preferred...)
	}
	if next == l.tag {
		return false
	}
	l.tag = next
	return true
}

func (l localizer) Text(key textKey) string {
	if l.tag == language.German {
		switch key {
		case textHeaderTitle:
			return "Terminal Games"
		case textTabGames:
			return "Spiele"
		case textTabProfile:
			return "Profil"
		case textTabAbout:
			return "Info"
		case textWindowTooSmall:
			return "Fenster ist zu klein"
		case textUnknownTab:
			return "Unbekannter Tab"
		case textAboutBody:
			return "Info"
		case textHelpQuit:
			return "beenden"
		case textHelpNextTab:
			return "nächster Tab"
		case textHelpPrevTab:
			return "voriger Tab"
		case textGamesListTitle:
			return "Offizielle Spiele"
		case textGamesNoMatch:
			return "Keine Spiele entsprechen dem aktuellen Filter."
		case textCarouselScreenshots:
			return "Screenshots"
		case textCarouselEscToClose:
			return "ESC zum Schließen"
		case textProfileNotSignedIn:
			return "Du bist nicht angemeldet."
		case textProfileLoading:
			return "Wird geladen..."
		case textProfileLoadFailed:
			return "Profil konnte nicht geladen werden."
		case textProfileUsername:
			return "Benutzername"
		case textProfileLanguages:
			return "Sprachen"
		case textProfileEnterToEdit:
			return "  enter zum Bearbeiten"
		case textProfileSearch:
			return "suche..."
		case textProfileAddLanguage:
			return "+ Sprache hinzufügen"
		case textProfileEnterToAdd:
			return "  enter zum Hinzufügen"
		case textProfileNoMatches:
			return "keine Treffer"
		case textProfileReplays:
			return "Replays"
		case textProfileReplaysCount:
			return "Replays (%d)"
		case textProfileNoReplays:
			return "  Noch keine Replays."
		case textProfileDeleteConfirm:
			return "löschen? y/n"
		case textProfileUnknownGame:
			return "unbekannt"
		case textProfileNameHint:
			return "benutzernamen eingeben"
		case textHelpUp:
			return "hoch"
		case textHelpDown:
			return "runter"
		case textHelpEdit:
			return "bearbeiten"
		case textHelpDelete:
			return "löschen"
		case textHelpMoveUp:
			return "nach oben"
		case textHelpMoveDown:
			return "nach unten"
		case textHelpSave:
			return "speichern"
		case textHelpCancel:
			return "abbrechen"
		case textHelpDone:
			return "fertig"
		case textHelpNavigate:
			return "navigieren"
		case textHelpSelect:
			return "auswählen"
		case textHelpBack:
			return "zurück"
		case textHelpConfirm:
			return "bestätigen"
		case textHelpFilter:
			return "filtern"
		case textHelpClearFilter:
			return "filter löschen"
		case textHelpApplyFilter:
			return "filter anwenden"
		case textHelpPlay:
			return "spielen"
		case textCountOf:
			return "von"
		case textPlayButton:
			return "Spielen"
		case textPlayLoading:
			return "Lade"
		}
	}
	switch key {
	case textHeaderTitle:
		return "Terminal Games"
	case textTabGames:
		return "Games"
	case textTabProfile:
		return "Profile"
	case textTabAbout:
		return "About"
	case textWindowTooSmall:
		return "Window must be larger"
	case textUnknownTab:
		return "Unknown tab"
	case textAboutBody:
		return "About"
	case textHelpQuit:
		return "quit"
	case textHelpNextTab:
		return "next tab"
	case textHelpPrevTab:
		return "prev tab"
	case textGamesListTitle:
		return "Official Games"
	case textGamesNoMatch:
		return "No games match the current filter."
	case textCarouselScreenshots:
		return "Screenshots"
	case textCarouselEscToClose:
		return "ESC to close"
	case textProfileNotSignedIn:
		return "You are not signed in."
	case textProfileLoading:
		return "Loading..."
	case textProfileLoadFailed:
		return "Failed to load profile."
	case textProfileUsername:
		return "Username"
	case textProfileLanguages:
		return "Languages"
	case textProfileEnterToEdit:
		return "  enter to edit"
	case textProfileSearch:
		return "search..."
	case textProfileAddLanguage:
		return "+ add language"
	case textProfileEnterToAdd:
		return "  enter to add"
	case textProfileNoMatches:
		return "no matches"
	case textProfileReplays:
		return "Replays"
	case textProfileReplaysCount:
		return "Replays (%d)"
	case textProfileNoReplays:
		return "  No replays yet."
	case textProfileDeleteConfirm:
		return "delete? y/n"
	case textProfileUnknownGame:
		return "unknown"
	case textProfileNameHint:
		return "enter username"
	case textHelpUp:
		return "up"
	case textHelpDown:
		return "down"
	case textHelpEdit:
		return "edit"
	case textHelpDelete:
		return "delete"
	case textHelpMoveUp:
		return "move up"
	case textHelpMoveDown:
		return "move down"
	case textHelpSave:
		return "save"
	case textHelpCancel:
		return "cancel"
	case textHelpDone:
		return "done"
	case textHelpNavigate:
		return "navigate"
	case textHelpSelect:
		return "select"
	case textHelpBack:
		return "back"
	case textHelpConfirm:
		return "confirm"
	case textHelpFilter:
		return "filter"
	case textHelpClearFilter:
		return "clear filter"
	case textHelpApplyFilter:
		return "apply filter"
	case textHelpPlay:
		return "play"
	case textCountOf:
		return "of"
	case textPlayButton:
		return "Play"
	case textPlayLoading:
		return "Loading"
	}
	return string(key)
}

func (l localizer) GameListLabels() gamelist.Labels {
	return gamelist.Labels{
		Up:          l.Text(textHelpUp),
		Down:        l.Text(textHelpDown),
		Filter:      l.Text(textHelpFilter),
		ClearFilter: l.Text(textHelpClearFilter),
		ApplyFilter: l.Text(textHelpApplyFilter),
		Cancel:      l.Text(textHelpCancel),
		Games:       l.Text(textTabGames),
		Of:          l.Text(textCountOf),
		FilterValue: l.Text(textHelpFilter),
	}
}

func (l localizer) CarouselLabels() carousel.Labels {
	return carousel.Labels{
		Screenshots: l.Text(textCarouselScreenshots),
		EscToClose:  l.Text(textCarouselEscToClose),
	}
}
