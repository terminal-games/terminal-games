// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	tea "charm.land/bubbletea/v2"
	"github.com/terminal-games/terminal-games/cmd/menu/carousel"
	"github.com/terminal-games/terminal-games/cmd/menu/gamelist"
	"github.com/terminal-games/terminal-games/cmd/menu/translations"
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

var supportedMenuLanguages = []language.Tag{
	language.English,
	language.German,
	language.Spanish,
	language.Chinese,
	language.Japanese,
}

type menuTranslations interface {
	HeaderTitle() string
	TabGames() string
	TabProfile() string
	TabAbout() string
	WindowTooSmall() string
	UnknownTab() string

	HelpQuit() string
	HelpNextTab() string
	HelpPrevTab() string
	HelpUp() string
	HelpDown() string
	HelpEdit() string
	HelpDelete() string
	HelpMoveUp() string
	HelpMoveDown() string
	HelpSave() string
	HelpCancel() string
	HelpDone() string
	HelpNavigate() string
	HelpSelect() string
	HelpBack() string
	HelpConfirm() string
	HelpFilter() string
	HelpClearFilter() string
	HelpApplyFilter() string
	HelpPlay() string
	HelpPageUp() string
	HelpPageDown() string
	HelpDetailsUp() string
	HelpDetailsDown() string

	GamesListTitle() string
	GamesNoMatch() string
	GamesByAuthor(author string) string
	GamesActiveSessions(count int, sessionsKnown bool) string
	GameListLabels() gamelist.Labels
	CarouselLabels() carousel.Labels
	PlayButton() string
	PlayLoading() string

	AboutTitle() string
	AboutBody() string
	AboutDeveloperDocs(link string) string
	AboutCreditsHeading() string
	AboutDevelopedBy(name string) string
	AboutOpenSource(link string) string
	AboutServerVersionLabel() string
	AboutMenuVersionLabel() string
	AboutCLIAPIVersionLabel() string
	AboutConnectedRegionLabel() string
	AboutLocalOnly() string
	AboutRegionHeader() string
	AboutLatencyHeader() string
	AboutSessionsHeader() string
	AboutNodesHeader() string
	AboutNetworkSummary(regionCount, sessionCount int) string
	AboutNetworkTopologyTitle() string

	ProfileLoading() string
	ProfileLoadFailed() string
	ProfileUsername() string
	ProfileLanguages() string
	ProfileEnterToEditHint() string
	ProfileLanguageSearchPlaceholder() string
	ProfileAddLanguage() string
	ProfileEnterToAddHint() string
	ProfileNoLanguageMatches() string
	ProfileReplaysTitle(count int) string
	ProfileNoReplays() string
	ProfileDeleteConfirm() string
	ProfileUnknownGame() string
	ProfileNamePlaceholder() string
}

type localizer struct {
	menuTranslations
	matcher language.Matcher
	tag     language.Tag
}

var _ menuTranslations = translations.English{}
var _ menuTranslations = translations.German{}
var _ menuTranslations = translations.Spanish{}
var _ menuTranslations = translations.Chinese{}
var _ menuTranslations = translations.Japanese{}

func newLocalizer() localizer {
	tag := language.English
	return localizer{
		menuTranslations: translationsForTag(tag),
		matcher:          language.NewMatcher(supportedMenuLanguages),
		tag:              tag,
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
	l.menuTranslations = translationsForTag(next)
	return true
}

func translationsForTag(tag language.Tag) menuTranslations {
	if tag == language.German {
		return translations.German{}
	}
	if tag == language.Spanish {
		return translations.Spanish{}
	}
	if tag == language.Chinese {
		return translations.Chinese{}
	}
	if tag == language.Japanese {
		return translations.Japanese{}
	}
	return translations.English{}
}
