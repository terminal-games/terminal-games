// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	tea "github.com/charmbracelet/bubbletea"
)

type gamesModel struct {
}

func newGamesModel() gamesModel {
	return gamesModel{}
}

func (m gamesModel) Update(msg tea.Msg) (gamesModel, tea.Cmd) {
	return m, nil
}

func (m gamesModel) renderGamesTab(width, height int) string {
	return "Games"
}

func (m model) renderGamesTab(width, height int) string {
	return m.games.renderGamesTab(width, height)
}
