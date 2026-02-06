// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

type gamesModel struct {
	keys gamesKeyMap
}

type gamesKeyMap struct {
}

func newGamesModel() gamesModel {
	return gamesModel{keys: newGamesKeyMap()}
}

func newGamesKeyMap() gamesKeyMap {
	return gamesKeyMap{}
}

func (m gamesModel) ShortHelp() []key.Binding {
	return m.keys.ShortHelp()
}

func (m gamesModel) FullHelp() [][]key.Binding {
	return m.keys.FullHelp()
}

func (k gamesKeyMap) ShortHelp() []key.Binding {
	return nil
}

func (k gamesKeyMap) FullHelp() [][]key.Binding {
	return nil
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
