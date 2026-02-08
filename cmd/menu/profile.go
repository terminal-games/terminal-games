// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"os"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type profileModel struct {
	keys profileKeyMap
}

type profileKeyMap struct {
}

func newProfileModel() profileModel {
	return profileModel{keys: newProfileKeyMap()}
}

func newProfileKeyMap() profileKeyMap {
	return profileKeyMap{}
}

func (m profileModel) ShortHelp() []key.Binding {
	return m.keys.ShortHelp()
}

func (m profileModel) FullHelp() [][]key.Binding {
	return m.keys.FullHelp()
}

func (k profileKeyMap) ShortHelp() []key.Binding {
	return nil
}

func (k profileKeyMap) FullHelp() [][]key.Binding {
	return nil
}

func (m profileModel) Update(msg tea.Msg) (profileModel, tea.Cmd) {
	return m, nil
}

var (
	profileTitle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffffff")).Bold(true)
	profileBody   = lipgloss.NewStyle().Foreground(lipgloss.Color("#cccccc"))
	profileSubtle = lipgloss.NewStyle().Foreground(lipgloss.Color("#888888"))
)

func (m profileModel) renderProfileTab(width, height int) string {
	userID := os.Getenv("USER_ID")
	username := os.Getenv("USERNAME")
	if userID == "" {
		return lipgloss.NewStyle().Width(width).Height(height).Render(
			profileSubtle.Render("You are not signed in."),
		)
	}
	if username == "" {
		username = "—"
	}
	content := profileTitle.Render("Signed in") + "\n\n" +
		profileBody.Render("Username: "+username) + "\n" +
		profileBody.Render("User ID: "+userID)
	return lipgloss.NewStyle().Width(width).Height(height).Render(content)
}

func (m *model) renderProfileTab(width, height int) string {
	return m.profile.renderProfileTab(width, height)
}
