// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	tea "github.com/charmbracelet/bubbletea"
)

type profileModel struct {
}

func newProfileModel() profileModel {
	return profileModel{}
}

func (m profileModel) Update(msg tea.Msg) (profileModel, tea.Cmd) {
	return m, nil
}

func (m profileModel) renderProfileTab(width, height int) string {
	return "Profile"
}

func (m model) renderProfileTab(width, height int) string {
	return m.profile.renderProfileTab(width, height)
}
