// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package theme

import (
	"image/color"

	"charm.land/lipgloss/v2"

	"github.com/terminal-games/terminal-games/pkg/app"
)

var (
	Primary   color.Color
	Accent    color.Color
	Danger    color.Color
	OnPrimary color.Color

	Text       color.Color
	TextMuted  color.Color
	TextSubtle color.Color
	Surface    color.Color
	Line       color.Color
)

func init() {
	SetHasDarkBackground(true)
}

func SetHasDarkBackground(isDark bool) {
	Primary = lipgloss.Color("#98c379")
	OnPrimary = lipgloss.Color("#171717")

	if isDark {
		Danger = lipgloss.Color("#f87171")
		Accent = lipgloss.Color("#60a5fa")
		Text = lipgloss.Color("#f5f5f5")
		TextMuted = lipgloss.Color("#d4d4d4")
		TextSubtle = lipgloss.Color("#a3a3a3")
		Surface = lipgloss.Color("#404040")
		Line = lipgloss.Color("#525252")
		return
	}

	Danger = lipgloss.Color("#dc2626")
	Accent = lipgloss.Color("#2563eb")
	Text = lipgloss.Color("#262626")
	TextMuted = lipgloss.Color("#525252")
	TextSubtle = lipgloss.Color("#737373")
	Surface = lipgloss.Color("#e5e5e5")
	Line = lipgloss.Color("#d4d4d4")
}

func ConfigureFromTerminalInfo() {
	isDark := true
	info, err := app.GetTerminalInfo()
	if err == nil && info.HasDarkBackground {
		isDark = info.DarkBackground
	}

	SetHasDarkBackground(isDark)
}
