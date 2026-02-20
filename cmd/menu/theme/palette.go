// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package theme

import "github.com/charmbracelet/lipgloss"

var (
	Primary   = lipgloss.AdaptiveColor{Light: "#98c379", Dark: "#98c379"}
	Accent    = lipgloss.AdaptiveColor{Light: "#2563eb", Dark: "#60a5fa"}
	Danger    = lipgloss.AdaptiveColor{Light: "#dc2626", Dark: "#f87171"}
	OnPrimary = lipgloss.AdaptiveColor{Light: "#171717", Dark: "#171717"}

	Text       = lipgloss.AdaptiveColor{Light: "#262626", Dark: "#f5f5f5"}
	TextMuted  = lipgloss.AdaptiveColor{Light: "#525252", Dark: "#d4d4d4"}
	TextSubtle = lipgloss.AdaptiveColor{Light: "#737373", Dark: "#a3a3a3"}
	Surface    = lipgloss.AdaptiveColor{Light: "#e5e5e5", Dark: "#404040"}
	Line       = lipgloss.AdaptiveColor{Light: "#d4d4d4", Dark: "#525252"}
)
