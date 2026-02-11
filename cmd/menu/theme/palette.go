// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package theme

import "github.com/charmbracelet/lipgloss"

var (
	Primary = lipgloss.AdaptiveColor{Light: "#50a14f", Dark: "#98c379"}
	Accent  = lipgloss.AdaptiveColor{Light: "#4078f2", Dark: "#61afef"}

	OnPrimary = lipgloss.AdaptiveColor{Light: "231", Dark: "235"}

	TextMuted  = lipgloss.AdaptiveColor{Light: "238", Dark: "252"}
	TextSubtle = lipgloss.AdaptiveColor{Light: "240", Dark: "249"}
	Line       = lipgloss.AdaptiveColor{Light: "245", Dark: "248"}
	Surface    = lipgloss.AdaptiveColor{Light: "252", Dark: "240"}
	Danger     = lipgloss.AdaptiveColor{Light: "160", Dark: "203"}
)
