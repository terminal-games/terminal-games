// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	_ "embed"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"

	"github.com/terminal-games/terminal-games/pkg/app"
	"github.com/terminal-games/terminal-games/pkg/audio"
	"github.com/terminal-games/terminal-games/pkg/bubblewrap"
	_ "github.com/terminal-games/terminal-games/pkg/net/http"
	"github.com/terminal-games/terminal-games/pkg/peer"
)

type model struct {
	timeLeft          int
	lastChar          string
	w                 int
	h                 int
	mainStyle         lipgloss.Style
	httpStyle         lipgloss.Style
	x                 int
	y                 int
	isHoveringZone    bool
	httpBody          string
	hasDarkBackground bool
	peerID            peer.ID
	peers             []peer.ID
	selectedPeerIdx   int
	recentMessages    []peerMessage
	networkInfo       app.NetworkInfo
	audioPlaying      bool
	audioVolume       float32
}

type httpBodyMsg string

type tickMsg time.Time

type peerListMsg []peer.ID

type networkInfoMsg app.NetworkInfo

type peerMessage struct {
	From          peer.ID
	Message       string
	Time          time.Time
	latencyAtTime uint32
}

type peerMsg peer.Message

var isHoveringAudio = &atomic.Bool{}

//go:embed "Mesmerizing Galaxy Loop.ogg"
var songEmbed []byte

//go:embed "terminal-games.json"
var terminalGamesManifestJSON []byte

var (
	songResource audio.Resource
	song         *audio.Instance
)

func init() {
	if len(terminalGamesManifestJSON) == 0 {
		panic("missing terminal-games.json")
	}
	var err error
	songResource, err = audio.NewResourceFromOGGVorbis(songEmbed)
	if err != nil {
		panic(err)
	}
}

func main() {
	r := bubblewrap.MakeRenderer()

	var initialVolume float32 = 0.8
	var audioPlaying bool = false
	song = songResource.NewInstance()
	song.SetVolume(initialVolume)
	song.SetLoop(true)
	song.Play()
	audioPlaying = true

	zone.NewGlobal()
	p := bubblewrap.NewProgram(model{
		timeLeft:          300,
		mainStyle:         lipgloss.NewStyle().Padding(1).Border(lipgloss.NormalBorder()),
		httpStyle:         lipgloss.NewStyle().Width(100),
		hasDarkBackground: r.HasDarkBackground(),
		peerID:            peer.CurrentID(),
		peers:             []peer.ID{},
		selectedPeerIdx:   0,
		recentMessages:    make([]peerMessage, 0),
		networkInfo:       app.NetworkInfo{},
		audioPlaying:      audioPlaying,
		audioVolume:       initialVolume,
	}, tea.WithAltScreen(), tea.WithMouseAllMotion())

	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(tick(), listenForPeerMessage(), refreshPeerList(), refreshNetworkInfo(m.networkInfo))
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := message.(type) {
	case tea.MouseMsg:
		m.x = msg.X
		m.y = msg.Y
		m.isHoveringZone = zone.Get("myId").InBounds(msg)
		isHoveringAudio.Store(m.isHoveringZone)
		return m, nil
	case tea.KeyMsg:
		m.lastChar = msg.String()
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			if m.selectedPeerIdx > 0 {
				m.selectedPeerIdx--
			}
			return m, nil
		case "down", "j":
			if m.selectedPeerIdx < len(m.peers)-1 {
				m.selectedPeerIdx++
			}
			return m, nil
		case "enter":
			if len(m.peers) > 0 && m.selectedPeerIdx < len(m.peers) {
				targetPeer := m.peers[m.selectedPeerIdx]
				message := fmt.Sprintf("Hello from %s at %s", m.peerID.String(), time.Now().Format("15:04:05"))
				return m, sendPeerMessage(targetPeer, message)
			}
			return m, nil
		case "r":
			url := fmt.Sprintf("https://pokeapi.co/api/v2/pokemon?limit=1&offset=%v", rand.Int32N(800))

			m.httpBody = fmt.Sprintf("making request... %v", url)
			return m, func() tea.Msg {
				resp, err := http.Get(url)
				if err != nil {
					panic(err)
				}
				defer resp.Body.Close()

				body, err := io.ReadAll(resp.Body)
				if err != nil {
					panic(err)
				}

				return httpBodyMsg(body)
			}
		case "f":
			fmt.Fprintf(os.Stdout, "\x1b[%d;%dH%c", m.h+2, 2, 'A')
			return m, nil
		case " ":
			if song != nil {
				if m.audioPlaying {
					song.Pause()
				} else {
					song.Play()
				}
				m.audioPlaying = !m.audioPlaying
			}
			return m, nil
		case "+", "=":
			if song != nil {
				m.audioVolume += 0.1
				song.SetVolume(m.audioVolume)
			}
			return m, nil
		case "-", "_":
			if song != nil {
				m.audioVolume -= 0.1
				if m.audioVolume < 0 {
					m.audioVolume = 0
				}
				song.SetVolume(m.audioVolume)
			}
			return m, nil
		case "m":
			if song != nil {
				if m.audioVolume > 0 {
					m.audioVolume = 0
				} else {
					m.audioVolume = 0.8
				}
				song.SetVolume(m.audioVolume)
			}
			return m, nil
		}
	case tickMsg:
		m.timeLeft--
		if m.timeLeft <= 0 {
			return m, tea.Quit
		}
		return m, tea.Batch(tick(), refreshPeerList(), refreshNetworkInfo(m.networkInfo))
	case networkInfoMsg:
		m.networkInfo = app.NetworkInfo(msg)
		return m, nil
	case peerListMsg:
		m.peers = msg
		if m.selectedPeerIdx >= len(m.peers) {
			m.selectedPeerIdx = max(0, len(m.peers)-1)
		}
		return m, nil
	case httpBodyMsg:
		m.httpBody = string(msg)
		return m, nil
	case peerMsg:
		pm := peer.Message(msg)
		ms, _ := pm.From.Latency()
		receivedMsg := peerMessage{
			From:          pm.From,
			Message:       string(pm.Data),
			Time:          time.Now(),
			latencyAtTime: ms,
		}
		m.recentMessages = append(m.recentMessages, receivedMsg)
		if len(m.recentMessages) > 10 {
			m.recentMessages = m.recentMessages[len(m.recentMessages)-10:]
		}
		if receivedMsg.Message != "pong" {
			_ = pm.From.Send([]byte("pong"))
		}
		return m, listenForPeerMessage()
	case tea.WindowSizeMsg:
		m.w = msg.Width
		m.h = msg.Height
		return m, nil
	}

	return m, nil
}

func (m model) View() string {
	hoverString := "[hover me]"
	if m.isHoveringZone {
		hoverString = "[hello there]"
	}
	markedZone := zone.Mark("myId", hoverString)

	peerListText := "No peers connected"
	if len(m.peers) > 0 {
		peerListText = ""
		for i, p := range m.peers {
			if i > 0 {
				peerListText += "\n"
			}
			cursor := "  "
			if i == m.selectedPeerIdx {
				cursor = "> "
			}
			selfMarker := ""
			if p == m.peerID {
				selfMarker = " (you)"
			}
			latency, err := p.Latency()
			latencyStr := "local"
			if err == nil && latency > 0 {
				latencyStr = fmt.Sprintf("%dms", latency)
			}
			peerListText += fmt.Sprintf("%s%s [%s] %s%s", cursor, p.String(), p.Region(), latencyStr, selfMarker)
		}
	}

	peerMessagesText := "No messages yet"
	if len(m.recentMessages) > 0 {
		peerMessagesText = ""
		for i, msg := range m.recentMessages {
			if i > 0 {
				peerMessagesText += "\n"
			}
			peerMessagesText += fmt.Sprintf("[%s %vms] From %s: %s",
				msg.Time.Format("15:04:05"),
				msg.latencyAtTime,
				msg.From.String(),
				msg.Message,
			)
		}
	}

	lastThrottledStr := "never"
	if !m.networkInfo.LastThrottled.IsZero() {
		lastThrottledStr = m.networkInfo.LastThrottled.Format("15:04:05")
	}

	audioStatus := "No audio loaded"
	if song != nil {
		playState := "Paused"
		if m.audioPlaying {
			playState = "Playing"
		}
		audioStatus = fmt.Sprintf("%s | Volume: %.0f%% | Position: %v / %v",
			playState,
			m.audioVolume*100,
			song.CurrentTime().Truncate(time.Second),
			song.Duration().Truncate(time.Second),
		)
	}

	content := m.mainStyle.Render(fmt.Sprintf(
		"Hi. Last char: %v. Size: %vx%v Mouse: %v %v %v %s This program will exit in %d seconds...\n\n"+
			"Peer ID: %s\n\n"+
			"Audio: %s\n"+
			"  [Space]=Play/Pause  [+/-]=Volume  [M]=Mute\n\n"+
			"Network: ↑%.0f B/s ↓%.0f B/s RTT %dms throttled: %s\n\n"+
			"Peers (↑/↓ to select, Enter to send message):\n%s\n\n"+
			"Recent Messages:\n%s\n\n"+
			"%v\n\n%+v\nhasDarkBackground=%v",
		m.lastChar, m.w, m.h, m.x, m.y, markedZone, TerminalOSC8Link("https://example.com", "example"), m.timeLeft,
		m.peerID.String(),
		audioStatus,
		m.networkInfo.BytesPerSecIn, m.networkInfo.BytesPerSecOut, m.networkInfo.LatencyMs, lastThrottledStr,
		peerListText,
		peerMessagesText,
		m.httpStyle.Render(m.httpBody), os.Environ(), m.hasDarkBackground,
	))
	return zone.Scan(lipgloss.Place(m.w, m.h, lipgloss.Left, lipgloss.Top, content))
}

func TerminalOSC8Link(link, text string) string {
	return fmt.Sprintf("\x1b]8;;%s\x1b\\%s\x1b]8;;\x1b\\", link, text)
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func sendPeerMessage(targetID peer.ID, message string) tea.Cmd {
	return func() tea.Msg {
		err := peer.Send([]byte(message), targetID)
		if err != nil {
			log.Printf("Failed to send peer message: %v", err)
		}
		return nil
	}
}

func listenForPeerMessage() tea.Cmd {
	return func() tea.Msg {
		msg, err := peer.Recv()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
		}
		return peerMsg(msg)
	}
}

func refreshPeerList() tea.Cmd {
	return func() tea.Msg {
		peers, err := peer.List()
		if err != nil {
			return peerListMsg([]peer.ID{})
		}
		return peerListMsg(peers)
	}
}

func refreshNetworkInfo(prev app.NetworkInfo) tea.Cmd {
	return func() tea.Msg {
		info, err := app.GetNetworkInfo()
		if err != nil {
			return networkInfoMsg(prev)
		}
		return networkInfoMsg(info)
	}
}

func GenerateSine(frequency, amplitude float32, pts uint64, numSamples int) []float32 {
	samples := make([]float32, numSamples)
	twoPiF := 2.0 * math.Pi * float64(frequency)

	for i := 0; i < numSamples; i++ {
		t := float64(pts+uint64(i)) / audio.SampleRate
		value := amplitude * float32(math.Sin(twoPiF*t))
		samples[i] = value
	}

	return samples
}
