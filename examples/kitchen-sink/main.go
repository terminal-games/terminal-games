// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	zone "github.com/lrstanley/bubblezone"
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
	targetPeerID      peer.ID
	recentMessages    []peerMessage
}

type httpBodyMsg string

type tickMsg time.Time

type peerMessage struct {
	From          peer.ID
	Message       string
	Time          time.Time
	latencyAtTime uint32
}

type peerMsg peer.Message

func main() {
	r := bubblewrap.MakeRenderer()

	var targetID peer.ID
	appArgs := os.Getenv("APP_ARGS")
	if appArgs != "" {
		parsedID, err := peer.ParseID(appArgs)
		if err != nil {
			log.Fatalf("Warning: failed to parse target peer ID from APP_ARGS (%q): %v", appArgs, err)
		}
		targetID = parsedID
	}

	zone.NewGlobal()
	p := bubblewrap.NewProgram(model{
		timeLeft:          300,
		mainStyle:         lipgloss.NewStyle().Padding(1).Border(lipgloss.NormalBorder()),
		httpStyle:         lipgloss.NewStyle().Width(100),
		hasDarkBackground: r.HasDarkBackground(),
		peerID:            peer.CurrentID(),
		targetPeerID:      targetID,
		recentMessages:    make([]peerMessage, 0),
	}, tea.WithAltScreen(), tea.WithMouseAllMotion())

	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(tick(), listenForPeerMessage())
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := message.(type) {
	case tea.MouseMsg:
		m.x = msg.X
		m.y = msg.Y
		m.isHoveringZone = zone.Get("myId").InBounds(msg)
		return m, nil
	case tea.KeyMsg:
		m.lastChar = msg.String()
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
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
		case "p":
			message := fmt.Sprintf("Hello from %s at %s", m.peerID.String(), time.Now().Format("15:04:05"))
			return m, sendPeerMessage(m.targetPeerID, message)
		}
	case tickMsg:
		m.timeLeft--
		if m.timeLeft <= 0 {
			return m, tea.Quit
		}
		return m, tick()
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

	content := m.mainStyle.Render(fmt.Sprintf(
		"Hi. Last char: %v. Size: %vx%v Mouse: %v %v %v %s This program will exit in %d seconds...\n\n"+
			"Peer ID: %s\n"+
			"Press 'p' to send a message to peer %s\n\n"+
			"Recent Messages:\n%s\n\n"+
			"%v\n\n%+v\nhasDarkBackground=%v",
		m.lastChar, m.w, m.h, m.x, m.y, markedZone, TerminalOSC8Link("https://example.com", "example"), m.timeLeft,
		m.peerID.String(),
		m.targetPeerID.String(),
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
