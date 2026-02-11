// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unsafe"
)

const (
	menuReqErrNotMenuApp   = -1
	menuReqErrInvalidInput = -2
	menuReqErrTooMany      = -3

	menuPollPending         = -1
	menuPollErrInvalidID    = -2
	menuPollErrTaskFailed   = -3
	menuPollErrBufferSmall  = -4
	menuPollErrRequestError = -5
)

//go:wasmimport terminal_games menu_games_list_start
//go:noescape
func menu_games_list_start() int32

//go:wasmimport terminal_games menu_profile_get_start
//go:noescape
func menu_profile_get_start() int32

//go:wasmimport terminal_games menu_profile_set_start
//go:noescape
func menu_profile_set_start(usernamePtr unsafe.Pointer, usernameLen uint32, localePtr unsafe.Pointer, localeLen uint32) int32

//go:wasmimport terminal_games menu_replays_list_start
//go:noescape
func menu_replays_list_start(localePtr unsafe.Pointer, localeLen uint32) int32

//go:wasmimport terminal_games menu_replay_delete_start
//go:noescape
func menu_replay_delete_start(createdAt int64) int32

//go:wasmimport terminal_games menu_poll
//go:noescape
func menu_poll(requestID int32, dataPtr unsafe.Pointer, dataMaxLen uint32, dataLenPtr unsafe.Pointer) int32

type menuGamesPayload struct {
	Games []gameData `json:"games"`
}

type menuProfilePayload struct {
	Username string `json:"username"`
	Locale   string `json:"locale"`
}

type menuReplaysPayload struct {
	Replays []menuReplay `json:"replays"`
}

type menuReplay struct {
	CreatedAt    int64  `json:"created_at"`
	AsciinemaURL string `json:"asciinema_url"`
	GameTitle    string `json:"game_title"`
}

func menuFetchGames() ([]gameData, error) {
	data, err := menuWaitFor(menu_games_list_start())
	if err != nil {
		return nil, err
	}
	var payload menuGamesPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return payload.Games, nil
}

func menuFetchProfile() (string, string, error) {
	data, err := menuWaitFor(menu_profile_get_start())
	if err != nil {
		return "", "", err
	}
	var payload menuProfilePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", "", err
	}
	return payload.Username, payload.Locale, nil
}

func menuSaveProfile(username, locale string) error {
	usernameBytes := []byte(username)
	if len(usernameBytes) == 0 {
		usernameBytes = []byte{0}
	}
	localeBytes := []byte(locale)
	if len(localeBytes) == 0 {
		localeBytes = []byte{0}
	}
	requestID := menu_profile_set_start(
		unsafe.Pointer(&usernameBytes[0]),
		uint32(len(username)),
		unsafe.Pointer(&localeBytes[0]),
		uint32(len(locale)),
	)
	_, err := menuWaitFor(requestID)
	return err
}

func menuFetchReplays(locale string) ([]replay, error) {
	localeBytes := []byte(locale)
	if len(localeBytes) == 0 {
		localeBytes = []byte{0}
	}
	requestID := menu_replays_list_start(unsafe.Pointer(&localeBytes[0]), uint32(len(locale)))
	data, err := menuWaitFor(requestID)
	if err != nil {
		return nil, err
	}
	var payload menuReplaysPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	replays := make([]replay, 0, len(payload.Replays))
	for _, r := range payload.Replays {
		replays = append(replays, replay{
			createdAt:    r.CreatedAt,
			asciinemaURL: r.AsciinemaURL,
			gameTitle:    r.GameTitle,
		})
	}
	return replays, nil
}

func menuDeleteReplay(createdAt int64) error {
	_, err := menuWaitFor(menu_replay_delete_start(createdAt))
	return err
}

func menuWaitFor(requestID int32) ([]byte, error) {
	if requestID < 0 {
		return nil, menuRequestError(requestID)
	}
	buf := make([]byte, 64*1024)
	for {
		var dataLen uint32
		ret := menu_poll(
			requestID,
			unsafe.Pointer(&buf[0]),
			uint32(len(buf)),
			unsafe.Pointer(&dataLen),
		)
		switch ret {
		case menuPollPending:
			time.Sleep(time.Millisecond)
		case menuPollErrBufferSmall:
			if dataLen == 0 {
				return nil, errors.New("menu host returned empty resize length")
			}
			buf = make([]byte, dataLen)
		default:
			if ret < 0 {
				return nil, menuPollError(ret)
			}
			if dataLen == 0 {
				return nil, nil
			}
			if dataLen > uint32(len(buf)) {
				return nil, fmt.Errorf("invalid menu host payload length %d", dataLen)
			}
			return append([]byte(nil), buf[:dataLen]...), nil
		}
	}
}

func menuRequestError(code int32) error {
	switch code {
	case menuReqErrNotMenuApp:
		return errors.New("menu host call rejected outside menu app")
	case menuReqErrInvalidInput:
		return errors.New("menu host call rejected due to invalid input")
	case menuReqErrTooMany:
		return errors.New("menu host call rejected due to too many in-flight requests")
	default:
		return fmt.Errorf("menu host call failed with code %d", code)
	}
}

func menuPollError(code int32) error {
	switch code {
	case menuPollErrInvalidID:
		return errors.New("menu host poll used invalid request id")
	case menuPollErrTaskFailed:
		return errors.New("menu host task failed")
	case menuPollErrRequestError:
		return errors.New("menu host request failed")
	default:
		return fmt.Errorf("menu host poll failed with code %d", code)
	}
}
