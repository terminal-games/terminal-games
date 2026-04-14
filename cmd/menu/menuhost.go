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

const (
	menuReqGamesList    = 0
	menuReqProfileGet   = 1
	menuReqProfileSet   = 2
	menuReqReplaysList  = 3
	menuReqReplayDelete = 4
	menuReqAboutStatus  = 5
	menuReqGameActivity = 6
)

//go:wasmimport terminal_games menu_request_v1
//go:noescape
func menu_request(typ int32, ptr1 unsafe.Pointer, len1 uint32, ptr2 unsafe.Pointer, len2 uint32, extra int64) int32

//go:wasmimport terminal_games menu_poll_v1
//go:noescape
func menu_poll(requestID int32, dataPtr unsafe.Pointer, dataMaxLen uint32, dataLenPtr unsafe.Pointer) int32

type menuGamesPayload struct {
	Apps []gameData `json:"apps"`
}

type menuGameActivityPayload struct {
	Apps          []menuGameActivity `json:"apps"`
	SessionsKnown bool               `json:"sessions_known"`
}

type menuGameActivity struct {
	AppID          int64 `json:"app_id"`
	ActiveSessions int   `json:"active_sessions"`
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

type aboutHostPayload struct {
	HostKind          string      `json:"host_kind"`
	ServerVersion     *string     `json:"server_version"`
	CliAPIVersion     string      `json:"cli_api_version"`
	CurrentRegion     string      `json:"current_region"`
	Nodes             []aboutNode `json:"nodes"`
	RefreshedAtUnixMs int64       `json:"refreshed_at_unix_ms"`
}

type aboutNode struct {
	NodeID         string   `json:"node_id"`
	LatencyMS      int      `json:"latency_ms"`
	ActiveSessions int      `json:"active_sessions"`
	SessionsKnown  bool     `json:"sessions_known"`
	IsCurrent      bool     `json:"is_current"`
	RegionCode     *string  `json:"region_code"`
	RegionName     *string  `json:"region_name"`
	LatitudeDeg    *float64 `json:"latitude_deg"`
	LongitudeDeg   *float64 `json:"longitude_deg"`
}

func menuFetchGames() ([]gameData, error) {
	data, err := menuWaitFor(menu_request(menuReqGamesList, nil, 0, nil, 0, 0))
	if err != nil {
		return nil, err
	}
	var payload menuGamesPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return payload.Apps, nil
}

func menuFetchGameActivity() (map[int64]gameActivity, error) {
	data, err := menuWaitFor(menu_request(menuReqGameActivity, nil, 0, nil, 0, 0))
	if err != nil {
		return nil, err
	}
	var payload menuGameActivityPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	activityByID := make(map[int64]gameActivity, len(payload.Apps))
	for _, app := range payload.Apps {
		activityByID[app.AppID] = gameActivity{
			ActiveSessions: app.ActiveSessions,
			SessionsKnown:  payload.SessionsKnown,
		}
	}
	return activityByID, nil
}

func menuFetchProfile() (string, string, error) {
	data, err := menuWaitFor(menu_request(menuReqProfileGet, nil, 0, nil, 0, 0))
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
	requestID := menu_request(
		menuReqProfileSet,
		unsafe.Pointer(&usernameBytes[0]),
		uint32(len(username)),
		unsafe.Pointer(&localeBytes[0]),
		uint32(len(locale)),
		0,
	)
	_, err := menuWaitFor(requestID)
	return err
}

func menuFetchReplays(locale string) ([]replay, error) {
	localeBytes := []byte(locale)
	if len(localeBytes) == 0 {
		localeBytes = []byte{0}
	}
	requestID := menu_request(menuReqReplaysList, unsafe.Pointer(&localeBytes[0]), uint32(len(locale)), nil, 0, 0)
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
	_, err := menuWaitFor(menu_request(menuReqReplayDelete, nil, 0, nil, 0, createdAt))
	return err
}

func menuFetchAboutStatus() (aboutHostPayload, error) {
	data, err := menuWaitFor(menu_request(menuReqAboutStatus, nil, 0, nil, 0, 0))
	if err != nil {
		return aboutHostPayload{}, err
	}
	var payload aboutHostPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return aboutHostPayload{}, err
	}
	return payload, nil
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
