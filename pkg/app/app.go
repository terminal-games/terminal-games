// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package app

import (
	"errors"
	"fmt"
	"time"
	"unsafe"
)

//go:wasmimport terminal_games change_app
//go:noescape
func change_app(address_ptr unsafe.Pointer, addressLen uint32) int32

//go:wasmimport terminal_games next_app_ready
//go:noescape
func next_app_ready() int32

//go:wasmimport terminal_games graceful_shutdown_poll
//go:noescape
func graceful_shutdown_poll() int32

// Change asks the host to switch to another app identified by its shortname.
// The current guest should exit after calling this function so the host can
// start the next app.
func Change(shortname string) error {
	if shortname == "" {
		return fmt.Errorf("shortname is empty")
	}

	b := []byte(shortname)
	ret := change_app(unsafe.Pointer(&b[0]), uint32(len(b)))
	if ret < 0 {
		return fmt.Errorf("change_app failed")
	}

	return nil
}

// Ready reports whether the next app requested via Change is fully warmed in
// the host's module cache and ready to switch to.
//
// This can be called in a loop by the current guest before exiting to ensure
// the next app will start quickly once the host performs the switch. This is
// useful for building a loading UI
func Ready() bool {
	return next_app_ready() > 0
}

// GracefulShutdownPoll polls whether a graceful shutdown has been triggered by the host.
//
// Returns true if a graceful shutdown has been initiated, false otherwise.
// This can be called periodically by the guest to check if it should begin
// shutting down gracefully (e.g., saving state, closing connections, etc.)
// before the host forces a hard shutdown.
func GracefulShutdownPoll() bool {
	return graceful_shutdown_poll() > 0
}

//go:wasmimport terminal_games network_info
//go:noescape
func network_info(
	bytesPerSecInPtr unsafe.Pointer,
	bytesPerSecOutPtr unsafe.Pointer,
	lastThrottledMsPtr unsafe.Pointer,
	latencyMsPtr unsafe.Pointer,
) int32

// NetworkInfo holds network information from the host.
type NetworkInfo struct {
	// BytesPerSecIn is the receive rate in bytes per second.
	BytesPerSecIn float64
	// BytesPerSecOut is the send rate in bytes per second.
	BytesPerSecOut float64
	// LastThrottled is when the connection was last throttled. The zero value
	// means never throttled.
	LastThrottled time.Time
	// LatencyMs is the TCP RTT in milliseconds, or -1 if unavailable.
	LatencyMs int32
}

// GetNetworkInfo fetches network information from the host (throughput, throttling, RTT).
// Returns an error if the host call fails.
func GetNetworkInfo() (NetworkInfo, error) {
	var bytesPerSecIn float64
	var bytesPerSecOut float64
	var lastThrottledMs int64
	var latencyMs int32
	ret := network_info(
		unsafe.Pointer(&bytesPerSecIn),
		unsafe.Pointer(&bytesPerSecOut),
		unsafe.Pointer(&lastThrottledMs),
		unsafe.Pointer(&latencyMs),
	)
	if ret < 0 {
		return NetworkInfo{}, errors.New("network_info host call failed")
	}
	var lastThrottled time.Time
	if lastThrottledMs > 0 {
		lastThrottled = time.UnixMilli(lastThrottledMs)
	}
	return NetworkInfo{
		BytesPerSecIn:  bytesPerSecIn,
		BytesPerSecOut: bytesPerSecOut,
		LastThrottled:  lastThrottled,
		LatencyMs:      latencyMs,
	}, nil
}
