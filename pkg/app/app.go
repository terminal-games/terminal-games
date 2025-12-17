// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package app

import (
	"fmt"
	"unsafe"
)

//go:wasmimport terminal_games change_app
//go:noescape
func change_app(address_ptr unsafe.Pointer, addressLen uint32) int32

//go:wasmimport terminal_games next_app_ready
//go:noescape
func next_app_ready() int32

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
