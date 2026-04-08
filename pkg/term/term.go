// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package term

import (
	"fmt"
	"unsafe"

	"github.com/terminal-games/terminal-games/pkg/internal/hosterr"
)

//go:wasmimport terminal_games terminal_read_v1
//go:noescape
func terminal_read(address_ptr unsafe.Pointer, addressLen uint32) int32

//go:wasmimport terminal_games terminal_size_v1
//go:noescape
func terminal_size(width_ptr unsafe.Pointer, height_ptr unsafe.Pointer) int32

type dimensions struct {
	w uint16
	h uint16
}

func Read(buffer []byte) (int, error) {
	n := terminal_read(unsafe.Pointer(&buffer[0]), uint32(len(buffer)))
	if n < 0 {
		if err := hosterr.MaybeVersionMismatch("terminal_games.terminal_read_v1", n); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("terminal_read failed with code %d", n)
	}
	return int(n), nil
}

func Size() (width int, height int, err error) {
	var size dimensions
	ret := terminal_size(unsafe.Pointer(&size.w), unsafe.Pointer(&size.h))
	if ret < 0 {
		if err := hosterr.MaybeVersionMismatch("terminal_games.terminal_size_v1", ret); err != nil {
			return 0, 0, err
		}
		return 0, 0, fmt.Errorf("terminal_size failed with code %d", ret)
	}
	return int(size.w), int(size.h), nil
}
