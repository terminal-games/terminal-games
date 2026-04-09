package hosterr

import "fmt"

const VersionMismatch int32 = -32000

func MaybeVersionMismatch(api string, code int32) error {
	if code == VersionMismatch {
		return fmt.Errorf("terminal-games host version mismatch for %s", api)
	}
	return nil
}

func Bool(api string, code int32) (bool, error) {
	if code < 0 {
		if err := MaybeVersionMismatch(api, code); err != nil {
			return false, err
		}
		return false, fmt.Errorf("%s failed with code %d", api, code)
	}
	return code > 0, nil
}
