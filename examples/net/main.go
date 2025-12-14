// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	_ "github.com/terminal-games/terminal-games/pkg/net/http"
)

func main() {
	go func() {
		for {
			// slog.Info("spinning")
			time.Sleep(1 * time.Millisecond)
		}
	}()

	slog.Info("starting")

	resp, err := http.Get("https://example.com")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	fmt.Println("Response Status:", resp.Status)
	fmt.Println("Response Headers:", resp.Header)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Printf("\nBody:\n%s\n", string(body))

	slog.Info("Bye")
}
