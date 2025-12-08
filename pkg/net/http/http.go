package http

import (
	"net/http"

	_ "golang.org/x/crypto/x509roots/fallback"

	"github.com/terminal-games/terminal-games/pkg/net"
)

func init() {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.DialContext = net.DialContext
	}
}
