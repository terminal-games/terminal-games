package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"time"
	"unsafe"
)

//go:wasmimport terminal_games triple
//go:noescape
func triple(num int32) int32

//go:wasmimport terminal_games dial
//go:noescape
func dial(address_ptr unsafe.Pointer, addressLen uint32) int32

//go:wasmimport terminal_games dial_write
//go:noescape
func dial_write(conn_id int32, address_ptr unsafe.Pointer, addressLen uint32) int32

//go:wasmimport terminal_games dial_read
//go:noescape
func dial_read(conn_id int32, address_ptr unsafe.Pointer, addressLen uint32) int32

type conn int32

//go:embed cacert.pem
var caCertsPEM []byte

func main() {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.DialContext = DialContext
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCertsPEM) {
		panic("Failed to parse CA certificates")
	}

	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return DialContext(ctx, network, "1.1.1.1:53")
		},
	}

	http.DefaultClient = &http.Client{
		Transport: &http.Transport{
			DialContext: DialContext,
			TLSClientConfig: &tls.Config{
				RootCAs: certPool,
			},
		},
	}

	go func() {
		for {
			// slog.Info("spinning")
			runtime.Gosched()
		}
	}()

	slog.Info("pre get")

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

type MyDialer struct {
	c conn
}

func (m *MyDialer) Close() error {
	slog.Info("Close")
	return nil
}

func (m *MyDialer) LocalAddr() net.Addr {
	slog.Info("LocalAddr")
	return nil
}

func (m *MyDialer) Read(b []byte) (n int, err error) {
	slog.Info("Read")
	for {
		readN := dial_read(int32(m.c), unsafe.Pointer(&b[0]), uint32(len(b)))
		if readN > 0 {
			slog.Info("read", "n", readN)
			return int(readN), nil
		}
		runtime.Gosched()
		// slog.Info("hi")
		// time.Sleep(500 * time.Millisecond)
	}
}

func (m *MyDialer) RemoteAddr() net.Addr {
	slog.Info("RemoteAddr")
	return nil
}

func (m *MyDialer) SetDeadline(t time.Time) error {
	slog.Info("SetDeadline")
	return nil
}

func (m *MyDialer) SetReadDeadline(t time.Time) error {
	slog.Info("SetReadDeadline")
	return nil
}

func (m *MyDialer) SetWriteDeadline(t time.Time) error {
	slog.Info("SetWriteDeadline")
	return nil
}

func (m *MyDialer) Write(b []byte) (n int, err error) {
	slog.Info("Write", "b", string(b))

	nInt := dial_write(int32(m.c), unsafe.Pointer(&b[0]), uint32(len(b)))

	return int(nInt), nil
}

var _ net.Conn = (*MyDialer)(nil)

func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	slog.Info("DialContext", "network", network, "address", address)

	ret := dial(unsafe.Pointer(&[]byte(address)[0]), uint32(len(address)))
	if ret < 0 {
		slog.ErrorContext(ctx, "dial error", "ret", ret)
		return nil, errors.New("dial error")
	}

	d := &MyDialer{
		c: conn(ret),
	}

	return d, nil
}
