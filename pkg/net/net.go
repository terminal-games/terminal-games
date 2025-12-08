package net

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"
	"unsafe"
)

//go:wasmimport terminal_games dial
//go:noescape
func dial(address_ptr unsafe.Pointer, addressLen uint32) int32

//go:wasmimport terminal_games conn_write
//go:noescape
func conn_write(conn_id int32, address_ptr unsafe.Pointer, addressLen uint32) int32

//go:wasmimport terminal_games conn_read
//go:noescape
func conn_read(conn_id int32, address_ptr unsafe.Pointer, addressLen uint32) int32

type conn int32

type WasmHostConn struct {
	c conn
}

func (m *WasmHostConn) Close() error {
	return nil
}

func (m *WasmHostConn) LocalAddr() net.Addr {
	return nil
}

func (m *WasmHostConn) Read(b []byte) (n int, err error) {
	for {
		readN := conn_read(int32(m.c), unsafe.Pointer(&b[0]), uint32(len(b)))
		if readN > 0 {
			return int(readN), nil
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (m *WasmHostConn) RemoteAddr() net.Addr {
	return nil
}

func (m *WasmHostConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *WasmHostConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *WasmHostConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (m *WasmHostConn) Write(b []byte) (n int, err error) {
	nInt := conn_write(int32(m.c), unsafe.Pointer(&b[0]), uint32(len(b)))
	return int(nInt), nil
}

var _ net.Conn = (*WasmHostConn)(nil)

func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	ret := dial(unsafe.Pointer(&[]byte(address)[0]), uint32(len(address)))
	if ret < 0 {
		slog.ErrorContext(ctx, "dial error", "ret", ret)
		return nil, errors.New("dial error")
	}

	d := &WasmHostConn{
		c: conn(ret),
	}

	return d, nil
}
