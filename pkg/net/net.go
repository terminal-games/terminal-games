package net

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
	"unsafe"
)

//go:wasmimport terminal_games dial
//go:noescape
func dial(address_ptr unsafe.Pointer, addressLen uint32, mode uint32) int32

//go:wasmimport terminal_games poll_dial
//go:noescape
func poll_dial(dial_id int32, local_addr_ptr unsafe.Pointer, local_addr_len_ptr unsafe.Pointer, remote_addr_ptr unsafe.Pointer, remote_addr_len_ptr unsafe.Pointer) int32

//go:wasmimport terminal_games conn_close
//go:noescape
func conn_close(conn_id int32) int32

//go:wasmimport terminal_games conn_write
//go:noescape
func conn_write(conn_id int32, address_ptr unsafe.Pointer, addressLen uint32) int32

//go:wasmimport terminal_games conn_read
//go:noescape
func conn_read(conn_id int32, address_ptr unsafe.Pointer, addressLen uint32) int32

const (
	dialErrAddressTooLong     = -1
	dialErrTooManyConnections = -2

	pollDialPending                 = -1
	pollDialErrInvalidDialID        = -2
	pollDialErrTaskFailed           = -3
	pollDialErrTooManyConnections   = -4
	pollDialErrDNSResolution        = -5
	pollDialErrNotGloballyReachable = -6
	pollDialErrConnectionFailed     = -7
	pollDialErrInvalidDNSName       = -8
	pollDialErrTLSHandshake         = -9

	connErrInvalidConnID   = -1
	connErrConnectionError = -2
)

var (
	ErrAddressTooLong       = errors.New("address too long")
	ErrTooManyConnections   = errors.New("too many connections (max 8)")
	ErrDialPending          = errors.New("dial still pending")
	ErrInvalidDialID        = errors.New("invalid dial ID")
	ErrDialTaskFailed       = errors.New("dial task failed")
	ErrDNSResolution        = errors.New("DNS resolution failed")
	ErrNotGloballyReachable = errors.New("address not globally reachable")
	ErrConnectionFailed     = errors.New("connection failed")
	ErrInvalidDNSName       = errors.New("invalid DNS name for TLS")
	ErrTLSHandshake         = errors.New("TLS handshake failed")
	ErrInvalidConnID        = errors.New("invalid connection ID")
	ErrDeadlineExceeded     = errors.New("deadline exceeded")
	ErrConnectionError      = errors.New("connection error")
)

type WasmHostConn struct {
	connID        int32
	localAddr     net.Addr
	remoteAddr    net.Addr
	mu            sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

var _ net.Conn = (*WasmHostConn)(nil)

type Dialer struct {
	UseTLS bool
}

func (d *Dialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	mode := uint32(0)
	if d.UseTLS {
		mode = 1
	}

	addressBytes := []byte(address)
	dialID := dial(unsafe.Pointer(&addressBytes[0]), uint32(len(addressBytes)), mode)
	if dialID < 0 {
		return nil, dialErrorFromCode(dialID)
	}

	var localAddrBuf [64]byte
	var remoteAddrBuf [64]byte
	var localAddrLen uint32
	var remoteAddrLen uint32

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		result := poll_dial(
			dialID,
			unsafe.Pointer(&localAddrBuf[0]),
			unsafe.Pointer(&localAddrLen),
			unsafe.Pointer(&remoteAddrBuf[0]),
			unsafe.Pointer(&remoteAddrLen),
		)
		if result >= 0 {
			localAddrStr := string(localAddrBuf[:localAddrLen])
			remoteAddrStr := string(remoteAddrBuf[:remoteAddrLen])
			conn := &WasmHostConn{
				connID:     result,
				localAddr:  &wasmAddr{addr: localAddrStr},
				remoteAddr: &wasmAddr{addr: remoteAddrStr},
			}
			return conn, nil
		}

		switch result {
		case pollDialPending:
			time.Sleep(1 * time.Millisecond)
			continue
		default:
			return nil, pollDialErrorFromCode(result)
		}
	}
}

func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d := &Dialer{UseTLS: false}
	return d.DialContext(ctx, network, address)
}

func DialTLS(ctx context.Context, network, address string) (net.Conn, error) {
	d := &Dialer{UseTLS: true}
	return d.DialContext(ctx, network, address)
}

func dialErrorFromCode(code int32) error {
	switch code {
	case dialErrAddressTooLong:
		return ErrAddressTooLong
	case dialErrTooManyConnections:
		return ErrTooManyConnections
	default:
		return errors.New("unknown dial error")
	}
}

func pollDialErrorFromCode(code int32) error {
	switch code {
	case pollDialPending:
		return ErrDialPending
	case pollDialErrInvalidDialID:
		return ErrInvalidDialID
	case pollDialErrTaskFailed:
		return ErrDialTaskFailed
	case pollDialErrTooManyConnections:
		return ErrTooManyConnections
	case pollDialErrDNSResolution:
		return ErrDNSResolution
	case pollDialErrNotGloballyReachable:
		return ErrNotGloballyReachable
	case pollDialErrConnectionFailed:
		return ErrConnectionFailed
	case pollDialErrInvalidDNSName:
		return ErrInvalidDNSName
	case pollDialErrTLSHandshake:
		return ErrTLSHandshake
	default:
		return errors.New("unknown poll_dial error")
	}
}

func (c *WasmHostConn) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	for {
		c.mu.Lock()
		deadline := c.readDeadline
		c.mu.Unlock()

		if !deadline.IsZero() && time.Now().After(deadline) {
			return 0, ErrDeadlineExceeded
		}

		readN := conn_read(c.connID, unsafe.Pointer(&b[0]), uint32(len(b)))
		if readN > 0 {
			return int(readN), nil
		}
		if readN == 0 {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		return 0, readErrorFromCode(readN)
	}
}

func readErrorFromCode(code int32) error {
	switch code {
	case connErrInvalidConnID:
		return ErrInvalidConnID
	case connErrConnectionError:
		return ErrConnectionError
	default:
		return errors.New("unknown read error")
	}
}

func (c *WasmHostConn) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	totalWritten := 0
	remaining := b

	for len(remaining) > 0 {
		c.mu.Lock()
		deadline := c.writeDeadline
		c.mu.Unlock()

		if !deadline.IsZero() && time.Now().After(deadline) {
			return totalWritten, ErrDeadlineExceeded
		}

		written := conn_write(c.connID, unsafe.Pointer(&remaining[0]), uint32(len(remaining)))

		if written > 0 {
			totalWritten += int(written)
			remaining = remaining[written:]
			continue
		}

		if written == 0 {
			time.Sleep(1 * time.Millisecond)
			continue
		}

		return totalWritten, writeErrorFromCode(written)
	}

	return totalWritten, nil
}

func writeErrorFromCode(code int32) error {
	switch code {
	case connErrInvalidConnID:
		return ErrInvalidConnID
	case connErrConnectionError:
		return ErrConnectionError
	default:
		return errors.New("unknown write error")
	}
}

func (c *WasmHostConn) Close() error {
	result := conn_close(c.connID)
	if result < 0 {
		return ErrInvalidConnID
	}
	return nil
}

func (c *WasmHostConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *WasmHostConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *WasmHostConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline = t
	c.writeDeadline = t
	return nil
}

func (c *WasmHostConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline = t
	return nil
}

func (c *WasmHostConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadline = t
	return nil
}

type wasmAddr struct {
	addr string
}

var _ net.Addr = (*wasmAddr)(nil)

func (a *wasmAddr) Network() string {
	return "tcp"
}

func (a *wasmAddr) String() string {
	if a == nil {
		return ""
	}
	return a.addr
}
