// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package benchmark

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"time"
)

// A writer owns one socket for SET and WAIT. It never reconnects or retries writes.
type connection struct {
	net.Conn
	reader *bufio.Reader
	ctx    context.Context
}

func connect(ctx context.Context, port int) (*connection, error) {
	c, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	return &connection{c, bufio.NewReaderSize(c, 32<<10), ctx}, nil
}

func (c *connection) deadline() error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(12 * time.Second)
	if end, ok := c.ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	return c.SetDeadline(deadline)
}

func (c *connection) command(args ...string) (any, error) {
	parts := make([][]byte, len(args))
	for i, a := range args {
		parts[i] = []byte(a)
	}
	return c.bytesCommand(parts...)
}

func (c *connection) bytesCommand(args ...[]byte) (any, error) {
	if err := c.deadline(); err != nil {
		return nil, err
	}
	if err := writeCommands(c.Conn, [][][]byte{args}); err != nil {
		return nil, err
	}
	return readRESP(c.reader, 0)
}

func writeCommands(w io.Writer, commands [][][]byte) error {
	var buffers net.Buffers
	for _, args := range commands {
		buffers = append(buffers, []byte(fmt.Sprintf("*%d\r\n", len(args))))
		for _, arg := range args {
			buffers = append(buffers, []byte(fmt.Sprintf("$%d\r\n", len(arg))), arg, []byte("\r\n"))
		}
	}
	_, err := buffers.WriteTo(w)
	return err
}

func readRESP(r *bufio.Reader, depth int) (any, error) {
	if depth > 16 {
		return nil, errors.New("RESP nesting exceeds limit")
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return nil, errors.New("invalid RESP header")
	}
	text := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return []byte(text), nil
	case '-':
		return nil, fmt.Errorf("Redis: %s", text)
	case ':':
		return strconv.ParseInt(text, 10, 64)
	case '$', '*':
		n, err := strconv.Atoi(text)
		if err != nil || n < -1 || n > 64<<20 {
			return nil, errors.New("invalid RESP length")
		}
		if n == -1 {
			return nil, nil
		}
		if line[0] == '$' {
			b := make([]byte, n+2)
			if _, err := io.ReadFull(r, b); err != nil {
				return nil, err
			}
			if string(b[n:]) != "\r\n" {
				return nil, errors.New("invalid RESP bulk terminator")
			}
			return b[:n], nil
		}
		if n > 65536 {
			return nil, errors.New("RESP array exceeds limit")
		}
		a := make([]any, n)
		for i := range a {
			if a[i], err = readRESP(r, depth+1); err != nil {
				return nil, err
			}
		}
		return a, nil
	default:
		return nil, errors.New("unsupported RESP type")
	}
}

func expect(value any, err error, want any) error {
	if err != nil {
		return err
	}
	if b, ok := value.([]byte); ok {
		value = string(b)
	}
	if !reflect.DeepEqual(value, want) {
		return fmt.Errorf("expected %v, got %v", want, value)
	}
	return nil
}

func (c *connection) ok(args ...string) error { v, e := c.command(args...); return expect(v, e, "OK") }
func (c *connection) wait(replicas, timeout int) error {
	v, e := c.command("WAIT", strconv.Itoa(replicas), strconv.Itoa(timeout))
	return expect(v, e, int64(replicas))
}
