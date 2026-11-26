// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package benchmark

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const clusterID = "HIJ1rG6XThu4T7D5Jg8xIg"

type packet []byte

func (b *packet) i16(v int)        { *b = binary.BigEndian.AppendUint16(*b, uint16(v)) }
func (b *packet) i32(v int)        { *b = binary.BigEndian.AppendUint32(*b, uint32(v)) }
func (b *packet) compact(s string) { *b = append(*b, byte(len(s)+1)); *b = append(*b, s...) }

func (f *fixture) rpc(port, key, version int, request []byte) ([]byte, error) {
	c, e := connect(f.ctx, port)
	if e != nil {
		return nil, e
	}
	defer c.Close()
	if e = c.deadline(); e != nil {
		return nil, e
	}
	var frame packet
	frame.i16(key)
	frame.i16(version)
	frame.i32(28)
	frame.i16(-1)
	frame = append(frame, 0)
	frame = append(frame, request...)
	var wire packet
	wire.i32(len(frame))
	wire = append(wire, frame...)
	if _, e = io.Copy(c, bytes.NewReader(wire)); e != nil {
		return nil, e
	}
	var size [4]byte
	if _, e = io.ReadFull(c, size[:]); e != nil {
		return nil, e
	}
	n := binary.BigEndian.Uint32(size[:])
	if n < 5 || n > 65536 {
		return nil, errors.New("invalid quorum response length")
	}
	data := make([]byte, n)
	if _, e = io.ReadFull(c, data); e != nil {
		return nil, e
	}
	if binary.BigEndian.Uint32(data) != 28 || data[4] != 0 {
		return nil, errors.New("invalid quorum response header")
	}
	return data[5:], nil
}

type view struct {
	leader, epoch     int
	hw                int64
	voters, observers map[int]int64
}
type decoder struct {
	*bytes.Reader
	err error
}

func (d *decoder) read(v any) {
	if d.err == nil {
		d.err = binary.Read(d.Reader, binary.BigEndian, v)
	}
}
func (d *decoder) i8() int    { var v uint8; d.read(&v); return int(v) }
func (d *decoder) i16() int   { var v int16; d.read(&v); return int(v) }
func (d *decoder) i32() int   { var v int32; d.read(&v); return int(v) }
func (d *decoder) i64() int64 { var v int64; d.read(&v); return v }
func (d *decoder) replicas() map[int]int64 {
	m := map[int]int64{}
	n := d.i8() - 1
	if n < 0 {
		d.err = errors.New("null quorum replicas")
	}
	for i := 0; i < n; i++ {
		node, end := d.i32(), d.i64()
		m[node] = end
		if d.i8() != 0 {
			d.err = errors.New("unexpected quorum tags")
		}
	}
	return m
}

func (f *fixture) describe(node, group int) (*view, error) {
	topic := "__dragonfly_data"
	if group == 0 {
		topic = "__cluster_metadata"
	}
	p := packet{2}
	p.compact(topic)
	p = append(p, 2)
	p.i32(group)
	p = append(p, 0, 0, 0)
	b, e := f.rpc(f.nodes[node].raft, 55, 0, p)
	if e != nil {
		return nil, e
	}
	d := decoder{Reader: bytes.NewReader(b)}
	if d.i16() != 0 || d.i8() != 2 || d.i8() != len(topic)+1 {
		return nil, errors.New("invalid describe quorum response")
	}
	t := make([]byte, len(topic))
	_, d.err = io.ReadFull(d.Reader, t)
	if string(t) != topic || d.i8() != 2 || d.i32() != group {
		return nil, errors.New("wrong quorum topic or partition")
	}
	code := d.i16()
	if code == 6 {
		return nil, nil
	}
	if code != 0 {
		return nil, fmt.Errorf("describe quorum error %d", code)
	}
	v := &view{leader: d.i32(), epoch: d.i32(), hw: d.i64()}
	v.voters = d.replicas()
	v.observers = d.replicas()
	if d.i8() != 0 || d.i8() != 0 || d.i8() != 0 || d.Len() != 0 {
		return nil, errors.New("unexpected describe quorum suffix")
	}
	return v, d.err
}

func (f *fixture) changeVoter(leader, node int, add bool) error {
	var p packet
	p.compact(clusterID)
	key, version := 81, 0
	if add {
		key, version = 80, 1
		p.i32(5000)
	}
	p.i32(node)
	p = append(p, bytes.Repeat([]byte{byte(node)}, 16)...)
	if add {
		p = append(p, 2)
		p.compact("RAFT")
		p.compact("127.0.0.1")
		p.i16(f.nodes[node].raft)
		p = append(p, 0, 1)
	}
	p = append(p, 0)
	b, e := f.rpc(f.nodes[leader].raft, key, version, p)
	if e != nil {
		return e
	}
	if len(b) < 6 {
		return errors.New("short change voter response")
	}
	if code := int16(binary.BigEndian.Uint16(b[4:])); code != 0 {
		return fmt.Errorf("change voter error %d", code)
	}
	return nil
}
