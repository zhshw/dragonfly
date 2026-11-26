// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package benchmark

import "encoding/binary"

// Match random.Random(42).randbytes from the former Python workload, including the
// warmup continuing after all measured values. Payloads are generated before timing.
type payloadRandom struct {
	state [624]uint32
	index int
}

func newPayloadRandom() *payloadRandom {
	r := &payloadRandom{index: 624}
	r.state[0] = 19650218
	for i := 1; i < 624; i++ {
		r.state[i] = 1812433253*(r.state[i-1]^(r.state[i-1]>>30)) + uint32(i)
	}
	i := 1
	for k := 0; k < 624; k++ {
		r.state[i] = (r.state[i] ^ ((r.state[i-1] ^ (r.state[i-1] >> 30)) * 1664525)) + 42
		i++
		if i == 624 {
			r.state[0] = r.state[623]
			i = 1
		}
	}
	for k := 0; k < 623; k++ {
		r.state[i] = (r.state[i] ^ ((r.state[i-1] ^ (r.state[i-1] >> 30)) * 1566083941)) - uint32(i)
		i++
		if i == 624 {
			r.state[0] = r.state[623]
			i = 1
		}
	}
	r.state[0] = 0x80000000
	return r
}

func (r *payloadRandom) word() uint32 {
	if r.index == 624 {
		for i := 0; i < 624; i++ {
			y := (r.state[i] & 0x80000000) | (r.state[(i+1)%624] & 0x7fffffff)
			r.state[i] = r.state[(i+397)%624] ^ (y >> 1)
			if y&1 != 0 {
				r.state[i] ^= 0x9908b0df
			}
		}
		r.index = 0
	}
	y := r.state[r.index]
	r.index++
	y ^= y >> 11
	y ^= (y << 7) & 0x9d2c5680
	y ^= (y << 15) & 0xefc60000
	y ^= y >> 18
	return y
}

func (r *payloadRandom) value() []byte {
	b := make([]byte, 256<<10)
	for i := 0; i < len(b); i += 4 {
		binary.LittleEndian.PutUint32(b[i:], r.word())
	}
	return b
}
