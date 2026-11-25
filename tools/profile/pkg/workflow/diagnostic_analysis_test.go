// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"slices"
	"testing"
)

func TestCPUCallPaths(t *testing.T) {
	const folded = `a;worker;crc 2
b;worker;crc 3
root;other;crc 4
root;other;crc;inner;crc 6
root;other;crc;tail 1
root;unrelated 4
`
	paths, total, err := CPUCallPaths(folded, "crc", true, 1, 3)
	if err != nil || total != 20 || paths.Matched != 16 || len(paths.Paths) != 3 {
		t.Fatalf("paths = %+v, total = %d, err = %v", paths, total, err)
	}
	for i, frames := range [][]string{{"inner", "crc"}, {"other", "crc"}, {"worker", "crc"}} {
		want := []int64{6, 5, 5}[i]
		if path := paths.Paths[i]; !slices.Equal(path.Frames, frames) || path.Samples != want || path.Percent != float64(want*5) {
			t.Errorf("path %d = %+v, want %v with %d samples", i, path, frames, want)
		}
	}
	leaf, _, err := CPUCallPaths(folded, "crc", false, 1, 0)
	if err != nil || leaf.Matched != 15 || len(leaf.Paths) != 0 {
		t.Fatalf("leaf summary = %+v, err = %v", leaf, err)
	}
}
