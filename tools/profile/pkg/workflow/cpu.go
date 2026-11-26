// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"dragonflydb.io/profile/pkg/otel"
)

type Hotspot struct {
	Name    string  `json:"function"`
	Samples int64   `json:"samples"`
	Percent float64 `json:"percent"`
}

type CallPath struct {
	Frames  []string `json:"frames"`
	Samples int64    `json:"samples"`
	Percent float64  `json:"percent"`
}

type CallPathReport struct {
	Symbol  string     `json:"symbol"`
	Matched int64      `json:"matched_samples"`
	Paths   []CallPath `json:"paths"`
}

// CPUHotspots ranks either leaf frames or all frames from a folded CPU profile.
func CPUHotspots(folded string, cumulative bool, limit int) ([]Hotspot, int64, error) {
	if limit < 0 {
		return nil, 0, errors.New("CPU hotspot limit must be nonnegative")
	}
	counts := map[string]int64{}
	total, err := scanFoldedStacks(folded, func(stack string, count int64) {
		frames := strings.Split(stack, ";")
		if !cumulative {
			frames = frames[len(frames)-1:]
		}
		seen := map[string]bool{}
		for _, frame := range frames {
			if !seen[frame] {
				counts[frame] += count
				seen[frame] = true
			}
		}
	})
	if err != nil {
		return nil, 0, err
	}
	var top []Hotspot
	for name, count := range counts {
		top = append(top, Hotspot{name, count, 100 * float64(count) / float64(total)})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].Samples != top[j].Samples {
			return top[i].Samples > top[j].Samples
		}
		return top[i].Name < top[j].Name
	})
	return top[:min(len(top), limit)], total, nil
}

// CPUCallPaths groups samples by the callers of a matching frame. In inclusive
// mode the match closest to the leaf represents each sample, including recursive calls.
func CPUCallPaths(folded, symbol string, inclusive bool, callerDepth, limit int) (CallPathReport, int64, error) {
	if symbol == "" || callerDepth < 0 || limit < 0 {
		return CallPathReport{}, 0, errors.New("call paths require a symbol and nonnegative caller depth and limit")
	}
	result := CallPathReport{Symbol: symbol, Paths: []CallPath{}}
	counts := map[string]int64{}
	total, err := scanFoldedStacks(folded, func(stack string, count int64) {
		frames := strings.Split(stack, ";")
		first := len(frames) - 1
		if inclusive {
			first = 0
		}
		for i := len(frames) - 1; i >= first; i-- {
			if !strings.Contains(frames[i], symbol) {
				continue
			}
			caller := 0
			if callerDepth > 0 {
				caller = max(0, i-callerDepth)
			}
			counts[strings.Join(frames[caller:i+1], ";")] += count
			result.Matched += count
			break
		}
	})
	if err != nil {
		return CallPathReport{}, 0, err
	}
	paths := sortedKeys(counts)
	sort.SliceStable(paths, func(i, j int) bool { return counts[paths[i]] > counts[paths[j]] })
	for _, path := range paths[:min(len(paths), limit)] {
		count := counts[path]
		result.Paths = append(result.Paths, CallPath{strings.Split(path, ";"), count, 100 * float64(count) / float64(total)})
	}
	return result, total, nil
}

func scanFoldedStacks(folded string, visit func(string, int64)) (int64, error) {
	var total int64
	scanner := bufio.NewScanner(strings.NewReader(folded))
	scanner.Buffer(make([]byte, 4096), 8<<20)
	for scanner.Scan() {
		line := scanner.Text()
		idx := strings.LastIndexByte(line, ' ')
		if idx < 1 {
			return 0, errors.New("malformed folded stack")
		}
		count, err := strconv.ParseInt(line[idx+1:], 10, 64)
		if err != nil || count <= 0 {
			return 0, errors.New("invalid CPU sample count")
		}
		total += count
		visit(line[:idx], count)
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	if total == 0 {
		return 0, errors.New("no CPU samples")
	}
	return total, nil
}

var (
	sampleLine = regexp.MustCompile(`^\s*(\d+)\s+\d+: ([\d ]+)\s*$`)
	frameLine  = regexp.MustCompile(`^\s*(\d+): (0x[\da-f]+)(?: M=\d+)?(?: (.*))?$`)
	fileSuffix = regexp.MustCompile(`\s+\S*:\d+(?: s=\d+)?$`)
	mapLine    = regexp.MustCompile(`^(\d+): (0x[\da-f]+)/(0x[\da-f]+)/(0x[\da-f]+) (\S+)`)
	locLine    = regexp.MustCompile(`^\s*(\d+): (0x[\da-f]+) M=(\d+)`)
)

func collapse(raw string) (string, error) {
	type sample struct {
		count int64
		ids   []string
	}
	var samples []sample
	frames := map[string]string{}
	section := ""
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(line, "Samples:") || strings.HasPrefix(line, "Locations") || strings.HasPrefix(line, "Mappings") {
			section = strings.SplitN(line, ":", 2)[0]
		} else if section == "Samples" {
			if m := sampleLine.FindStringSubmatch(line); m != nil {
				n, err := strconv.ParseInt(m[1], 10, 64)
				if err != nil || n <= 0 {
					return "", errors.New("invalid pprof sample count")
				}
				samples = append(samples, sample{n, strings.Fields(m[2])})
			}
		} else if section == "Locations" {
			if m := frameLine.FindStringSubmatch(line); m != nil {
				name := m[3]
				if name == "" {
					name = m[2]
				}
				frames[m[1]] = strings.ReplaceAll(fileSuffix.ReplaceAllString(name, ""), ";", ":")
			}
		}
	}
	counts := map[string]int64{}
	for _, s := range samples {
		stack := make([]string, len(s.ids))
		for i, id := range s.ids {
			name, ok := frames[id]
			if !ok {
				name = "unknown-" + id
			}
			stack[len(stack)-1-i] = name
		}
		counts[strings.Join(stack, ";")] += s.count
	}
	if len(counts) == 0 {
		return "", errors.New("pprof has no CPU samples")
	}
	var result strings.Builder
	for _, stack := range sortedKeys(counts) {
		fmt.Fprintf(&result, "%s %d\n", stack, counts[stack])
	}
	return result.String(), nil
}

func capture(ctx context.Context, input string, log io.Writer, args ...string) (string, error) {
	var stdout bytes.Buffer
	err := executeIO(ctx, "", nil, args, strings.NewReader(input), &stdout, log, log)
	return stdout.String(), err
}

func symbolize(ctx context.Context, raw string, log io.Writer) (string, error) {
	type mapping struct {
		start, offset uint64
		path          string
	}
	type location struct {
		id      string
		address uint64
	}
	mappings := map[string]mapping{}
	byFile := map[string][]location{}
	frames := map[string]string{}
	lines := strings.Split(raw, "\n")
	parse := func(s string) uint64 { n, _ := strconv.ParseUint(s, 0, 64); return n }
	section := ""
	for _, line := range lines {
		if strings.HasPrefix(line, "Mappings") {
			section = "Mappings"
			continue
		}
		if section == "Mappings" {
			if m := mapLine.FindStringSubmatch(line); m != nil {
				mappings[m[1]] = mapping{parse(m[2]), parse(m[4]), m[5]}
			}
		}
	}
	section = ""
	for _, line := range lines {
		if strings.HasPrefix(line, "Locations") {
			section = "Locations"
			continue
		}
		if strings.HasPrefix(line, "Mappings") {
			section = "Mappings"
		}
		if section != "Locations" {
			continue
		}
		if m := locLine.FindStringSubmatch(line); m != nil {
			mp, ok := mappings[m[3]]
			if !ok || parse(m[2]) < mp.start {
				return "", errors.New("invalid pprof ELF mapping")
			}
			relative := parse(m[2]) - mp.start + mp.offset
			frames[m[1]] = fmt.Sprintf("[%s+%#x]", filepath.Base(mp.path), relative)
			if st, err := os.Stat(mp.path); err == nil && st.Mode().IsRegular() {
				byFile[mp.path] = append(byFile[mp.path], location{m[1], relative})
			}
		}
	}
	for _, path := range sortedKeys(byFile) {
		entries := byFile[path]
		var input strings.Builder
		for _, e := range entries {
			fmt.Fprintf(&input, "%#x\n", e.address)
		}
		output, err := capture(ctx, input.String(), log, "addr2line", "-f", "-C", "-e", path)
		if err != nil {
			return "", err
		}
		names := strings.Split(strings.TrimSuffix(output, "\n"), "\n")
		if len(names) != len(entries)*2 {
			return "", errors.New("unexpected addr2line output")
		}
		for i, e := range entries {
			if names[i*2] != "??" {
				frames[e.id] = strings.ReplaceAll(names[i*2], ";", ":")
			}
		}
	}
	for i, line := range lines {
		if m := locLine.FindStringSubmatch(line); m != nil && frames[m[1]] != "" {
			lines[i] = fmt.Sprintf("%s: %s M=%s %s :0 s=0", m[1], m[2], m[3], frames[m[1]])
		}
	}
	return strings.Join(lines, "\n"), nil
}

func cpuReports(ctx context.Context, run, out string, m otel.Metadata, opt ReportOptions, log io.Writer) ([]string, error) {
	var prefixes []string
	for _, node := range sortedKeys(m.Nodes) {
		prefix := m.Nodes[node].Role + "-node-" + node
		var raw string
		var err error
		profiles, e := filepath.Glob(filepath.Join(run, "cpu", "node-"+node+".prof.*"))
		if e != nil || len(profiles) == 0 {
			return nil, fmt.Errorf("no CPU profile for %s", prefix)
		}
		args := append([]string{opt.Go, "tool", "pprof", "-symbolize=none", "-raw", m.Binary}, profiles...)
		raw, err = capture(ctx, "", log, args...)
		if err == nil {
			raw, err = symbolize(ctx, raw, log)
		}
		if err != nil {
			return nil, err
		}
		folded, err := collapse(raw)
		if err != nil {
			return nil, err
		}
		files := map[string]string{prefix + ".raw.txt": raw, prefix + ".folded": folded}
		for _, cumulative := range []bool{false, true} {
			top, total, err := CPUHotspots(folded, cumulative, 40)
			if err != nil {
				return nil, err
			}
			var text strings.Builder
			fmt.Fprintf(&text, "CPU samples: %d; period: %.3f ms; cumulative=%v\nsamples    percent    function\n", total, 1000/m.Frequency, cumulative)
			for _, item := range top {
				fmt.Fprintf(&text, "%7d    %7.2f%%    %s\n", item.Samples, item.Percent, item.Name)
			}
			suffix := ".top.txt"
			if cumulative {
				suffix = ".cumulative.txt"
			}
			files[prefix+suffix] = text.String()
		}
		svg, err := capture(ctx, folded, log, "perl", filepath.Join(opt.Flamegraph, "flamegraph.pl"), "--title", fmt.Sprintf("%s %s CPU (%g Hz)", m.Mode, m.Nodes[node].Role, m.Frequency), "--countname", "samples")
		if err != nil {
			return nil, err
		}
		files[prefix+".svg"] = svg
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(out, name), []byte(content), 0644); err != nil {
				return nil, err
			}
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
