// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package benchmark

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type node struct {
	id, resp, raft int
	cmd            *exec.Cmd
	done           chan struct{}
	err            error
	logOffset      int64
}

type fixture struct {
	ctx                    context.Context
	config                 Config
	root, topology, voters string
	nodes                  map[int]*node
	order                  []int
	leader, other          int
}

// ProcessEnvironment isolates formal runs from inherited trace and CPU settings.
func ProcessEnvironment(extra []string) []string {
	values := map[string]string{}
	for _, item := range os.Environ() {
		k, v, _ := strings.Cut(item, "=")
		if strings.HasPrefix(k, "RAFT_TRACE_") || strings.HasPrefix(k, "OTEL_EXPORTER_") || strings.HasPrefix(k, "CPUPROFILE") ||
			strings.HasPrefix(k, "PROFILE_DIAGNOSTIC_") || k == "PROFILE_CPU_PROFILER" || k == "LD_PRELOAD" || k == "RAFT_OBSERVE_DIR" {
			continue
		}
		values[k] = v
	}
	for _, item := range extra {
		k, v, _ := strings.Cut(item, "=")
		values[k] = v
	}
	var result []string
	for k, v := range values {
		result = append(result, k+"="+v)
	}
	sort.Strings(result)
	return result
}

func (f *fixture) prepare() error {
	var err error
	f.root, err = os.MkdirTemp(filepath.Join(f.config.Output, "testdata"), "cluster-")
	if err != nil {
		return err
	}
	f.nodes = map[int]*node{}
	ids := []int{11, 12, 13, 21, 22}
	if f.config.Mode == "raft" {
		ids = []int{1, 2, 3, 11, 12, 21, 22, 13}
	}
	var reservations []net.Listener
	defer func() {
		for _, l := range reservations {
			_ = l.Close()
		}
	}()
	port := func() (int, error) {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			return 0, e
		}
		reservations = append(reservations, l)
		return l.Addr().(*net.TCPAddr).Port, nil
	}
	for _, id := range ids {
		n := &node{id: id}
		f.nodes[id] = n
		if id > 3 {
			if n.resp, err = port(); err != nil {
				return err
			}
		}
		if f.config.Mode == "raft" {
			if n.raft, err = port(); err != nil {
				return err
			}
		}
		if err = os.Mkdir(filepath.Join(f.root, strconv.Itoa(id)), 0755); err != nil {
			return err
		}
	}
	directory := func(id int) string { return base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{byte(id)}, 16)) }
	for _, id := range []int{1, 2, 3} {
		if n := f.nodes[id]; n != nil {
			f.voters += fmt.Sprintf(",%d@127.0.0.1:%d", id, n.raft)
		}
	}
	f.voters = strings.TrimPrefix(f.voters, ",")
	endpoint := func(id int) map[string]any {
		n := f.nodes[id]
		m := map[string]any{"id": fmt.Sprintf("%040x", id), "ip": "127.0.0.1", "port": n.resp}
		if f.config.Mode == "raft" {
			m["broker_id"] = id
			m["raft_port"] = n.raft
		}
		return m
	}
	var groups []map[string]any
	for i, ids := range [][]int{{11, 12, 13}, {21, 22}} {
		members := []map[string]any{}
		for _, id := range ids {
			members = append(members, endpoint(id))
		}
		g := map[string]any{"slot_ranges": []map[string]int{{"start": i * 8192, "end": i*8192 + 8191}}}
		if f.config.Mode == "raft" {
			g["group_id"] = (i + 1) * 10
			g["members"] = members
		} else {
			g["master"] = members[0]
			g["replicas"] = members[1:]
		}
		groups = append(groups, g)
	}
	if f.config.Mode == "raft" {
		f.topology = filepath.Join(f.root, "topology.json")
		b, _ := json.Marshal(map[string]any{"groups": groups})
		if err = os.WriteFile(f.topology, b, 0644); err != nil {
			return err
		}
		for _, id := range ids {
			args := []string{"format", filepath.Join(f.root, strconv.Itoa(id)), clusterID, strconv.Itoa(id)}
			if id == 11 || id == 12 || id == 13 {
				args = append(args, directory(id), "--data-group", "10")
				if id == 13 {
					args = append(args, "--no-initial-controllers")
				} else {
					args = append(args, "--initial-controllers", fmt.Sprintf("11@127.0.0.1:%d:%s,12@127.0.0.1:%d:%s", f.nodes[11].raft, directory(11), f.nodes[12].raft, directory(12)))
				}
			}
			cmd := exec.CommandContext(f.ctx, filepath.Join(filepath.Dir(f.config.Binary), "raft_storage"), args...)
			cmd.Env = ProcessEnvironment(nil)
			if b, e := cmd.CombinedOutput(); e != nil {
				return fmt.Errorf("format node %d: %w: %s", id, e, b)
			}
		}
	} else {
		b, _ := json.Marshal(groups)
		f.topology = string(b)
	}
	for _, l := range reservations {
		_ = l.Close()
	}
	for _, id := range ids {
		if err = f.start(id); err != nil {
			return err
		}
	}
	if err = f.poll(35*time.Second, func() (bool, error) {
		for _, id := range []int{11, 12, 13, 21, 22} {
			v, e := f.command(id, "INFO", "server")
			if e != nil {
				return false, e
			}
			text, ok := v.([]byte)
			if !ok || !strings.Contains(string(text), "multiplexing_api:iouring\r\n") {
				return false, errors.New("expected iouring client backend")
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	fmt.Fprintln(f.config.Log, "Client I/O backend verified: iouring (5 nodes)")
	if err = f.poll(35*time.Second, func() (bool, error) { return f.routes(false) }); err != nil {
		return err
	}
	if f.config.Mode == "raft" {
		if err = f.poll(35*time.Second, func() (bool, error) {
			v, e := f.describe(f.leader, 10)
			if e != nil || v == nil {
				return false, e
			}
			end, ok := v.observers[13]
			return ok && end >= v.hw && len(v.voters) == 2, nil
		}); err != nil {
			return err
		}
		if err = f.changeVoter(f.leader, 13, true); err != nil {
			return err
		}
	}
	return f.poll(35*time.Second, func() (bool, error) { return f.routes(true) })
}

func (f *fixture) start(id int) error {
	if err := f.ctx.Err(); err != nil {
		return err
	}
	n := f.nodes[id]
	args := []string{"--dir=" + filepath.Join(f.root, strconv.Itoa(id)), "--force_epoll=false", "--logtostderr"}
	if f.config.Mode == "raft" {
		args = append(args, "--cluster_mode=raft", fmt.Sprintf("--raft_node_id=%d", id), "--raft_cluster_id="+clusterID, "--raft_quorum_voters="+f.voters)
		if id < 4 {
			args = append(args, "--raft_role=controller", "--port=0", "--proactor_threads=1", "--raft_bootstrap_topology="+f.topology)
		} else {
			args = append(args, "--raft_role=data", "--raft_async_io=true", "--raft_drain_writes=true", "--raft_data_ack_mode="+f.config.Ack, fmt.Sprintf("--raft_listen=127.0.0.1:%d", n.raft))
			if id < 20 {
				args = append(args, "--raft_data_snapshot_bytes=4611686018427387904", "--raft_data_snapshot_interval_ms=0")
			}
		}
	} else {
		args = append(args, "--cluster_mode=yes", fmt.Sprintf("--cluster_node_id=%040x", id), "--dbfilename=")
	}
	if id > 3 {
		args = append(args, "--proactor_threads=2", fmt.Sprintf("--port=%d", n.resp), "--bind=127.0.0.1")
	}
	log, err := os.OpenFile(f.logPath(id), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	st, err := log.Stat()
	if err != nil {
		log.Close()
		return err
	}
	n.logOffset = st.Size()
	n.cmd = exec.Command(f.config.Binary, args...)
	n.cmd.Dir = filepath.Join(f.root, strconv.Itoa(id))
	n.cmd.Stdout, n.cmd.Stderr = log, log
	n.cmd.Env = f.processEnvironment(id)
	if err = n.cmd.Start(); err != nil {
		log.Close()
		return err
	}
	n.done = make(chan struct{})
	f.order = append(f.order, id)
	go func() { n.err = n.cmd.Wait(); log.Close(); close(n.done) }()
	if f.config.Mode == "yes" {
		if err = f.poll(35*time.Second, func() (bool, error) { v, e := f.command(id, "PING"); return e == nil && stringValue(v) == "PONG", e }); err != nil {
			return err
		}
		v, e := f.command(id, "DFLYCLUSTER", "CONFIG", f.topology)
		if err = expect(v, e, "OK"); err != nil {
			return err
		}
		upstream := 0
		if id == 12 || id == 13 {
			upstream = 11
		}
		if id == 22 {
			upstream = 21
		}
		if upstream != 0 {
			v, e = f.command(id, "REPLICAOF", "127.0.0.1", strconv.Itoa(f.nodes[upstream].resp))
			return expect(v, e, "OK")
		}
	}
	return nil
}

func (f *fixture) processEnvironment(id int) []string {
	extra := f.config.TraceEnvironment
	if f.config.Profiler != "" && (id == 11 || id == 12) {
		extra = append(append([]string{}, extra...), "LD_PRELOAD="+f.config.Profiler,
			"CPUPROFILE="+filepath.Join(f.config.Output, "cpu", fmt.Sprintf("node-%d.prof", id)), fmt.Sprintf("CPUPROFILESIGNAL=%d", syscall.SIGUSR2), "CPUPROFILE_FREQUENCY=500")
	}
	return ProcessEnvironment(extra)
}

func (f *fixture) logPath(id int) string {
	return filepath.Join(f.config.Output, "logs", fmt.Sprintf("%d.log", id))
}
func (f *fixture) command(id int, args ...string) (any, error) {
	c, e := connect(f.ctx, f.nodes[id].resp)
	if e != nil {
		return nil, e
	}
	defer c.Close()
	return c.command(args...)
}
func (f *fixture) poll(timeout time.Duration, check func() (bool, error)) error {
	return f.pollEvery(timeout, 100*time.Millisecond, check)
}

func (f *fixture) pollEvery(timeout, interval time.Duration, check func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(f.ctx, timeout)
	defer cancel()
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cluster wait: %w (last: %v)", err, last)
		}
		for _, n := range f.nodes {
			if n.cmd != nil {
				select {
				case <-n.done:
					return fmt.Errorf("node %d exited: %v: %s", n.id, n.err, f.tail(n.id))
				default:
				}
			}
		}
		ok, e := check()
		if e == nil && ok {
			return nil
		}
		last = e
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
	}
}
func (f *fixture) tail(id int) string {
	b, _ := os.ReadFile(f.logPath(id))
	if len(b) > 4000 {
		b = b[len(b)-4000:]
	}
	return string(b)
}

func (f *fixture) stop(id int, crash bool) error {
	n := f.nodes[id]
	if n == nil || n.cmd == nil {
		return nil
	}
	cmd := n.cmd
	signal := os.Signal(syscall.SIGTERM)
	if crash {
		signal = syscall.SIGKILL
	}
	err := cmd.Process.Signal(signal)
	timer := time.NewTimer(180 * time.Second)
	defer timer.Stop()
	select {
	case <-n.done:
	case <-timer.C:
		_ = cmd.Process.Kill()
		<-n.done
		err = errors.Join(err, errors.New("shutdown timed out"))
	}
	n.cmd = nil
	if crash {
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() && status.Signal() == syscall.SIGKILL {
			return err
		}
	}
	if n.err != nil {
		err = errors.Join(err, fmt.Errorf("node %d: %w: %s", id, n.err, f.tail(id)))
	}
	return err
}
func (f *fixture) close() error {
	var err error
	for i := len(f.order) - 1; i >= 0; i-- {
		err = errors.Join(err, f.stop(f.order[i], false))
	}
	return err
}
func stringValue(v any) string {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(v)
}

func (f *fixture) routes(promoted bool) (bool, error) {
	var rows []any
	for _, id := range []int{11, 12, 13, 21, 22} {
		v, e := f.command(id, "CLUSTER", "SLOTS")
		if e != nil {
			return false, e
		}
		a, ok := v.([]any)
		if !ok || len(a) != 2 {
			return false, nil
		}
		sort.Slice(a, func(i, j int) bool {
			x, _ := a[i].([]any)
			y, _ := a[j].([]any)
			return len(x) > 0 && len(y) > 0 && stringValue(x[0]) < stringValue(y[0])
		})
		if rows == nil {
			rows = a
		} else if !reflect.DeepEqual(rows, a) {
			return false, nil
		}
	}
	for i, raw := range rows {
		row, ok := raw.([]any)
		if !ok || len(row) < 3 || row[0] != int64(i*8192) || row[1] != int64(i*8192+8191) {
			return false, nil
		}
		members := []int{11, 12, 13}
		if i == 1 {
			members = []int{21, 22}
		}
		if promoted && len(row) != len(members)+2 {
			return false, nil
		}
		var found []int
		for _, raw := range row[2:] {
			p, ok := raw.([]any)
			if !ok || len(p) < 3 || stringValue(p[0]) != "127.0.0.1" {
				return false, nil
			}
			id := 0
			for _, n := range members {
				if p[1] == int64(f.nodes[n].resp) && stringValue(p[2]) == fmt.Sprintf("%040x", n) {
					id = n
				}
			}
			if id == 0 {
				return false, nil
			}
			found = append(found, id)
		}
		leader := found[0]
		if i == 0 {
			f.leader = leader
		} else {
			f.other = leader
		}
		if !promoted {
			continue
		}
		sort.Ints(found)
		if !reflect.DeepEqual(found, members) {
			return false, nil
		}
		if f.config.Mode == "raft" {
			v, e := f.describe(leader, (i+1)*10)
			if e != nil || !caughtUp(v, leader, members) {
				return false, e
			}
		} else {
			if leader != members[0] {
				return false, nil
			}
			v, e := f.command(leader, "ROLE")
			if e != nil {
				return false, e
			}
			role, ok := v.([]any)
			if !ok || len(role) < 2 || stringValue(role[0]) != "master" {
				return false, nil
			}
			replicas, ok := role[1].([]any)
			if !ok || len(replicas) != len(members)-1 {
				return false, nil
			}
			ports := map[string]bool{}
			for _, r := range replicas {
				a, ok := r.([]any)
				if !ok || len(a) < 3 || !online(a[2]) {
					return false, nil
				}
				ports[stringValue(a[1])] = true
			}
			for _, id := range members[1:] {
				if !ports[strconv.Itoa(f.nodes[id].resp)] {
					return false, nil
				}
				v, e = f.command(id, "ROLE")
				if e != nil {
					return false, e
				}
				a, ok := v.([]any)
				if !ok || len(a) != 4 || (stringValue(a[0]) != "slave" && stringValue(a[0]) != "replica") || stringValue(a[1]) != "127.0.0.1" || stringValue(a[2]) != strconv.Itoa(f.nodes[leader].resp) || !online(a[3]) {
					return false, nil
				}
			}
		}
	}
	if promoted && f.config.Mode == "raft" {
		for _, id := range []int{1, 2, 3} {
			v, e := f.describe(id, 0)
			if e != nil {
				return false, e
			}
			if caughtUp(v, id, []int{1, 2, 3}) {
				return true, nil
			}
		}
		return false, nil
	}
	return true, nil
}
func online(v any) bool { s := stringValue(v); return s == "online" || s == "stable_sync" }
func caughtUp(v *view, leader int, members []int) bool {
	if v == nil || v.leader != leader || v.epoch <= 0 || v.hw <= 0 || len(v.voters) != len(members) {
		return false
	}
	for _, id := range members {
		end, ok := v.voters[id]
		if !ok || end < v.hw {
			return false
		}
	}
	return true
}

func (f *fixture) prepareReadback() error {
	if f.config.Mode == "yes" {
		b, e := os.ReadFile(f.logPath(13))
		if e != nil {
			return e
		}
		if !bytes.Contains(b[f.nodes[13].logOffset:], []byte("Started full sync with ")) {
			return errors.New("recovered replica did not perform full sync")
		}
		return nil
	}
	files, e := filepath.Glob(filepath.Join(f.root, strconv.Itoa(f.leader), "__dragonfly_data-10", "*.checkpoint"))
	if e != nil {
		return e
	}
	for _, path := range files {
		name := filepath.Base(path)
		if len(name) >= 20 {
			offset, _ := strconv.ParseInt(name[:20], 10, 64)
			if offset > 0 {
				return errors.New("unexpected retained snapshot in fetch recovery benchmark")
			}
		}
	}
	other := 11
	if f.leader == 11 {
		other = 12
	}
	for _, id := range []int{other, f.leader} {
		if e = f.changeVoter(f.leader, id, false); e != nil {
			return e
		}
	}
	return f.poll(35*time.Second, func() (bool, error) {
		v, e := f.describe(13, 10)
		return v != nil && v.leader == 13 && len(v.voters) == 1 && v.voters[13] >= v.hw, e
	})
}
