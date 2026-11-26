// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package otel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func nativeEvent(id, name, request string, start int64) NativeEvent {
	return NativeEvent{TraceID: "stream", SpanID: id, Name: name, Start: strconv.FormatInt(start, 10), End: strconv.FormatInt(start+1, 10), Duration: "0.000001", Fields: map[string]string{"capture_id": "capture", "schema": "3", "stream_id": "stream", "event_id": id, "request_id": request, "process_instance": "process", "node_id": "11", "group_id": "10"}, Values: map[string]string{}, States: map[string]string{}}
}

func TestEventSlowPathRanksUnregisteredEventsWithoutAttributes(t *testing.T) {
	span := func(id, name, parent string, elapsed int64) NativeEvent {
		e := nativeEvent(id, name, parent, 9007199254740993)
		e.End = strconv.FormatInt(9007199254740993+elapsed, 10)
		e.Duration = milliseconds(elapsed)
		return e
	}
	a := span("1", "custom.operation", "0", 17909001)
	b := span("2", "another.operation", "0", 17909000)
	child := span("3", "arbitrary.child", "1", 2000000)
	point := span("4", "checkpoint", "0", 0)
	rows := []NativeEvent{a, b, child, point, nativeManifest(4)}
	r, err := RankNative("capture", rows, NativeQuery{Limit: 10})
	if err != nil || !r.Complete || r.Sort != "duration" || r.Attribute != "" || r.Total != 4 || r.Missing != 0 || r.Rows[0].SpanID != "1" || r.Rows[1].SpanID != "2" || r.Rows[3].Duration != "0.000000" {
		t.Fatalf("generic event ranking: %+v %v", r, err)
	}
	if len(r.Events) != 2 || r.Events[0].SpanID != "1" || r.Events[1].SpanID != "3" {
		t.Fatalf("expected selected event and explicit child, no temporal neighbours: %+v", r.Events)
	}
	options, names := r.Nodes, r.EventNames
	if len(options) != 1 || options[0].ID != "11" || !reflect.DeepEqual(names, []string{"another.operation", "arbitrary.child", "checkpoint", "custom.operation"}) {
		t.Fatalf("incomplete filter options: %+v %v", options, names)
	}
	r, err = RankNative("capture", rows, NativeQuery{Name: "another.operation", Node: "11", Minimum: "17", Limit: 1})
	if err != nil || !reflect.DeepEqual(r.Nodes, options) || !reflect.DeepEqual(r.EventNames, names) {
		t.Fatalf("filters and limit changed capture options: %+v %v", r, err)
	}
	r, err = RankNative("capture", rows, NativeQuery{Minimum: "17.909001", Limit: 10})
	if err != nil || r.Total != 1 || r.Rows[0].SpanID != "1" {
		t.Fatalf("event duration threshold lost precision: %+v %v", r, err)
	}
	r, err = RankNative("capture", rows, NativeQuery{Name: "another.operation", Limit: 10})
	if err != nil || r.Total != 1 || r.Rows[0].SpanID != "2" {
		t.Fatalf("event name filter failed: %+v %v", r, err)
	}
	link := span("5", "arbitrary.link", "0", 0)
	link.Fields["io_id"] = "2"
	if !nativeEventEvidence(b, link) || nativeEventEvidence(b, a) {
		t.Fatal("explicit I/O references must work independently of event names")
	}
}

func TestNativeNodeRolesRespectEpochAndExplicitRequestIdentity(t *testing.T) {
	first := nativeEvent("1", "raft.fetch", "1", 100)
	first.Fields["epoch"], first.Fields["peer_id"], first.Fields["outcome"] = "7", "12", "Data"
	second := nativeEvent("2", "raft.fetch", "2", 200)
	second.Fields["epoch"], second.Fields["node_id"], second.Fields["peer_id"], second.Fields["outcome"] = "8", "12", "11", "Empty"
	child := nativeEvent("3", "custom.child", "1", 110)
	io := nativeEvent("4", "raft.folly.fsync", "0", 111)
	io.Fields["role"] = "Log"
	other := nativeEvent("5", "custom.child", "1", 110)
	other.TraceID = "other"
	other.Fields["group_id"] = "20"
	failed := nativeEvent("6", "raft.fetch", "6", 300)
	failed.Fields["epoch"], failed.Fields["peer_id"], failed.Fields["outcome"] = "9", "99", "Error"
	explicit := nativeEvent("7", "custom.role", "0", 111)
	explicit.Fields["node_role"] = "Leader"
	unknown := nativeEvent("8", "custom.role", "0", 90)
	unknown.Fields["node_role"], unknown.Fields["epoch"] = "Unknown", "7"
	rows := []NativeEvent{first, second, child, io, other, failed, explicit, unknown}
	r, err := RankNative("capture", rows, NativeQuery{Name: "custom.child", Node: "11", Limit: 1, Selected: "stream:3"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.Nodes, []NativeNode{{ID: "11", Roles: []string{"Follower", "Leader"}}, {ID: "12", Roles: []string{"Follower", "Leader"}}}) {
		t.Fatalf("role-changing node options: %+v", r.Nodes)
	}
	if !reflect.DeepEqual(r.Selected.NodeRoles, []string{"Follower"}) {
		t.Fatalf("request child lost epoch role: %+v", r.Selected)
	}
	for _, tc := range []struct {
		selected string
		roles    []string
	}{
		{"stream:1", []string{"Follower"}},
		{"stream:2", []string{"Follower"}},
		{"stream:4", []string{"Leader"}},
		{"other:5", []string{}},
		{"stream:6", []string{}},
		{"stream:7", []string{"Leader"}},
		{"stream:8", []string{"Unknown"}},
	} {
		r, err := RankNative("capture", rows, NativeQuery{Limit: 1, Selected: tc.selected})
		if err != nil || !reflect.DeepEqual(r.Selected.NodeRoles, tc.roles) {
			t.Fatalf("roles for %s: %+v %v", tc.selected, r.Selected, err)
		}
	}
	if rows[0].NodeRoles != nil || rows[0].Fields["node_role"] != "" {
		t.Fatal("query mutated original capture")
	}
}

func TestLegacyEventRolesUseTheirOwnProcessAndTimeInterval(t *testing.T) {
	span := func(id, role, epoch string, start, end int64) NativeEvent {
		e := nativeEvent(id, "custom.event", "0", start)
		e.End, e.Duration = strconv.FormatInt(end, 10), milliseconds(end-start)
		e.Fields["node_role"], e.Fields["epoch"] = role, epoch
		return e
	}
	rows := []NativeEvent{
		span("unknown-before", "Unknown", "7", 0, 1),
		span("leader-before", "Leader", "7", 10, 20),
		span("leader-after", "Leader", "7", 80, 90),
		span("unknown-after", "Unknown", "7", 95, 96),
		span("follower", "Follower", "8", 100, 150),
		span("during-leader", "", "", 40, 60),
		span("during-follower", "", "", 120, 130),
		span("cross-transition", "", "", 85, 110),
		span("restarted", "", "", 40, 60),
	}
	rows[len(rows)-1].Fields["process_instance"] = "another-process"
	for _, tc := range []struct {
		id    string
		roles []string
	}{
		{"during-leader", []string{"Leader"}},
		{"during-follower", []string{"Follower"}},
		{"cross-transition", []string{}},
		{"restarted", []string{}},
	} {
		r, err := RankNative("capture", rows, NativeQuery{Limit: 1, Selected: "stream:" + tc.id})
		if err != nil || !reflect.DeepEqual(r.Selected.NodeRoles, tc.roles) {
			t.Fatalf("roles for %s: %+v %v", tc.id, r.Selected, err)
		}
	}
}

func nativeManifest(count int) NativeEvent {
	e := nativeEvent("manifest", "raft.capture", "0", 0)
	for k, v := range map[string]string{"event_count": strconv.Itoa(count), "expected_streams": "1", "expected_nodes": "11", "outcome": "Complete", "dropped": "0", "dropped_streams": "0"} {
		e.Fields[k] = v
	}
	return e
}

func TestNativeRankingUsesExactMillisecondsAndExplicitIOIDs(t *testing.T) {
	a, b := nativeEvent("1", "raft.fetch", "1", 100), nativeEvent("2", "raft.fetch", "2", 101)
	for _, e := range []*NativeEvent{&a, &b} {
		e.States["raft.fetch.return_wait"] = "Observed"
	}
	a.Values["raft.fetch.return_wait"] = "9007199254.740992"
	b.Values["raft.fetch.return_wait"] = "9007199254.740993"
	link := nativeEvent("3", "raft.fetch.io_dependency", "2", 101)
	link.Fields["io_id"] = "4"
	io := nativeEvent("4", "raft.folly.fsync", "0", 99)
	unrelated := nativeEvent("5", "raft.folly.fsync", "0", 99)
	missing := nativeEvent("6", "raft.fetch", "6", 100)
	missing.States["raft.fetch.return_wait"] = "Incomplete"
	rows := []NativeEvent{a, b, link, io, unrelated, missing, nativeManifest(6)}
	r, err := RankNative("capture", rows, NativeQuery{Name: "raft.fetch", Attribute: "raft.fetch.return_wait", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Complete || r.Total != 2 || r.Missing != 1 || r.Rows[0].SpanID != "2" {
		t.Fatalf("wrong ranking or completeness: %+v", r)
	}
	if len(r.Events) != 3 {
		t.Fatalf("wanted selected request, dependency and exact I/O, got %+v", r.Events)
	}
	for _, e := range r.Events {
		if e.SpanID == "5" {
			t.Fatal("included temporal neighbour without an explicit dependency")
		}
	}
	r, err = RankNative("capture", rows, NativeQuery{Name: "raft.fetch", Attribute: "raft.fetch.return_wait", Minimum: "9007199254.740993", Limit: 10})
	if err != nil || r.Total != 1 || r.Rows[0].SpanID != "2" {
		t.Fatalf("rounded away a one-nanosecond difference in millisecond values: %+v %v", r, err)
	}
	rows = append(rows[:5], rows[6])
	r, err = RankNative("capture", rows, NativeQuery{Name: "raft.fetch", Attribute: "raft.fetch.return_wait", Limit: 10})
	if err != nil || r.Complete {
		t.Fatalf("missing data reported complete: %+v %v", r, err)
	}
}

func nativeTestObservation(e NativeEvent) map[string]any {
	metadata := map[string]any{}
	for k, v := range e.Fields {
		metadata[k] = v
	}
	metadata["start_ns"], metadata["end_ns"] = json.Number(e.Start), json.Number(e.End)
	metadata["schema"], metadata["node_id"] = 3, 11
	for k, v := range e.Values {
		metadata["value."+k] = json.Number(v)
	}
	for k, v := range e.States {
		metadata["state."+k] = v
	}
	return map[string]any{
		"id": e.SpanID, "traceId": e.TraceID, "name": e.Name, "metadata": metadata,
		"startTime": "2026-11-25T18:48:53.942Z", "endTime": "2026-11-25T18:49:05.932Z",
	}
}

func nativeTestConfig(t *testing.T, server *httptest.Server) LangfuseConfig {
	t.Helper()
	t.Cleanup(server.Close)
	credentials := filepath.Join(t.TempDir(), "env")
	if err := os.WriteFile(credentials, []byte("LANGFUSE_PUBLIC_KEY=test\nLANGFUSE_SECRET_KEY=test\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return LangfuseConfig{URL: server.URL, EnvFile: credentials}
}

func TestNativeQueryReadsAllPagesAndPreservesMetadataPrecision(t *testing.T) {
	root := nativeManifest(1)
	e := nativeEvent("1", "raft.fetch", "1", 9007199254740993)
	e.Values["raft.fetch.return_wait"] = "17.909001"
	e.States["raft.fetch.return_wait"] = "Observed"
	pages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages++
		var filters []map[string]string
		if err := json.Unmarshal([]byte(r.URL.Query().Get("filter")), &filters); err != nil || len(filters) != 3 {
			t.Errorf("invalid capture filter: %v", filters)
		}
		if r.URL.Query().Get("cursor") == "" {
			json.NewEncoder(w).Encode(map[string]any{"data": []any{nativeTestObservation(root)}, "meta": map[string]string{"cursor": "page2"}})
		} else {
			json.NewEncoder(w).Encode(map[string]any{"data": []any{nativeTestObservation(e)}})
		}
	}))
	r, err := QueryNative(context.Background(), nativeTestConfig(t, server), "v15", "capture", NativeQuery{Name: "raft.fetch", Attribute: "raft.fetch.return_wait", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if pages != 2 || !r.Complete || r.Rows[0].Start != "9007199254740993" || r.Rows[0].Values["raft.fetch.return_wait"] != "17.909001" {
		t.Fatalf("native read changed values or skipped a page: %+v", r)
	}
}

func TestNativeReaderReusesCaptureWithFullMetadata(t *testing.T) {
	root := nativeTestObservation(nativeManifest(1))
	tickets := strings.Repeat("1234,", 50) + "5678"
	e := nativeEvent("1", "raft.fetch", "1", 100)
	e.Fields["ticket_ids"] = tickets
	full := nativeTestObservation(e)
	e.Fields["ticket_ids"] = tickets[:200]
	core := nativeTestObservation(e)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		q := r.URL.Query()
		rows := []any{root}
		if q.Get("name") == "raft.capture" {
			if q.Get("fields") != "core" || q.Has("expandMetadata") {
				t.Error("manifest request should only fetch core fields")
			}
		} else {
			if q.Get("fromStartTime") != "2026-11-25T18:48:53.941Z" || q.Get("toStartTime") != "2026-11-25T18:49:05.933Z" {
				t.Errorf("wrong capture bounds: %v", q)
			}
			if q.Get("expandMetadata") == "ticket_ids" {
				rows = append(rows, full)
			} else {
				rows = append(rows, core)
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	reader := NewNativeReader(NativeConfig{Langfuse: nativeTestConfig(t, server)})
	r, err := reader.Query(context.Background(), "v15", "capture", NativeQuery{Limit: 10})
	if err != nil || !r.Complete || r.Total != 1 || r.Selected == nil || r.Selected.Fields["ticket_ids"] != tickets {
		t.Fatalf("first query lost long metadata: %+v %v", r, err)
	}
	r, err = reader.Query(context.Background(), "v15", "capture", NativeQuery{Limit: 10, Name: "raft.fetch", Selected: "stream:1"})
	if err != nil || !r.Complete || r.Selected == nil || r.Selected.Fields["ticket_ids"] != tickets || calls.Load() != 3 {
		t.Fatalf("second query did not reuse the complete capture: %+v %v (calls %d)", r, err, calls.Load())
	}
	summary, err := reader.Summary(context.Background(), "v15", "capture")
	if err != nil || !summary.Complete || summary.Total != r.Total || len(summary.Stages) != 1 || calls.Load() != 3 {
		t.Fatalf("summary did not reuse the ranked capture: %+v %v (calls %d)", summary, err, calls.Load())
	}
}

func TestNativeSummaryCountsPointsAndSeparatesProcessClocks(t *testing.T) {
	span := func(id, name, stream, process, role string, start, end int64) NativeEvent {
		e := nativeEvent(id, name, "0", start)
		e.End, e.Duration = strconv.FormatInt(end, 10), milliseconds(end-start)
		e.TraceID, e.Fields["process_instance"], e.Fields["node_role"] = stream, process, role
		return e
	}

	// Preserve integer timestamp precision; merge overlapping streams only within one process.
	const origin int64 = 9007199254740993
	rows := []NativeEvent{
		span("1", "custom.operation", "stream", "process", "Follower", origin, origin+10_000_000),
		span("2", "custom.operation", "other-stream", "process", "Follower", origin+5_000_000, origin+15_000_000),
		span("3", "custom.operation", "restarted-stream", "restarted-process", "Follower", origin, origin+10_000_000),
		span("4", "custom.operation", "stream", "process", "Leader", origin, origin+1_000_000),
		span("5", "checkpoint", "stream", "process", "Leader", origin, origin),
		nativeManifest(3),
	}
	otherNode := span("6", "custom.operation", "node-12-stream", "node-12-process", "Follower", origin+30_000_000, origin+32_000_000)
	otherNode.Fields["node_id"] = "12"
	rows = append(rows, otherNode)
	data, err := prepareNativeCapture("capture", rows)
	if err != nil {
		t.Fatal(err)
	}
	r, err := data.eventSummary()
	if err != nil || r.Total != 6 || r.Complete || len(r.Issues) == 0 || len(r.EventNames) != 2 || len(r.Stages) != 4 {
		t.Fatalf("wrong counts or lost incomplete status: %+v %v", r, err)
	}
	for _, s := range r.Stages {
		switch {
		case s.Node == "12":
			if s.Count != 1 || s.Union != 2 || s.P95 != 2 {
				t.Fatalf("nodes sharing a role were combined: %+v", s)
			}
		case s.Role == "follower":
			if s.Count != 3 || s.Sum != 30 || s.Union != 25 || s.P50 != 10 || s.P95 != 10 || s.Max != 10 {
				t.Fatalf("incorrect overlapping stream statistics: %+v", s)
			}
		case s.Name == "checkpoint":
			if s.Count != 1 || s.Union != 0 || s.P95 != 0 {
				t.Fatalf("zero-duration event was lost: %+v", s)
			}
		case s.Role == "leader":
			if s.Count != 1 || s.P95 != 1 {
				t.Fatalf("roles were combined or duplicated: %+v", s)
			}
		}
	}
	windowed, err := data.eventSummary(Window{Start: origin + 9_000_000, End: origin + 11_000_000})
	if err != nil || windowed.Scope != "measurement" || windowed.Total != 3 || len(windowed.Stages) != 1 {
		t.Fatalf("incorrect window filtering: %+v %v", windowed, err)
	}
	s := windowed.Stages[0]
	if s.Count != 3 || s.Union != 3 || s.Sum != 4 || s.P95 != 2 {
		t.Fatalf("did not clip intervals at the measurement bounds: %+v", s)
	}
}

func TestNativeReaderRetriesIncompleteCapture(t *testing.T) {
	root := nativeTestObservation(nativeManifest(1))
	event := nativeTestObservation(nativeEvent("1", "raft.fetch", "1", 100))
	var ready atomic.Bool
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		q := r.URL.Query()
		rows := []any{root}
		if q.Get("name") == "" && !q.Has("fromStartTime") {
			if q.Get("expandMetadata") != "ticket_ids" {
				t.Error("unbounded fallback should fetch original metadata")
			}
			if ready.Load() {
				rows = append(rows, event)
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	reader := NewNativeReader(NativeConfig{Langfuse: nativeTestConfig(t, server)})
	r, err := reader.Query(context.Background(), "v15", "capture", NativeQuery{Limit: 10})
	if err != nil || r.Complete || calls.Load() != 3 {
		t.Fatalf("expected an incomplete result after the unbounded retry: %+v %v (calls %d)", r, err, calls.Load())
	}
	ready.Store(true)
	for i := 0; i < 2; i++ {
		r, err = reader.Query(context.Background(), "v15", "capture", NativeQuery{Limit: 10})
		if err != nil || !r.Complete || r.Total != 1 || calls.Load() != 6 {
			t.Fatalf("expected a newly complete result to be cached: %+v %v (calls %d)", r, err, calls.Load())
		}
	}
}

func TestNativeEvidenceRequiresExactBusinessIdentity(t *testing.T) {
	request := nativeEvent("1", "raft.fetch", "1", 100)
	request.Fields["epoch"], request.Fields["peer_id"], request.Fields["first_offset"], request.Fields["last_offset"] = "7", "12", "218", "232"
	leader := nativeEvent("2", "raft.hw.advance", "0", 9999)
	leader.TraceID = "leader"
	leader.Fields["epoch"], leader.Fields["node_id"], leader.Fields["previous_offset"], leader.Fields["end_offset"] = "7", "12", "220", "233"
	if !nativeLeaderEvidence(request, leader) {
		t.Fatal("missing exact offset coverage")
	}
	leader.Fields["epoch"] = "8"
	if nativeLeaderEvidence(request, leader) {
		t.Fatal("matched another epoch")
	}
	leader.Fields["epoch"], leader.Fields["end_offset"] = "7", "232"
	if nativeLeaderEvidence(request, leader) {
		t.Fatal("matched watermark that does not cover tail")
	}
	admission := nativeEvent("3", "raft.set.admission.wait", "3", 100)
	admission.Fields["last_wal_completion_ns"] = "123"
	wal := nativeEvent("4", "raft.wal.append.complete", "0", 123)
	if !nativeLeaderEvidence(admission, wal) {
		t.Fatal("missing explicit WAL completion")
	}
	wal.Start = "124"
	if nativeLeaderEvidence(admission, wal) {
		t.Fatal("matched temporal neighbour")
	}
}
