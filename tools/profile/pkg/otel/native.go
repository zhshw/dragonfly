// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package otel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// NativeEvent contains values computed by C++, never inferred from neighbouring observations.
// NodeRoles is display metadata resolved from recorded role or Fetch group/epoch evidence.
// Millisecond attributes and original nanosecond timestamps remain decimal strings across JSON.
type NativeEvent struct {
	TraceID   string            `json:"trace_id"`
	SpanID    string            `json:"span_id"`
	Name      string            `json:"name"`
	Start     string            `json:"start_ns"`
	End       string            `json:"end_ns"`
	Duration  string            `json:"duration_ms"`
	Fields    map[string]string `json:"fields"`
	Values    map[string]string `json:"values"`
	States    map[string]string `json:"states"`
	NodeRoles []string          `json:"node_roles"`
}

// NativeNode lists the roles evidenced for a node across the complete capture.
type NativeNode struct {
	ID    string   `json:"id"`
	Roles []string `json:"roles"`
}

type NativeCapture struct {
	ID         string `json:"id"`
	Experiment string `json:"experiment_id"`
	Release    string `json:"release"`
	BinarySHA  string `json:"binary_sha"`
	Workload   string `json:"workload"`
	Started    string `json:"started"`
	Streams    int    `json:"streams"`
	Client     bool   `json:"client,omitempty"`
}

type NativeQuery struct {
	Name      string
	Attribute string
	Node      string
	Group     string
	Outcome   string
	Partial   string
	Minimum   string
	Limit     int
	Selected  string
	Start     int64
	End       int64
}

type NativeReport struct {
	Capture    string            `json:"capture_id"`
	Complete   bool              `json:"complete"`
	Issues     []string          `json:"issues"`
	Total      int               `json:"total"`
	Missing    int               `json:"missing"`
	Sort       string            `json:"sort"`
	Attribute  string            `json:"attribute,omitempty"`
	Units      map[string]string `json:"units"`
	Rows       []NativeEvent     `json:"rows"`
	Selected   *NativeEvent      `json:"selected,omitempty"`
	Events     []NativeEvent     `json:"events"`
	Nodes      []NativeNode      `json:"nodes"`
	EventNames []string          `json:"event_names"`
}

const maxNativeObservations = 250_000

type nativeObservation struct {
	ID       string         `json:"id"`
	TraceID  string         `json:"traceId"`
	Project  string         `json:"projectId"`
	Name     string         `json:"name"`
	Start    string         `json:"startTime"`
	End      string         `json:"endTime"`
	Metadata map[string]any `json:"metadata"`
}

func flattenMetadata(prefix string, value any, out map[string]string) {
	switch v := value.(type) {
	case map[string]any:
		for k, item := range v {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			flattenMetadata(key, item, out)
		}
	case string:
		out[prefix] = v
	case json.Number:
		out[prefix] = v.String()
	}
}

func decodeNative(row nativeObservation) (NativeEvent, error) {
	fields := map[string]string{}
	flattenMetadata("", row.Metadata, fields)
	e := NativeEvent{TraceID: row.TraceID, SpanID: row.ID, Name: row.Name, Start: fields["start_ns"], End: fields["end_ns"], Fields: fields, Values: map[string]string{}, States: map[string]string{}}
	if fields["schema"] != "3" || fields["stream_id"] != row.TraceID || fields["capture_id"] == "" {
		return e, errors.New("invalid native trace identity")
	}
	start, err := strconv.ParseInt(e.Start, 10, 64)
	if err != nil {
		return e, fmt.Errorf("invalid start_ns: %w", err)
	}
	end, err := strconv.ParseInt(e.End, 10, 64)
	if err != nil || start < 0 || end < start {
		return e, errors.New("invalid native span boundaries")
	}
	e.Duration = milliseconds(end - start)
	for key, value := range fields {
		if strings.HasPrefix(key, "value.") {
			if _, err := NativeNumber(value); err != nil {
				return e, fmt.Errorf("invalid native value %s", key)
			}
			e.Values[strings.TrimPrefix(key, "value.")] = value
			delete(fields, key)
		} else if strings.HasPrefix(key, "state.") {
			e.States[strings.TrimPrefix(key, "state.")] = value
			delete(fields, key)
		}
	}
	return e, nil
}

func milliseconds(value int64) string {
	return fmt.Sprintf("%d.%06d", value/1000000, value%1000000)
}

// NativeNumber preserves saved decimal precision for sorting and thresholds. No business
// duration is derived here; values already use the unit published in the capture manifest.
func NativeNumber(value string) (*big.Rat, error) {
	if len(value) == 0 || len(value) > 128 || !json.Valid([]byte(value)) || !strings.ContainsAny(value[:1], "-0123456789") {
		return nil, errors.New("invalid numeric value")
	}
	if i := strings.IndexAny(value, "eE"); i >= 0 {
		exponent, err := strconv.ParseInt(value[i+1:], 10, 32)
		if err != nil || exponent < -308 || exponent > 308 {
			return nil, errors.New("numeric exponent out of range")
		}
	}
	n, ok := new(big.Rat).SetString(value)
	if !ok {
		return nil, errors.New("invalid numeric value")
	}
	return n, nil
}

func NativeCaptures(ctx context.Context, config LangfuseConfig, experiment string) ([]NativeCapture, error) {
	c, err := openLangfuse(config)
	if err != nil {
		return nil, err
	}
	rows, err := c.nativeObservations(ctx, experiment, "", url.Values{
		"name": {"raft.capture"}, "fields": {"core,basic,metadata"}, "expandMetadata": {"ticket_ids"},
	})
	if err != nil {
		return nil, err
	}
	byID := map[string]*NativeCapture{}
	for _, row := range rows {
		e, err := decodeNative(row)
		if err != nil {
			return nil, err
		}
		id := e.Fields["capture_id"]
		if byID[id] == nil {
			byID[id] = &NativeCapture{ID: id, Experiment: experiment, Release: e.Fields["release"], BinarySHA: e.Fields["binary_sha"], Workload: e.Fields["workload"], Started: row.Start}
		}
		byID[id].Streams++
		byID[id].Client = byID[id].Client || e.Fields["node_id"] == "client"
		if row.Start < byID[id].Started {
			byID[id].Started = row.Start
		}
	}
	out := make([]NativeCapture, 0, len(byID))
	for _, item := range byID {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Started > out[j].Started })
	return out, nil
}

func QueryNative(ctx context.Context, config LangfuseConfig, experiment, capture string, query NativeQuery) (NativeReport, error) {
	if capture == "" || experiment == "" {
		return NativeReport{}, errors.New("experiment and capture are required")
	}
	c, err := openLangfuse(config)
	if err != nil {
		return NativeReport{}, err
	}
	events, err := c.nativeEvents(ctx, experiment, capture, url.Values{
		"fields": {"core,basic,metadata"}, "expandMetadata": {"ticket_ids"},
	})
	if err != nil {
		return NativeReport{}, err
	}
	return RankNative(capture, events, query)
}

type nativeCaptureData struct {
	events  []NativeEvent
	summary NativeReport
}

func prepareNativeCapture(capture string, events []NativeEvent) (*nativeCaptureData, error) {
	r := NativeReport{Capture: capture, Issues: []string{}, Units: map[string]string{}}
	manifests := map[string]NativeEvent{}
	counts := map[string]int{}
	processes := map[string]int{}
	nodes := map[string]bool{}
	for _, e := range events {
		if e.Fields["capture_id"] != capture {
			return nil, errors.New("mixed capture identity")
		}
		if e.Name == "raft.capture" {
			manifests[e.TraceID] = e
			processes[e.Fields["process_instance"]]++
			nodes[e.Fields["node_id"]] = true
		} else {
			counts[e.TraceID]++
		}
	}
	if len(manifests) == 0 {
		r.Issues = append(r.Issues, "No capture manifests")
	}
	for id, count := range counts {
		if _, ok := manifests[id]; !ok {
			r.Issues = append(r.Issues, fmt.Sprintf("Stream %s has %d events without a manifest", id, count))
		}
	}
	identity := map[string]string{}
	for id, m := range manifests {
		for _, key := range []string{"binary_sha", "release", "workload", "experiment_id"} {
			if previous, ok := identity[key]; ok && previous != m.Fields[key] {
				r.Issues = append(r.Issues, "Inconsistent "+key)
			}
			identity[key] = m.Fields[key]
		}
		for key, unit := range m.Fields {
			if strings.HasPrefix(key, "unit.") {
				r.Units[strings.TrimPrefix(key, "unit.")] = unit
			}
		}
		want, err := strconv.Atoi(m.Fields["event_count"])
		if err != nil || want != counts[id] {
			r.Issues = append(r.Issues, "Event count mismatch for stream "+id)
		}
		if m.Fields["outcome"] != "Complete" || m.Fields["dropped"] != "0" || m.Fields["dropped_streams"] != "0" {
			r.Issues = append(r.Issues, "Incomplete stream "+id)
		}
		want, err = strconv.Atoi(m.Fields["expected_streams"])
		if err != nil || want != processes[m.Fields["process_instance"]] {
			r.Issues = append(r.Issues, "Missing process streams for "+m.Fields["process_instance"])
		}
		if m.Fields["expected_nodes"] == "" {
			r.Issues = append(r.Issues, "Node coverage is unspecified")
		}
		for _, node := range strings.Split(m.Fields["expected_nodes"], ",") {
			if node != "" && !nodes[node] {
				r.Issues = append(r.Issues, "Missing node "+node)
			}
		}
	}
	sort.Strings(r.Issues)
	r.Issues = compactStrings(r.Issues)
	r.Complete = len(r.Issues) == 0
	events = append([]NativeEvent(nil), events...)
	r.Nodes, r.EventNames = nativeOptions(events)
	return &nativeCaptureData{events: events, summary: r}, nil
}

// RankNative sorts every event by its own span duration unless a request attribute is selected.
// It contains no RPC pairing or business wait/overlap calculation.
func RankNative(capture string, events []NativeEvent, q NativeQuery) (NativeReport, error) {
	data, err := prepareNativeCapture(capture, events)
	if err != nil {
		return NativeReport{}, err
	}
	return data.rank(q)
}

func (data *nativeCaptureData) rank(q NativeQuery) (NativeReport, error) {
	r := data.summary
	r.Sort, r.Attribute = "duration", q.Attribute
	r.Rows, r.Events = []NativeEvent{}, []NativeEvent{}
	if q.Attribute != "" {
		r.Sort = "attribute"
	}
	if q.Minimum == "" {
		q.Minimum = "0"
	}
	minimum, err := NativeNumber(q.Minimum)
	if err != nil || minimum.Sign() < 0 {
		return r, errors.New("invalid minimum value")
	}
	if q.Limit < 1 || q.Limit > 100 || (q.Partial != "" && q.Partial != "0" && q.Partial != "1") || q.Start < 0 || q.End < 0 || (q.End != 0 && q.Start > q.End) {
		return r, errors.New("invalid native query: limit, partial or time range")
	}
	if q.Attribute != "" && len(r.Units) > 0 && r.Units[q.Attribute] == "" {
		return r, fmt.Errorf("unknown span attribute %q", q.Attribute)
	}
	events := data.events
	values := map[string]*big.Rat{}
	for _, e := range events {
		if e.Name == "raft.capture" || (q.Name != "" && e.Name != q.Name) || (q.Node != "" && e.Fields["node_id"] != q.Node) || (q.Group != "" && e.Fields["group_id"] != q.Group) || (q.Outcome != "" && e.Fields["outcome"] != q.Outcome) {
			continue
		}
		start, err := strconv.ParseInt(e.Start, 10, 64)
		if err != nil {
			return r, errors.New("invalid native start")
		}
		if (q.Start != 0 && start < q.Start) || (q.End != 0 && start > q.End) {
			continue
		}
		if q.Partial != "" && e.Values["raft.fetch.partial"] != q.Partial {
			continue
		}
		value := e.Duration
		if q.Attribute != "" {
			var ok bool
			value, ok = e.Values[q.Attribute]
			if !ok || e.States[q.Attribute] != "Observed" {
				if _, applicable := e.States[q.Attribute]; applicable {
					r.Missing++
				}
				continue
			}
		}
		n, err := NativeNumber(value)
		if err != nil {
			return r, fmt.Errorf("invalid saved value for %s", q.Attribute)
		}
		if n.Cmp(minimum) < 0 {
			continue
		}
		r.Rows = append(r.Rows, e)
		values[e.TraceID+":"+e.SpanID] = n
	}
	sort.Slice(r.Rows, func(i, j int) bool {
		a := values[r.Rows[i].TraceID+":"+r.Rows[i].SpanID]
		b := values[r.Rows[j].TraceID+":"+r.Rows[j].SpanID]
		if order := a.Cmp(b); order != 0 {
			return order > 0
		}
		return r.Rows[i].TraceID+":"+r.Rows[i].SpanID < r.Rows[j].TraceID+":"+r.Rows[j].SpanID
	})
	r.Total = len(r.Rows)
	if len(r.Rows) > q.Limit {
		r.Rows = r.Rows[:q.Limit]
	}
	selected := q.Selected
	if selected == "" && len(r.Rows) > 0 {
		selected = r.Rows[0].TraceID + ":" + r.Rows[0].SpanID
	}
	for i := range events {
		if events[i].TraceID+":"+events[i].SpanID == selected {
			event := events[i]
			r.Selected = &event
			break
		}
	}
	if selected != "" && r.Selected == nil {
		return r, errors.New("selected event is absent from capture")
	}
	if r.Selected != nil {
		s := *r.Selected
		if q.Attribute == "" {
			for _, e := range events {
				if e.Name != "raft.capture" && nativeEventEvidence(s, e) {
					r.Events = append(r.Events, e)
				}
			}
			sortNativeEvents(r.Events)
			return r, nil
		}
		ioIDs := map[string]bool{}
		reportRequests := map[string]bool{}
		sequence, sequenceErr := strconv.ParseUint(s.Fields["sequence"], 10, 64)
		last, lastErr := strconv.ParseInt(s.Fields["last_offset"], 10, 64)
		if s.Name == "raft.fetch" && sequenceErr == nil && lastErr == nil {
			for _, candidate := range events {
				offset, err := strconv.ParseInt(candidate.Fields["requested_offset"], 10, 64)
				if candidate.Name == "raft.fetch" && candidate.TraceID == s.TraceID && candidate.Fields["epoch"] == s.Fields["epoch"] && candidate.Fields["peer_id"] == s.Fields["peer_id"] && candidate.Fields["sequence"] == strconv.FormatUint(sequence+1, 10) && err == nil && offset > last {
					reportRequests[candidate.Fields["event_id"]] = true
				}
			}
		}
		ticketIDs := map[string]bool{}
		for _, id := range strings.Split(s.Fields["ticket_ids"], ",") {
			ticketIDs[id] = id != ""
		}
		for _, e := range events {
			if e.TraceID == s.TraceID && e.Fields["request_id"] == s.Fields["event_id"] {
				if id := e.Fields["io_id"]; id != "" {
					ioIDs[id] = true
				}
			}
		}
		for _, e := range events {
			if e.Name == "raft.capture" {
				continue
			}
			if (e.TraceID == s.TraceID && (e.SpanID == s.SpanID || e.Fields["request_id"] == s.Fields["event_id"] || (e.Name == "raft.folly.fsync" && ioIDs[e.Fields["event_id"]]) || (e.Name == "raft.set.ticket.retired" && ticketIDs[e.Fields["ticket_id"]]) || (e.Name == "raft.fetch.report" && reportRequests[e.Fields["request_id"]]))) || nativeLeaderEvidence(s, e) {
				r.Events = append(r.Events, e)
			}
		}
		sortNativeEvents(r.Events)
	}
	return r, nil
}

// Options cover the entire capture, independently of ranking filters and limits.
// Role labels are query metadata, not changes to the recorded event attributes.
func nativeOptions(events []NativeEvent) ([]NativeNode, []string) {
	type nodeKey struct{ group, node string }
	type epochKey struct{ group, node, epoch string }
	type clockKey struct{ process, group, node string }
	type roleKey struct{ epoch, role string }
	type roleRange struct {
		roleKey
		Interval
	}
	roles := map[nodeKey]map[string]bool{}
	epochRoles := map[epochKey]map[string]bool{}
	ranges := map[clockKey][]roleRange{}
	requests := map[string]NativeEvent{}
	nodes := map[string]map[string]bool{}
	names := map[string]bool{}
	clock := func(e NativeEvent) clockKey {
		process := e.Fields["process_instance"]
		if process == "" {
			process = e.TraceID
		}
		return clockKey{process, e.Fields["group_id"], e.Fields["node_id"]}
	}
	observe := func(e NativeEvent, role string) {
		start, a := strconv.ParseInt(e.Start, 10, 64)
		end, b := strconv.ParseInt(e.End, 10, 64)
		if role == "" || a != nil || b != nil || end < start {
			return
		}
		key := clock(e)
		identity := roleKey{e.Fields["epoch"], role}
		ranges[key] = append(ranges[key], roleRange{identity, Interval{start, end}})
	}
	add := func(group, node, epoch, role string) {
		if group == "" || node == "" || role == "" || role == "Unknown" {
			return
		}
		key := nodeKey{group, node}
		if roles[key] == nil {
			roles[key] = map[string]bool{}
		}
		roles[key][role] = true
		if epoch != "" {
			key := epochKey{group, node, epoch}
			if epochRoles[key] == nil {
				epochRoles[key] = map[string]bool{}
			}
			epochRoles[key][role] = true
		}
	}
	for _, e := range events {
		f := e.Fields
		if node := f["node_id"]; node != "" && nodes[node] == nil {
			nodes[node] = map[string]bool{}
		}
		if e.Name == "raft.capture" {
			continue
		}
		names[e.Name] = true
		add(f["group_id"], f["node_id"], f["epoch"], f["node_role"])
		observe(e, f["node_role"])
		requests[e.TraceID+":"+f["event_id"]] = e

		// A successful Fetch identifies its follower and leader in this group/epoch.
		// The fsync `role` field describes an I/O file, not a Raft node role.
		if e.Name == "raft.fetch" && (f["outcome"] == "Data" || f["outcome"] == "Empty") && f["epoch"] != "" && f["peer_id"] != "" && f["peer_id"] != f["node_id"] {
			add(f["group_id"], f["node_id"], f["epoch"], "Follower")
			add(f["group_id"], f["peer_id"], f["epoch"], "Leader")
			observe(e, "Follower")
		}
	}
	for key, observations := range ranges {
		sort.SliceStable(observations, func(i, j int) bool {
			if observations[i].Begin != observations[j].Begin {
				return observations[i].Begin < observations[j].Begin
			}
			return observations[i].End < observations[j].End
		})
		merged := []roleRange{}
		for _, next := range observations {
			if len(merged) > 0 && merged[len(merged)-1].roleKey == next.roleKey {
				last := &merged[len(merged)-1]
				last.End = max(last.End, next.End)
			} else {
				merged = append(merged, next)
			}
		}
		ranges[key] = merged
	}
	for i := range events {
		e := &events[i]
		f := e.Fields
		epoch := f["epoch"]
		if epoch == "" && f["request_id"] != "" && f["request_id"] != "0" {
			epoch = requests[e.TraceID+":"+f["request_id"]].Fields["epoch"]
		}
		observed := epochRoles[epochKey{f["group_id"], f["node_id"], epoch}]
		if epoch == "" {
			// Older events may lack a role/epoch. Require a containing role interval in this
			// process, bracketed by recorded evidence, with no conflicting interval. Never
			// label a single event with every role the node held during the whole capture.
			observed = map[string]bool{}
			start, a := strconv.ParseInt(e.Start, 10, 64)
			end, b := strconv.ParseInt(e.End, 10, 64)
			if a == nil && b == nil && end >= start {
				for _, bounds := range ranges[clock(*e)] {
					if bounds.role == "Unknown" || start < bounds.Begin || end > bounds.End {
						continue
					}
					conflict := false
					for _, interval := range ranges[clock(*e)] {
						if interval.role != bounds.role && interval.Begin <= end && interval.End >= start {
							conflict = true
						}
					}
					if !conflict {
						observed[bounds.role] = true
					}
				}
			}
		}
		if role := f["node_role"]; role != "" {
			observed = map[string]bool{role: true}
		}
		e.NodeRoles = []string{}
		if len(observed) == 1 {
			e.NodeRoles = sortedNativeRoles(observed)
		}
	}
	for key, observed := range roles {
		if nodes[key.node] != nil {
			for role := range observed {
				nodes[key.node][role] = true
			}
		}
	}
	options := make([]NativeNode, 0, len(nodes))
	for id, observed := range nodes {
		options = append(options, NativeNode{ID: id, Roles: sortedNativeRoles(observed)})
	}
	sort.Slice(options, func(i, j int) bool {
		a, aErr := strconv.ParseInt(options[i].ID, 10, 64)
		b, bErr := strconv.ParseInt(options[j].ID, 10, 64)
		if aErr == nil && bErr == nil && a != b {
			return a < b
		}
		return options[i].ID < options[j].ID
	})
	eventNames := make([]string, 0, len(names))
	for name := range names {
		eventNames = append(eventNames, name)
	}
	sort.Strings(eventNames)
	return options, eventNames
}

func sortedNativeRoles(observed map[string]bool) []string {
	roles := make([]string, 0, len(observed))
	for role := range observed {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	return roles
}

func sortNativeEvents(events []NativeEvent) {
	sort.Slice(events, func(i, j int) bool {
		a, _ := strconv.ParseInt(events[i].Start, 10, 64)
		b, _ := strconv.ParseInt(events[j].Start, 10, 64)
		if a != b {
			return a < b
		}
		return events[i].TraceID+":"+events[i].SpanID < events[j].TraceID+":"+events[j].SpanID
	})
}

// Event Slow Path uses recorded relationships, never an event-name allowlist or time neighbours.
func nativeEventEvidence(selected, event NativeEvent) bool {
	if selected.TraceID != event.TraceID {
		return false
	}
	if selected.SpanID == event.SpanID {
		return true
	}
	valid := func(id string) bool { return id != "" && id != "0" }
	parent := selected.Fields["request_id"]
	if !valid(parent) {
		parent = selected.Fields["event_id"]
	}
	if valid(parent) && (event.Fields["event_id"] == parent || event.Fields["request_id"] == parent) {
		return true
	}
	for _, key := range []string{"request_id", "io_id", "flush_id"} {
		if id := selected.Fields[key]; valid(id) && event.Fields["event_id"] == id {
			return true
		}
		if id := event.Fields[key]; valid(id) && id == selected.Fields["event_id"] {
			return true
		}
	}
	for _, key := range []string{"flush_id", "ticket_id"} {
		if id := selected.Fields[key]; valid(id) && event.Fields[key] == id {
			return true
		}
	}
	for _, id := range strings.Split(selected.Fields["ticket_ids"], ",") {
		if valid(id) && event.Fields["ticket_id"] == id {
			return true
		}
	}
	return false
}

func compactStrings(values []string) []string {
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

// Cross-stream evidence uses group, epoch, peer and offset coverage, never a time neighbour.
func nativeLeaderEvidence(request, event NativeEvent) bool {
	if request.Name == "raft.set.admission.wait" {
		return event.Name == "raft.wal.append.complete" && request.Fields["process_instance"] == event.Fields["process_instance"] && request.Fields["group_id"] == event.Fields["group_id"] && request.Fields["last_wal_completion_ns"] == event.Start
	}

	if request.Name != "raft.fetch" || request.Fields["group_id"] != event.Fields["group_id"] || request.Fields["epoch"] == "" || request.Fields["epoch"] != event.Fields["epoch"] || request.Fields["peer_id"] != event.Fields["node_id"] {
		return false
	}
	first, a := strconv.ParseInt(request.Fields["first_offset"], 10, 64)
	last, b := strconv.ParseInt(request.Fields["last_offset"], 10, 64)
	previous, c := strconv.ParseInt(event.Fields["previous_offset"], 10, 64)
	end, d := strconv.ParseInt(event.Fields["end_offset"], 10, 64)
	if a != nil || b != nil || c != nil || d != nil {
		return false
	}
	switch event.Name {
	case "raft.wal.append.complete":
		return previous <= last && end > first
	case "raft.hw.advance", "raft.reply.advance":
		return previous <= last && end > last
	}
	return false
}
