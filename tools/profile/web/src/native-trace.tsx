import { useEffect, useMemo, useState } from 'react';
import { api, endpoint } from './model';

type NativeEvent = { trace_id: string; span_id: string; name: string; start_ns: string; end_ns: string; duration_ms: string; node_roles: string[]; fields: Record<string, string>; values: Record<string, string>; states: Record<string, string> };
type Capture = { id: string; release: string; binary_sha: string; workload: string; streams: number };
type NodeOption = { id: string; roles: string[] };
type Report = { nodes: NodeOption[]; event_names: string[]; complete: boolean; issues: string[]; total: number; missing: number; sort: 'duration' | 'attribute'; attribute?: string; units: Record<string, string>; rows: NativeEvent[]; selected?: NativeEvent; events: NativeEvent[] };
const nodeLabel = (id: string, roles: string[]) => `${roles.length ? roles.join(' / ') : 'Unknown'} ${id}`;
const eventNode = (event: NativeEvent) => nodeLabel(event.fields.node_id, event.node_roles);
const keyOf = (e: NativeEvent) => `${e.trace_id}:${e.span_id}`;
const ms = (value?: string) => value === undefined ? '—' : `${Number(value).toFixed(3)} ms`;
const ns = (value: string) => ms(String(Number(value) / 1e6));
const duration = (e: NativeEvent) => BigInt(e.end_ns) - BigInt(e.start_ns);
const compare = (a: bigint, b: bigint) => a < b ? -1 : a > b ? 1 : 0;
const requestAttributes = ['raft.fetch.return_wait', 'raft.fetch', 'raft.fetch.rpc', 'raft.fetch.await_flush', 'raft.await_io', 'raft.fetch.fsync_overlap'];
const valueText = (e: NativeEvent, name: string, units: Report['units']) => e.states[name] !== 'Observed' ? (e.states[name] || 'Missing') : units[name] === 'ms' ? ms(e.values[name]) : `${e.values[name]} ${units[name] ?? ''}`;

export function NativeTraceView({ id }: { id: string }) {
  const [captures, setCaptures] = useState<Capture[]>();
  const [capture, setCapture] = useState('');
  const [mode, setMode] = useState<'requests' | 'events'>('requests');
  const [attribute, setAttribute] = useState('raft.fetch.return_wait');
  const [node, setNode] = useState('');
  const [partial, setPartial] = useState('');
  const [minimum, setMinimum] = useState('10');
  const [name, setName] = useState('');
  const [start, setStart] = useState('');
  const [end, setEnd] = useState('');
  const [selected, setSelected] = useState('');
  const [report, setReport] = useState<Report>();
  const [options, setOptions] = useState<{ capture: string; nodes: NodeOption[]; names: string[] }>();
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    const abort = new AbortController();
    setCaptures(undefined); setCapture(''); setOptions(undefined); setError('');
    api<{ captures: Capture[] }>(`${endpoint(id)}/captures`, { signal: abort.signal })
      .then(data => { if (!abort.signal.aborted) { setCaptures(data.captures ?? []); setCapture(data.captures?.[0]?.id ?? ''); } })
      .catch(e => { if (!abort.signal.aborted) setError(e.message); });
    return () => abort.abort();
  }, [id, revision]);
  useEffect(() => {
    if (!capture) return;
    const abort = new AbortController();
    setBusy(true); setError('');
    const query = new URLSearchParams({ capture, attribute: mode === 'requests' ? attribute : '', node, partial: mode === 'requests' ? partial : '', name: mode === 'events' ? name : '', min: minimum || '0', start_ns: start, end_ns: end, selected, limit: '10' });
    api<Report>(`${endpoint(id)}/${mode}?${query}`, { signal: abort.signal })
      .then(data => { if (!abort.signal.aborted) { setReport(data); setOptions({ capture, nodes: data.nodes, names: data.event_names }); } })
      .catch(e => { if (!abort.signal.aborted) { setError(e.message); setReport(undefined); } })
      .finally(() => { if (!abort.signal.aborted) setBusy(false); });
    return () => abort.abort();
  }, [id, capture, mode, attribute, node, partial, minimum, name, start, end, selected]);
  const change = (callback: () => void) => { setSelected(''); setReport(undefined); callback(); };
  const filters = options?.capture === capture ? options : undefined;
  const current = captures?.find(c => c.id === capture);
  const attributes = useMemo(() => [...new Set([attribute, ...requestAttributes, ...Object.keys(report?.units ?? {})])], [attribute, report?.units]);
  return <div className="native-analysis">
    <div className="section-toolbar"><div><h2>Stage analysis</h2><p>Rank slow events or inspect Fetch response delays, then open the timeline.</p></div><button className="button" onClick={() => setRevision(n => n + 1)}>Refresh captures</button></div>
    <div className="segments" role="tablist" aria-label="Analysis type">
      <button role="tab" aria-selected={mode === 'events'} aria-pressed={mode === 'events'} onClick={() => mode !== 'events' && change(() => { setMode('events'); setMinimum('0'); })}>Event Slow Path</button>
      <button role="tab" aria-selected={mode === 'requests'} aria-pressed={mode === 'requests'} onClick={() => mode !== 'requests' && change(() => { setMode('requests'); setAttribute('raft.fetch.return_wait'); setMinimum('10'); })}>Fetch delay</button>
    </div>
    <div className="native-captures segments" role="tablist" aria-label="Capture rounds">{captures?.map(c => <button key={c.id} role="tab" aria-selected={capture === c.id} aria-pressed={capture === c.id} onClick={() => capture !== c.id && change(() => { setCapture(c.id); setNode(''); setName(''); })}>{c.id}</button>)}</div>
    {current && <p className="native-identity">{current.release} · {current.workload} · {current.streams} streams · <code title={current.binary_sha}>SHA {current.binary_sha.slice(0, 16)}</code></p>}
    {error && <div className="notice" role="alert">{error}</div>}
    {captures?.length === 0 && <div className="panel inline-empty">No native captures have been published for this experiment.</div>}
    {!captures && !error && <div role="status">Loading captures…</div>}
    {capture && <>
      <div className="native-filters">
        {mode === 'requests' && <label>Sort attribute<select aria-label="Sort attribute" value={attribute} onChange={e => change(() => setAttribute(e.target.value))}>{attributes.filter(m => m.startsWith('raft.fetch') || m === 'raft.await_io').map(m => <option key={m}>{m}</option>)}</select></label>}
        <label>Node<select aria-label="Node" value={node} disabled={!filters} onChange={e => change(() => setNode(e.target.value))}><option value="">All nodes</option>{filters?.nodes.map(n => <option key={n.id} value={n.id}>{nodeLabel(n.id, n.roles)}</option>)}</select></label>
        {mode === 'requests' ? <label>Response<select aria-label="Response" value={partial} onChange={e => change(() => setPartial(e.target.value))}><option value="">All responses</option><option value="1">Partial prefix</option><option value="0">Complete payload</option></select></label> : <label>Event name<select aria-label="Event name" value={name} disabled={!filters} onChange={e => change(() => setName(e.target.value))}><option value="">All events</option>{filters?.names.map(n => <option key={n} value={n}>{n}</option>)}</select></label>}
        <label>{mode === 'events' ? 'Minimum duration (ms)' : `Minimum (${report?.units[attribute] ?? 'ms'})`}<input aria-label="Minimum value" type="number" min="0" step="any" value={minimum} onChange={e => change(() => setMinimum(e.target.value))} /></label>
        <details><summary>Measurement window</summary><label>Start (original ns)<input value={start} onChange={e => change(() => setStart(e.target.value))} /></label><label>End (original ns)<input value={end} onChange={e => change(() => setEnd(e.target.value))} /></label></details>
      </div>
      {busy && <p role="status">Reading complete capture…</p>}
      {report && <>
        <div className={`native-completeness ${report.complete ? 'complete' : 'incomplete'}`}><strong>{report.complete ? 'Complete capture' : 'Incomplete capture'}</strong><span>{report.total} matches · {report.missing} missing values · Top 10 descending</span>{report.issues.map(issue => <p key={issue}>{issue}</p>)}</div>
        <section className="panel data-panel native-ranking"><div className="panel-heading"><h2>{mode === 'requests' ? 'Fetch delay' : 'Slow path'}</h2><span>{mode === 'requests' ? attribute : 'Duration · ms'}</span></div>
          <div className="table-scroll"><table className="data-table"><thead><tr><th>{mode === 'requests' ? 'Fetch' : '#'}</th><th>Node</th><th>{mode === 'requests' ? 'Offsets' : 'Event'}</th><th>{mode === 'requests' ? attribute : 'Duration'} ↓</th>{mode === 'requests' && <><th>RPC</th><th>Return wait</th><th>Gate wait</th><th>I/O wait</th><th>Append / payload</th></>}<th>Outcome</th></tr></thead><tbody>{report.rows.map((row, index) => <tr key={keyOf(row)} className={keyOf(row) === (report.selected && keyOf(report.selected)) ? 'selected' : ''}>
            {mode === 'requests' ? <td><button className="text-link" aria-pressed={keyOf(row) === (report.selected && keyOf(report.selected))} onClick={() => setSelected(keyOf(row))}>#{index + 1} · Fetch {row.fields.sequence}</button></td> : <td><button className="text-link mono" aria-label={`Open slow path ${index + 1}: ${row.name}`} aria-pressed={keyOf(row) === (report.selected && keyOf(report.selected))} onClick={() => setSelected(keyOf(row))}>{index + 1}</button></td>}<td title="Roles evidenced in this capture; events with an epoch use that epoch's role.">{eventNode(row)}</td><td>{mode === 'requests' ? `${row.fields.first_offset ?? row.fields.requested_offset}–${row.fields.last_offset ?? '—'}` : <button className="text-link" aria-pressed={keyOf(row) === (report.selected && keyOf(report.selected))} onClick={() => setSelected(keyOf(row))}>{row.name}</button>}</td><td className="mono strong">{mode === 'requests' ? valueText(row, attribute, report.units) : ms(row.duration_ms)}</td>{mode === 'requests' && <><td>{ms(row.values['raft.fetch.rpc'])}</td><td>{ms(row.values['raft.fetch.return_wait'])}</td><td>{ms(row.values['raft.fetch.await_flush'])}</td><td>{ms(row.values['raft.await_io'])}</td><td>{row.values['raft.fetch.appended_bytes'] ?? '—'} / {row.values['raft.fetch.payload_bytes'] ?? '—'}</td></>}<td>{row.fields.outcome}</td>
          </tr>)}</tbody></table></div>{report.rows.length === 0 && <div className="inline-empty">No matching events.</div>}
        </section>
        {report.selected && <NativeDetail key={keyOf(report.selected)} report={report} mode={mode} />}
      </>}
    </>}
  </div>;
}

function NativeDetail({ report, mode }: { report: Report; mode: 'requests' | 'events' }) {
  const selected = report.selected!;
  const [sort, setSort] = useState<'time' | 'duration'>('time');
  const isFetch = mode === 'requests' && selected.name === 'raft.fetch';
  const streams = [...new Set(report.events.map(e => e.trace_id))];
  const reasons = [...new Set(report.events.filter(e => e.name === 'raft.fetch.flush_gate.blocked' && e.fields.outcome === 'Rejected').map(e => e.fields.reason))];
  const blocks = isFetch ? [
    { title: `Leader ${selected.fields.peer_id}`, events: report.events.filter(e => e.trace_id !== selected.trace_id), note: 'Linked by group, epoch and offset coverage. Node-local time axis.' },
    { title: `Follower ${selected.fields.node_id}`, events: report.events.filter(e => e.trace_id === selected.trace_id && e.name !== 'raft.folly.fsync'), note: 'RPC, return wait, gate and append share this request context.' },
    { title: `I/O · ${eventNode(selected)}`, events: report.events.filter(e => e.trace_id === selected.trace_id && e.name === 'raft.folly.fsync'), note: 'Shared operations linked by explicit io_id. Overlap and wait dependency are distinct.' },
  ] : streams.map(stream => {
    const events = report.events.filter(e => e.trace_id === stream);
    return { title: `${eventNode(selected)} · Group ${events[0].fields.group_id}`, events, note: 'Selected event and directly linked events. Node-local time axis.' };
  });
  return <section className="panel native-detail">
    <div className="panel-heading"><div><h2>{isFetch ? `Fetch ${selected.fields.sequence} · ${eventNode(selected)} · offsets ${selected.fields.first_offset ?? '—'}–${selected.fields.last_offset ?? '—'}` : `${selected.name} · ${eventNode(selected)}`}</h2><p className="mono">{keyOf(selected)}</p></div><label>Event order <select aria-label="Event order" value={sort} onChange={e => setSort(e.target.value as typeof sort)}><option value="time">Start time ↑</option><option value="duration">Duration ↓</option></select></label></div>
    {isFetch && reasons.length > 0 && <div className="native-reason"><strong>Gate rejected: {reasons.join(', ')}</strong><span>Return wait {ms(selected.values['raft.fetch.return_wait'])} · Gate wait {ms(selected.values['raft.fetch.await_flush'])} · I/O wait {ms(selected.values['raft.await_io'])}</span></div>}
    <div className="native-values"><div><small>Duration</small><strong title={`${selected.duration_ms} ms`}>{ms(selected.duration_ms)}</strong></div>{Object.keys(selected.values).map(name => <div key={name}><small>{name}</small><strong title={`${selected.values[name]} ${report.units[name] ?? ''}`}>{valueText(selected, name, report.units)}</strong></div>)}</div>
    {blocks.map(block => <TimelineBlock key={block.events[0]?.trace_id + block.title} {...block} sort={sort} />)}
    <details className="native-fields"><summary>Recorded attributes and boundaries</summary><dl>{Object.entries(selected.fields).map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{value}</dd></div>)}</dl></details>
  </section>;
}

function TimelineBlock({ title, events, note, sort }: { title: string; events: NativeEvent[]; note: string; sort: 'time' | 'duration' }) {
  const ordered = [...events].sort((a, b) => sort === 'time' ? compare(BigInt(a.start_ns), BigInt(b.start_ns)) : compare(duration(b), duration(a)));
  const first = events.reduce((n, e) => BigInt(e.start_ns) < n ? BigInt(e.start_ns) : n, events.length ? BigInt(events[0].start_ns) : 0n);
  const last = events.reduce((n, e) => BigInt(e.end_ns) > n ? BigInt(e.end_ns) : n, first + 1n);
  const extent = Number(last - first);
  return <div className="native-block"><div className="native-block-heading"><h3>{title}</h3><p>{note}</p></div>{events.length ? <>
    <div className="native-axis"><span>0 ms · node-local origin</span><span>{ns((last - first).toString())}</span></div>
    {ordered.map(e => <div className={`native-event ${e.fields.outcome === 'Rejected' || e.name.includes('wait') ? 'wait' : ''}`} key={keyOf(e)}>
      <div className="native-event-label"><strong>{e.name}</strong><small>{e.fields.reason || e.fields.role || e.fields.outcome}{e.fields.io_id && ` · io ${e.fields.io_id}`}{e.fields.flush_id && ` · flush ${e.fields.flush_id}`}{e.fields.ticket_id && ` · ticket ${e.fields.ticket_id}`}{e.fields.end_offset && ` · offset ${e.fields.end_offset}`}</small></div>
      <div className="native-track" title={`Start ${ns((BigInt(e.start_ns) - first).toString())} · Duration ${ns(duration(e).toString())}`}><span style={{ left: `${Number(BigInt(e.start_ns) - first) * 100 / extent}%`, width: `${Math.max(0.35, Number(duration(e)) * 100 / extent)}%` }} /></div><span className="mono">{ns(duration(e).toString())}</span>
    </div>)}
  </> : <p className="muted">No linked events recorded.</p>}</div>;
}
