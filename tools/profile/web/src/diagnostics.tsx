import { Fragment, useEffect, useState } from 'react';
import { ArrowDownToLine, ArrowUpRight, Flame, Search, Workflow } from 'lucide-react';
import { api, artifactURL, endpoint, number, targetTitle } from './model';
import type { DiagnosticCapture, DiagnosticComparison, DiagnosticSummary, ReferenceDiagnostics, Role, Stage } from './model';
import { FlamegraphHotspots } from './flamegraph-hotspots';

type Panel = { key: 'current' | 'previous' | 'reference'; title: string; id?: string; capture?: DiagnosticCapture; raft?: boolean };
type EventSummary = {
  capture_id: string; complete: boolean; issues: string[]; total: number; event_names: string[];
  stages: Stage[]; role_stages?: Stage[]; scope: 'measurement' | 'capture';
  protocol_waits_omitted?: number;
};

export function DiagnosticsView({ id, view, referenceID }: { id: string; view: 'flamegraphs' | 'trace'; referenceID?: string }) {
  const [comparison, setComparison] = useState<DiagnosticComparison>();
  const [reference, setReference] = useState<ReferenceDiagnostics>();
  const [role, setRole] = useState<Role>('leader');
  const [flameTab, setFlameTab] = useState<'current' | 'previous'>('current');
  const [compareWith, setCompareWith] = useState<'previous' | 'reference'>('previous');
  const [request, setRequest] = useState(0);
  const [error, setError] = useState('');
  const isCPU = view === 'flamegraphs';
  const traceReferenceID = isCPU ? undefined : referenceID;
  useEffect(() => {
    const abort = new AbortController();
    setComparison(undefined);
    setReference(undefined);
    setError('');
    Promise.all([
      api<DiagnosticComparison>(`${endpoint(id)}/diagnostic-comparison`, { signal: abort.signal }),
      traceReferenceID ? api<ReferenceDiagnostics>(`/api/v1/comparison-targets/${encodeURIComponent(traceReferenceID)}/diagnostics`, { signal: abort.signal }) : Promise.resolve(undefined),
    ]).then(([comparison, reference]) => {
      if (abort.signal.aborted) return;
      setComparison(comparison);
      setReference(reference);
    }).catch((e) => { if (!abort.signal.aborted) setError(e.message); });
    return () => abort.abort();
  }, [id, traceReferenceID, request]);

  const showReference = !isCPU && !!reference && (compareWith === 'reference' || !comparison?.previous_id);
  const panels: Panel[] = [
    { key: 'current', title: '本轮', id, capture: comparison?.current },
    showReference && reference ? {
      key: 'reference', title: reference.target.mode === 'yes' ? '原 Cluster' : '对比参照',
      id: reference.target.id, capture: reference.capture, raft: reference.target.mode === 'raft',
    } : { key: 'previous', title: '上一轮', id: comparison?.previous_id, capture: comparison?.previous },
  ];
  const activeFlame = panels[flameTab === 'previous' && comparison?.previous_id ? 1 : 0];

  return <div className={isCPU ? 'view-content' : 'view-content trace-view'}>
    <div className="section-toolbar">
      <div><h2>{isCPU ? 'CPU 火焰图' : 'Trace'}</h2>{!isCPU && <p>Trace captures and aggregate event durations.</p>}</div>
      {isCPU && <NodeRolePicker role={role} setRole={setRole} />}
    </div>
    {!isCPU && comparison?.previous_id && reference &&
      <div className="section-toolbar">
        <h3>对照</h3>
        <div className="segments" role="group" aria-label="诊断对照">
          <button aria-pressed={!showReference} onClick={() => setCompareWith('previous')}>上一轮 · {comparison.previous_id}</button>
          <button aria-pressed={showReference} onClick={() => setCompareWith('reference')}>对比参照 · {reference.target.id}</button>
        </div>
      </div>}
    {error ? <div role="alert" className="notice">{error} <button className="text-link" onClick={() => { setError(''); setRequest((n) => n + 1); }}>重试</button></div> :
      !comparison ? <div className="panel inline-empty" role="status">正在读取诊断记录…</div> : <>
        {isCPU ? <>
          {comparison.previous_id && <div className="section-toolbar">
            <div className="segments" role="tablist" aria-label="火焰图轮次" onKeyDown={(event) => {
              const key = event.key;
              if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(key)) return;
              event.preventDefault();
              const next = key === 'Home' ? 'current' : key === 'End' || activeFlame.key === 'current' ? 'previous' : 'current';
              setFlameTab(next);
              event.currentTarget.querySelector<HTMLButtonElement>(`#flame-tab-${next}`)?.focus();
            }}>
              {panels.map((panel) => <button key={panel.key} id={`flame-tab-${panel.key}`} role="tab" aria-controls="flame-tab-panel"
                aria-selected={panel.key === activeFlame.key} tabIndex={panel.key === activeFlame.key ? 0 : -1}
                onClick={() => setFlameTab(panel.key === 'previous' ? 'previous' : 'current')}>{panelTitle(panel)}</button>)}
            </div>
          </div>}
          <div id="flame-tab-panel" className="capture-stack" role={comparison.previous_id ? 'tabpanel' : undefined} aria-labelledby={comparison.previous_id ? `flame-tab-${activeFlame.key}` : undefined}>
            <FlamegraphPanel panel={activeFlame} role={role} />
          </div>
        </> : <TraceOverview panels={panels} />}
      </>}
  </div>;
}

function NodeRolePicker({ role, setRole }: { role: Role; setRole: (role: Role) => void }) {
  return <div className="segments" role="group" aria-label="CPU 采样节点">
    <button aria-pressed={role === 'leader'} onClick={() => setRole('leader')}>Leader</button>
    <button aria-pressed={role === 'follower'} onClick={() => setRole('follower')}>Follower</button>
  </div>;
}

export function ReferenceView({ id }: { id: string }) {
  const [reference, setReference] = useState<ReferenceDiagnostics>();
  const [error, setError] = useState('');
  const [role, setRole] = useState<Role>('leader');
  useEffect(() => {
    const abort = new AbortController();
    api<ReferenceDiagnostics>(`/api/v1/comparison-targets/${encodeURIComponent(id)}/diagnostics`, { signal: abort.signal })
      .then(setReference).catch((e) => { if (!abort.signal.aborted) setError(e.message); });
    return () => abort.abort();
  }, [id]);
  if (error) return <div role="alert" className="notice">{error}</div>;
  if (!reference) return <div className="panel inline-empty" role="status">正在读取基准诊断…</div>;
  const { target, capture } = reference;
  const isCluster = target.mode === 'yes';
  const panel: Panel = { key: 'current', title: 'CPU 火焰图', id: target.id, capture };
  const tracePanel = { ...panel, title: 'Trace', raft: !isCluster };
  return <>
    <div className="hero">
      <div className="experiment-meta"><span className="id-label">对比参照</span></div>
      <div className="title-row"><h1>{isCluster ? '原 Cluster 基准' : targetTitle(target)}</h1></div>
      <p className="description">{isCluster && `${targetTitle(target)} · `}SET + WAIT 1 · {number(target.metrics.replication_mib_per_s.median)} MiB/s</p>
      <div className="workload-chips"><span>{target.workload.clients} 连接</span><span>Pipeline {target.workload.pipeline_depth}</span><span>{target.workload.payload_mib} MiB</span></div>
    </div>
    {capture ? <div className="view-content">
      <div className="section-toolbar"><h2>诊断采集</h2><NodeRolePicker role={role} setRole={setRole} /></div>
      <FlamegraphPanel panel={panel} role={role} />
      <TraceOverview panels={[tracePanel]} />
    </div> : <div className="panel inline-empty">这条基准暂无诊断记录</div>}
  </>;
}

function panelTitle(panel: Panel) {
  const id = panel.id ?? panel.capture?.experiment_id;
  return id ? `${panel.title} · ${id}` : panel.title;
}

function SourceHeading({ panel }: { panel: Panel }) {
  return <h2>{panelTitle(panel)}</h2>;
}

function FlamegraphPanel({ panel, role }: { panel: Panel; role: Role }) {
  const capture = panel.capture;
  const selected = capture?.observation.flamegraphs[role];
  const graph = capture && selected && artifactURL(capture.experiment_id, selected);
  return <div className="capture-stack">
    {capture && selected && <FlamegraphHotspots key={graph} url={artifactURL(capture.experiment_id, `${selected}-folded`)} profile={capture.profile.nodes[role]} />}
    <section className="panel graph-panel">
      <div className="panel-heading">
        <SourceHeading panel={panel} />
        {graph && <a className="button" href={graph} target="_blank" rel="noreferrer">打开原图 <ArrowUpRight size={15} /></a>}
      </div>
      {graph ? <iframe key={graph} title={`${panelTitle(panel)} ${role} CPU 火焰图`} src={graph} sandbox="allow-scripts" /> : <div className="inline-empty"><Flame size={22} /><p>{panel.key === 'previous' && !panel.id ? '暂无上一轮火焰图' : '此节点暂无火焰图'}</p></div>}
    </section>
  </div>;
}

function TraceOverview({ panels }: { panels: Panel[] }) {
  const [includeProtocolWaits, setIncludeProtocolWaits] = useState(false);
  const [summaries, setSummaries] = useState<Record<string, EventSummary>>({});
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const sources = JSON.stringify(panels.flatMap(({ key, capture }) => capture?.profile.native
    ? [{ key, id: capture.experiment_id, capture: capture.profile.capture_id }] : []));
  useEffect(() => {
    const abort = new AbortController();
    const captures: { key: string; id: string; capture?: string }[] = JSON.parse(sources);
    setSummaries({}); setError(''); setLoading(captures.length > 0);
    Promise.all(captures.map(async (source) => {
      if (!source.capture) throw new Error(`${source.id}: missing capture ID`);
      const params = new URLSearchParams({ capture: source.capture, include_protocol_waits: includeProtocolWaits ? '1' : '0' });
      const summary = await api<EventSummary>(`${endpoint(source.id)}/event-summary?${params}`, { signal: abort.signal });
      return [source.key, summary] as const;
    })).then((entries) => {
      if (!abort.signal.aborted) setSummaries(Object.fromEntries(entries));
    }).catch((e) => { if (!abort.signal.aborted) setError(e.message); })
      .finally(() => { if (!abort.signal.aborted) setLoading(false); });
    return () => abort.abort();
  }, [sources, includeProtocolWaits]);
  const summaryFor = (panel: Panel) => {
    const summary = summaries[panel.key];
    return summary?.capture_id === panel.capture?.profile.capture_id ? summary : undefined;
  };
  return <>
    <div className="diagnostic-columns">
      {panels.map((panel) => <TracePanel key={panel.key} panel={panel} summary={summaryFor(panel)} />)}
    </div>
    {loading && <div role="status" className="notice">Loading event statistics…</div>}
    {error && <div role="alert" className="notice">Event statistics: {error}</div>}
    {panels.some(({ capture }) => capture) && <TraceComparison panels={panels} summaries={panels.map(summaryFor)}
      includeProtocolWaits={includeProtocolWaits} setIncludeProtocolWaits={setIncludeProtocolWaits} />}
  </>;
}

function TracePanel({ panel, summary }: { panel: Panel; summary?: EventSummary }) {
  const capture = panel.capture;
  return <section className="panel capture-trace">
    <div className="panel-heading"><SourceHeading panel={panel} /><span className="evidence-icon trace"><Workflow size={17} /></span></div>
    {capture ? <div className="capture-trace-body">
      <div><strong>{capture.profile.native && !summary ? '—' : number(summary?.total ?? capture.profile.span_count, 0)}</strong> events{summary && ` · ${summary.event_names.length} event names`}</div>
      <p>{capture.profile.native ? `${summary?.scope === 'capture' ? 'Full capture' : 'Measurement window'} · OTEL` : 'Measurement window · archived trace'}</p>
      {!!summary?.protocol_waits_omitted && <p>{number(summary.protocol_waits_omitted, 0)} protocol waits omitted</p>}
      {summary && !summary.complete && <p role="alert">Incomplete capture: {summary.issues.join('; ')}</p>}
      {capture.profile.trace_url && <p>Langfuse 已核验 {number(capture.profile.verified_observations ?? 0, 0)} 条展示样本</p>}
      <div className="trace-actions">
        {capture.profile.capture_id && <a className="button primary" href={`#/${capture.experiment_id}/slow`}>Open slow analysis <ArrowUpRight size={15} /></a>}
        {capture.observation.timeline && <a className="button" href={artifactURL(capture.experiment_id, capture.observation.timeline)} download={`${capture.experiment_id}-${capture.observation.variant}-timeline.json`}><ArrowDownToLine size={15} />时间线 JSON</a>}
        {capture.profile.trace_url && <a className="button primary" href={capture.profile.trace_url} target="_blank" rel="noreferrer">打开 Langfuse <ArrowUpRight size={15} /></a>}
      </div>
    </div> : <div className="inline-empty"><p>暂无{panel.title} Trace</p></div>}
  </section>;
}

const waitMetrics: { label: string; value: (diagnostic: DiagnosticSummary) => string }[] = [
  { label: '两节点 fsync 并集 (ms)', value: (trace) => number(trace.fsync_union_ms) },
  { label: 'Follower fsync ∩ await_io (ms)', value: (trace) => number(trace.nodes.follower.fsync_await_ms) },
  { label: 'Leader worker 排队并集 (ms)', value: (trace) => number(trace.nodes.leader.worker_queue_ms) },
  { label: 'fsync 中启动编码 / 追加 (次)', value: (trace) => `${trace.nodes.leader.starts_during_fsync} / ${trace.nodes.follower.starts_during_fsync}` },
];

type StageMetrics = { count: number; union_ms: number; p95_ms?: number };
type PhaseRow = { key: string; stage: string; role: string; values: (StageMetrics | undefined)[] };

function TraceComparison({ panels, summaries, includeProtocolWaits, setIncludeProtocolWaits }: {
  panels: Panel[];
  summaries: (EventSummary | undefined)[];
  includeProtocolWaits: boolean;
  setIncludeProtocolWaits: (include: boolean) => void;
}) {
  const [query, setQuery] = useState('');
  const [role, setRole] = useState('all');
  const [showAll, setShowAll] = useState(false);
  const raftPanels = panels.filter((panel) => panel.raft !== false && !panel.capture?.profile.native);
  const grouped = new Map<string, PhaseRow>();
  const search = query.trim().toLowerCase();
  panels.forEach(({ capture }, index) => {
    const summary = summaries[index];
    const nodeRoles = new Map(Object.entries(capture?.profile.nodes ?? {}).map(([role, info]) => [info.node, role]));
    for (const stage of summary?.role_stages ?? summary?.stages ?? capture?.observation.stages ?? []) {
      const nodeRole = nodeRoles.get(stage.node) ?? stage.role;
      const key = `${stage.stage}\0${nodeRole}`;
      const row = grouped.get(key) ?? {
        key, stage: stage.stage, role: nodeRole, values: Array<StageMetrics | undefined>(panels.length).fill(undefined),
      };
      const existing = row.values[index];
      // Older archives may contain only per-node percentiles.
      row.values[index] = existing ? { count: existing.count + stage.count, union_ms: existing.union_ms + stage.union_ms } : stage;
      grouped.set(key, row);
    }
  });
  const allPhases = [...grouped.values()];
  const roles = [...new Set(allPhases.map((phase) => phase.role))].sort();
  const longest = (row: PhaseRow) => Math.max(...row.values.map((phase) => phase?.union_ms ?? 0));
  const canCompare = panels.length === 2 && panels.every(({ capture }) => capture);
  const phases = allPhases.filter((phase) => (role === 'all' || phase.role.split(' / ').includes(role) || phase.role === role) && phase.stage.toLowerCase().includes(search))
    .sort((a, b) => longest(b) - longest(a));
  return <>
    {raftPanels.length > 0 && <section className="panel data-panel">
      <div className="panel-heading"><h2>Raft 等待指标</h2></div>
      <div className="table-scroll"><table className="data-table diagnostic-table">
        <thead><tr><th>指标</th>{raftPanels.map((panel) => <th key={panel.key}>{panelTitle(panel)}</th>)}</tr></thead>
        <tbody>{waitMetrics.map(({ label, value }) => <tr key={label}><td>{label}</td>{raftPanels.map(({ key, capture }) => <td className="mono" key={key}>{capture ? value(capture.profile) : '—'}</td>)}</tr>)}</tbody>
      </table></div>
    </section>}
    <section className="panel data-panel">
      <div className="panel-heading phase-heading">
        <div><h2>Event statistics</h2><p>Grouped by event and node role. {canCompare ? 'Sorted by the larger duration union. ' : 'Sorted by duration union. '}—: unavailable.</p></div>
        <div className="phase-controls">
          {panels.some(({ capture }) => capture?.profile.native) && <label className="protocol-waits"><input type="checkbox" checked={includeProtocolWaits} onChange={(event) => { setIncludeProtocolWaits(event.target.checked); setShowAll(false); }} />Include protocol waits</label>}
          <select aria-label="Node role" value={role} onChange={(event) => { setRole(event.target.value); setShowAll(false); }}>
            <option value="all">All roles</option>{roles.map((value) => <option key={value} value={value}>{value}</option>)}
          </select>
          <label className="search"><Search size={15} /><input placeholder="Filter events" aria-label="Filter events" value={query} onChange={(event) => { setQuery(event.target.value); setShowAll(false); }} /></label>
        </div>
      </div>
      <div className="table-scroll"><table className="data-table diagnostic-table">
        <thead>
          <tr><th rowSpan={2}>Event</th><th rowSpan={2}>Node role</th>{panels.map((panel) => <th colSpan={3} scope="colgroup" className="phase-start" key={panel.key}>{panelTitle(panel)}</th>)}</tr>
          <tr>{panels.map(({ key }) => <Fragment key={key}><th className="phase-start">Events</th><th>Union ms</th><th>P95 ms</th></Fragment>)}</tr>
        </thead>
        <tbody>{(showAll ? phases : phases.slice(0, 12)).map((phase) => <tr key={phase.key}>
          <td className="mono strong">{phase.stage}</td><td>{phase.role}</td>
          {phase.values.map((value, i) => <Fragment key={panels[i].key}>
            <td className="mono phase-start">{value ? number(value.count, 0) : '—'}</td>
            <td className="mono">{value ? number(value.union_ms, 3) : '—'}</td><td className="mono">{value?.p95_ms === undefined ? '—' : number(value.p95_ms, 3)}</td>
          </Fragment>)}
        </tr>)}</tbody>
      </table></div>
      {phases.length === 0 && <div className="inline-empty">No matching events</div>}
      {phases.length > 12 && <button className="show-more" onClick={() => setShowAll(!showAll)}>{showAll ? 'Show less' : `Show all ${phases.length} event groups`}</button>}
    </section>
  </>;
}
