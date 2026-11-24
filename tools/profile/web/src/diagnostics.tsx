import { Fragment, useEffect, useState } from 'react';
import { ArrowDownToLine, ArrowUpRight, Flame, Search, Workflow } from 'lucide-react';
import { api, artifactURL, endpoint, number, targetTitle } from './model';
import type { DiagnosticCapture, DiagnosticComparison, DiagnosticSummary, ReferenceDiagnostics, Role, Stage } from './model';

type Panel = { key: 'current' | 'previous'; title: string; id?: string; capture?: DiagnosticCapture };

export function DiagnosticsView({ id, view }: { id: string; view: 'flamegraphs' | 'trace' }) {
  const [comparison, setComparison] = useState<DiagnosticComparison>();
  const [role, setRole] = useState<Role>('leader');
  const [request, setRequest] = useState(0);
  const [error, setError] = useState('');
  useEffect(() => {
    const abort = new AbortController();
    api<DiagnosticComparison>(`${endpoint(id)}/diagnostic-comparison`, { signal: abort.signal })
      .then(setComparison).catch((e) => { if (!abort.signal.aborted) setError(e.message); });
    return () => abort.abort();
  }, [id, request]);

  const isCPU = view === 'flamegraphs';
  const panels: Panel[] = [
    { key: 'current', title: '本轮', id, capture: comparison?.current },
    { key: 'previous', title: '上一轮', id: comparison?.previous_id, capture: comparison?.previous },
  ];

  return <div className="view-content">
    <div className="section-toolbar">
      <div><h2>{isCPU ? 'CPU 火焰图' : 'Trace'}</h2><p>{isCPU ? '按节点角色查看。火焰图支持点击缩放和搜索。' : '按节点角色与阶段查看耗时，重叠区间合并统计。'}</p></div>
      {isCPU && <NodeRolePicker role={role} setRole={setRole} />}
    </div>
    {error ? <div role="alert" className="notice">{error} <button className="text-link" onClick={() => { setError(''); setRequest((n) => n + 1); }}>重试</button></div> :
      !comparison ? <div className="panel inline-empty" role="status">正在读取诊断记录…</div> : <>
        <div className="diagnostic-columns">
          {panels.map((panel) => isCPU ? <FlamegraphPanel key={panel.key} panel={panel} role={role} /> : <TracePanel key={panel.key} panel={panel} />)}
        </div>
        {!isCPU && panels.some(({ capture }) => capture) && <TraceComparison panels={panels} />}
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
  const panel: Panel = { key: 'current', title: 'CPU 火焰图', capture };
  const tracePanel = { ...panel, title: 'Trace' };
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
      {!isCluster && <>
        <TracePanel panel={tracePanel} />
        <TraceComparison panels={[tracePanel]} />
      </>}
    </div> : <div className="panel inline-empty">这条基准暂无诊断记录</div>}
  </>;
}

function panelTitle(panel: Panel) {
  const id = panel.capture?.experiment_id ?? panel.id;
  return id ? `${panel.title} · ${id}` : panel.title;
}

function SourceHeading({ panel }: { panel: Panel }) {
  return <h2>{panelTitle(panel)}</h2>;
}

function FlamegraphPanel({ panel, role }: { panel: Panel; role: Role }) {
  const capture = panel.capture;
  const selected = capture?.observation.flamegraphs[role];
  const graph = capture && selected && artifactURL(capture.experiment_id, selected);
  const profile = capture?.profile.nodes[role];
  return <div className="capture-stack">
    <section className="panel graph-panel">
      <div className="panel-heading">
        <SourceHeading panel={panel} />
        {graph && <a className="button" href={graph} target="_blank" rel="noreferrer">打开原图 <ArrowUpRight size={15} /></a>}
      </div>
      {graph ? <iframe key={graph} title={`${panelTitle(panel)} ${role} CPU 火焰图`} src={graph} sandbox="allow-scripts" /> : <div className="inline-empty"><Flame size={22} /><p>{panel.key === 'previous' && !panel.id ? '暂无上一轮火焰图' : '此节点暂无火焰图'}</p></div>}
    </section>
    {profile && <section className="panel data-panel">
      <div className="panel-heading"><div><h2>CPU 热点</h2><p>按叶子函数的样本数排序</p></div><span className="count-label">{number(profile.cpu_ms, 0)} ms CPU</span></div>
      <div className="table-scroll"><table className="data-table"><thead><tr><th>函数</th><th>样本</th><th>占比</th></tr></thead>
        <tbody>{profile.hotspots.map((hotspot) => <tr key={hotspot.function}><td className="mono hotspot" title={hotspot.function}>{hotspot.function}</td><td className="mono">{hotspot.samples}</td><td className="mono">{number(hotspot.percent, 1)}%</td></tr>)}</tbody>
      </table></div>
    </section>}
  </div>;
}

function TracePanel({ panel }: { panel: Panel }) {
  const capture = panel.capture;
  return <section className="panel capture-trace">
    <div className="panel-heading"><SourceHeading panel={panel} /><span className="evidence-icon trace"><Workflow size={17} /></span></div>
    {capture ? <div className="capture-trace-body">
      <div><strong>{number(capture.profile.span_count, 0)}</strong> 条完整事件</div>
      {capture.profile.trace_url && <p>Langfuse 已核验 {number(capture.profile.verified_observations ?? 0, 0)} 条展示样本</p>}
      <div className="trace-actions">
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

function TraceComparison({ panels }: { panels: Panel[] }) {
  const [query, setQuery] = useState('');
  const [role, setRole] = useState('all');
  const [showAll, setShowAll] = useState(false);
  const grouped = new Map<string, { stage: string; role: string; values: (Stage | undefined)[] }>();
  const search = query.trim().toLowerCase();
  panels.forEach(({ capture }, index) => {
    for (const stage of capture?.observation.stages ?? []) {
      if ((role !== 'all' && stage.role !== role) || !stage.stage.toLowerCase().includes(search)) continue;
      const key = `${stage.role}:${stage.stage}`;
      const row = grouped.get(key) ?? { stage: stage.stage, role: stage.role, values: Array<Stage | undefined>(panels.length).fill(undefined) };
      row.values[index] = stage;
      grouped.set(key, row);
    }
  });
  const longest = (row: { values: (Stage | undefined)[] }) => Math.max(...row.values.map((phase) => phase?.union_ms ?? 0));
  const phases = [...grouped.values()].sort((a, b) => longest(b) - longest(a));
  return <>
    <section className="panel data-panel">
      <div className="panel-heading"><h2>Raft 等待指标</h2></div>
      <div className="table-scroll"><table className="data-table diagnostic-table">
        <thead><tr><th>指标</th>{panels.map((panel) => <th key={panel.key}>{panelTitle(panel)}</th>)}</tr></thead>
        <tbody>{waitMetrics.map(({ label, value }) => <tr key={label}><td>{label}</td>{panels.map(({ key, capture }) => <td className="mono" key={key}>{capture ? value(capture.profile) : '—'}</td>)}</tr>)}</tbody>
      </table></div>
    </section>
    <section className="panel data-panel">
      <div className="panel-heading phase-heading">
        <div><h2>阶段耗时</h2><p>{panels.length === 2 && panels.every(({ capture }) => capture) ? '按两次采集中较长的区间并集排序。' : '按区间并集排序。'}</p></div>
        <div className="phase-controls">
          <select aria-label="按节点角色筛选" value={role} onChange={(event) => { setRole(event.target.value); setShowAll(false); }}>
            <option value="all">全部节点</option><option value="leader">Leader</option><option value="follower">Follower</option><option value="client">客户端</option>
          </select>
          <label className="search"><Search size={15} /><input placeholder="筛选阶段" aria-label="筛选阶段" value={query} onChange={(event) => { setQuery(event.target.value); setShowAll(false); }} /></label>
        </div>
      </div>
      <div className="table-scroll"><table className="data-table diagnostic-table">
        <thead>
          <tr><th rowSpan={2}>阶段</th><th rowSpan={2}>节点</th>{panels.map((panel) => <th colSpan={3} scope="colgroup" className="phase-start" key={panel.key}>{panelTitle(panel)}</th>)}</tr>
          <tr>{panels.map(({ key }) => <Fragment key={key}><th className="phase-start">事件数</th><th>并集 ms</th><th>P95 ms</th></Fragment>)}</tr>
        </thead>
        <tbody>{(showAll ? phases : phases.slice(0, 12)).map((phase) => <tr key={`${phase.role}:${phase.stage}`}>
          <td className="mono strong">{phase.stage}</td><td>{phase.role}</td>
          {phase.values.map((value, i) => <Fragment key={panels[i].key}>
            <td className="mono phase-start">{value ? number(value.count, 0) : '—'}</td>
            <td className="mono">{value ? number(value.union_ms, 3) : '—'}</td><td className="mono">{value ? number(value.p95_ms, 3) : '—'}</td>
          </Fragment>)}
        </tr>)}</tbody>
      </table></div>
      {phases.length === 0 && <div className="inline-empty">没有匹配的阶段</div>}
      {phases.length > 12 && <button className="show-more" onClick={() => setShowAll(!showAll)}>{showAll ? '收起' : `显示全部 ${phases.length} 个阶段`}</button>}
    </section>
  </>;
}
