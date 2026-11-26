import '@fontsource-variable/geist';
import './style.css';
import { createRoot } from 'react-dom/client';
import { useCallback, useEffect, useState } from 'react';
import {
  Activity, ArrowRight, ChartBar, ChartNoAxesCombined, Check, ChevronRight,
  Clipboard, FlaskConical, Flame, GitCommitHorizontal, ListOrdered, Plus, Search, Trash2, Workflow,
} from 'lucide-react';
import { api, decisionNames, endpoint, number, percent, referenceHref, targetTitle } from './model';
import type { Detail, ExperimentLabel, Metrics, Summary, View } from './model';
import { RunsView } from './evidence';
import { DiagnosticsView, ReferenceView } from './diagnostics';
import { StageAnalysis } from './stages';
import { CreateExperiment, TargetDetails } from './create';
import { ExecutionPanel } from './execution';
import { DeleteExperiment } from './delete';
import { ConfigureComparisons } from './comparisons';

const views = [
  { id: 'overview', label: '总览', icon: ChartNoAxesCombined },
  { id: 'runs', label: '运行记录', icon: ListOrdered },
  { id: 'flamegraphs', label: '火焰图', icon: Flame },
  { id: 'stages', label: '阶段分析', icon: ChartBar },
  { id: 'trace', label: 'Trace', icon: Workflow },
] as const;

function route(): { id?: string; view: View; reference?: string; requestAnalysis?: boolean } {
  const [id, tab, section] = location.hash.slice(2).split('/');
  if (id === 'reference' && tab && section === 'diagnostics') {
    try {
      return { reference: decodeURIComponent(tab), view: 'overview' };
    } catch {
      return { view: 'overview' };
    }
  }
  return { id, view: views.find(({ id }) => id === tab)?.id || 'overview',
    requestAnalysis: tab === 'stages' && section === 'requests' };
}

function href(id: string, view: View) {
  return `#/${id}/${view}`;
}

interface ComparisonRow {
  label: string;
  alias?: string;
  style: string;
  metrics?: Metrics;
}

function ComparisonChart({ rows, metric, title, subtitle, unit, scaleStep }: {
  rows: ComparisonRow[];
  metric: keyof Metrics;
  title: string;
  subtitle: string;
  unit: string;
  scaleStep: number;
}) {
  const max = Math.max(...rows.map(({ metrics }) => metrics?.[metric]?.max ?? 0));
  const ceiling = Math.max(scaleStep, Math.ceil(max / scaleStep) * scaleStep);

  return <section className="panel chart-panel" aria-label={title}>
    <div className="panel-heading">
      <div><h2>{title}</h2><p>{subtitle} · 各三轮中位数</p></div>
      <span className="panel-unit">{unit}</span>
    </div>
    <div className="bar-chart">
      {rows.map(({ label, alias, style, metrics }) => {
        const value = metrics?.[metric];
        return value && <div key={label} className="bar-row" title={alias}>
          <div className="bar-label"><span className={`dot ${style}`} />{label}</div>
          <div className="bar-track" aria-hidden="true"><div className={`bar-fill ${style}`} style={{ width: `${100 * value.median / ceiling}%` }} /></div>
          <span className="bar-value">{number(value.median)}</span>
          <span className="bar-range">{number(value.min)}–{number(value.max)}</span>
        </div>;
      })}
    </div>
  </section>;
}

function Overview({ summary, configure }: { summary: Summary; configure: () => void }) {
  const { experiment, variants: builds, diagnostics, comparisons } = summary;
  const previous = comparisons.previous;
  const current = builds.current?.metrics;
  if (!current) return <div className="inline-empty">缺少正式 benchmark 指标</div>;
  const compared = previous?.target.metrics;
  const p95Change = previous?.result?.p95_pct;
  const sameBinary = previous?.target.sha256 === builds.current?.sha256;
  const base = comparisons.cumulative_baseline;
  const reference = comparisons.reference;
  const comparisonRows = [
    { label: '基线', alias: targetTitle(base?.target), style: 'baseline', metrics: base?.target.metrics },
    { label: '上一轮', alias: targetTitle(previous?.target), style: 'prior', metrics: compared },
    { label: '本轮', style: 'current', metrics: current },
    { label: '对比参照', alias: targetTitle(reference?.target), style: 'cluster', metrics: reference?.target.metrics },
  ];
  const capture = diagnostics?.current;
  return <div className="view-content">
    <div className="section-toolbar"><h2>实验对照</h2><button className="button" onClick={configure}>配置对照</button></div>
    {[...new Set([base, previous, reference].flatMap((choice) => choice?.warning ? [choice.warning] : []))]
      .map((warning) => <p className="dialog-help" key={warning}>{warning}</p>)}
    <div className="metrics-grid">
      <section className="metric-card">
        <p>{base?.warning ? '较基准吞吐差值' : '累计吞吐收益'}</p>
        <div className="metric-value cumulative">{base?.result ? percent(base.result.throughput_pct) : '—'}</div>
        <div className="metric-foot" title={targetTitle(base?.target)}>{base ? `${targetTitle(base.target)} · ${number(base.target.metrics.replication_mib_per_s.median)} MiB/s` : '未选基线'}</div>
      </section>
      <section className="metric-card">
        <p>{sameBinary ? '同版本复测差值' : compared ? '吞吐较上一轮' : '本轮吞吐'}</p>
        <div className="metric-value">{previous?.result ? percent(previous.result.throughput_pct) : number(current.replication_mib_per_s.median)}</div>
        <div className="metric-foot"><span>{previous?.target.experiment_id ?? 'SET + WAIT 1'}</span><span>{compared && `${number(compared.replication_mib_per_s.median)} → `}{number(current.replication_mib_per_s.median)} MiB/s</span></div>
      </section>
      <section className="metric-card">
        <p>达到参照吞吐</p>
        <div className="metric-value">{reference?.result ? `${number(reference.result.target_throughput_pct, 1)}%` : '—'}</div>
        <div className="metric-foot" title={targetTitle(reference?.target)}>{reference ? <><span>{targetTitle(reference.target)} · {number(reference.target.metrics.replication_mib_per_s.median)} MiB/s</span><a className="text-link" href={referenceHref(reference.target.id)}>查看基准诊断 <ArrowRight size={13} /></a></> : '未选对比参照'}</div>
      </section>
      <section className="metric-card">
        <p>{compared ? '批次 P95 耗时较上一轮' : '批次 P95 耗时'}</p>
        <div className="metric-value">{p95Change === undefined ? number(current.batch_p95_ms.median) : percent(p95Change)}</div>
        <div className="metric-foot" title="三轮正式测试的批次 P95 中位数">
          <span>{compared && `${number(compared.batch_p95_ms.median)} → `}{number(current.batch_p95_ms.median)} ms</span>
          {!compared && <span>暂无上一轮</span>}
        </div>
      </section>
    </div>
    <div className="overview-grid">
      <ComparisonChart rows={comparisonRows} metric="replication_mib_per_s" title="吞吐"
        subtitle="SET + WAIT 1" unit="MiB/s" scaleStep={100} />
      <ComparisonChart rows={comparisonRows} metric="batch_p95_ms" title="耗时"
        subtitle="批次 P95" unit="ms" scaleStep={10} />
    </div>
    {capture && <section className="panel">
      <div className="panel-heading">
        <div><h2>本轮 · 独立诊断</h2><p>Leader / Follower</p></div>
        <a className="text-link" href={href(experiment.id, 'trace')}>查看 Trace <ArrowRight size={14} /></a>
      </div>
      <div className="diagnostic-values">
        <div><span>CPU 时间</span><strong>{number(capture.nodes.leader.cpu_ms, 0)} / {number(capture.nodes.follower.cpu_ms, 0)} <small>ms</small></strong></div>
        <div><span>峰值 RSS</span><strong>{capture.nodes.leader.peak_rss_mib != null && capture.nodes.follower.peak_rss_mib != null ? <>{number(capture.nodes.leader.peak_rss_mib)} / {number(capture.nodes.follower.peak_rss_mib)} <small>MiB</small></> : '未采集'}</strong></div>
        <div><span>完整事件</span><strong>{number(capture.span_count, 0)} <small>条</small></strong></div>
      </div>
    </section>}
  </div>;
}

function Draft({ summary, refresh }: { summary: Summary; refresh: () => void }) {
  const { cumulative_baseline, reference, previous } = summary.comparisons;
  return <div className="view-content">
    <div className="draft-targets">
      {previous && <section className="panel"><h2>上一轮</h2><TargetDetails target={previous.target} /></section>}
      {cumulative_baseline && <section className="panel"><h2>基线</h2><TargetDetails target={cumulative_baseline.target} /></section>}
      {reference && <section className="panel"><h2>对比参照</h2><TargetDetails target={reference.target} /></section>}
    </div>
    <ExecutionPanel id={summary.experiment.id} refresh={refresh} />
  </div>;
}

function App() {
  const [page, setPage] = useState(route);
  const [experiments, setExperiments] = useState<ExperimentLabel[] | null>(null);
  const [query, setQuery] = useState('');
  const [detail, setDetail] = useState<Detail | null>(null);
  const [listError, setListError] = useState('');
  const [detailError, setDetailError] = useState('');
  const [request, setRequest] = useState(0);
  const [copyState, setCopyState] = useState<'idle' | 'loading' | 'copied'>('idle');
  const [copyError, setCopyError] = useState('');
  const [creating, setCreating] = useState(false);
  const [configuring, setConfiguring] = useState(false);
  const [canDelete, setCanDelete] = useState(false);
  const [deleting, setDeleting] = useState<ExperimentLabel | null>(null);
  const [deleteWarning, setDeleteWarning] = useState('');
  const refresh = useCallback(() => setRequest((n) => n + 1), []);

  useEffect(() => {
    const changed = () => setPage(route());
    addEventListener('hashchange', changed);
    return () => removeEventListener('hashchange', changed);
  }, []);

  useEffect(() => {
    const abort = new AbortController();
    api<{ experiments: ExperimentLabel[]; can_delete?: boolean }>('/api/v1/experiments', { signal: abort.signal })
      .then(({ experiments, can_delete }) => { setListError(''); setExperiments(experiments); setCanDelete(!!can_delete); })
      .catch((error) => { if (!abort.signal.aborted) setListError(error.message); });
    return () => abort.abort();
  }, [request]);

  const selected = page.reference ? undefined : page.id || experiments?.[0]?.id;
  useEffect(() => {
    if (!selected) return;
    const abort = new AbortController();
    setDetailError('');
    setCopyState('idle');
    setCopyError('');
    api<Detail>(endpoint(selected), { signal: abort.signal }).then(setDetail).catch((error) => {
      if (!abort.signal.aborted) setDetailError(error.message);
    });
    return () => abort.abort();
  }, [selected, request]);

  const active = detail?.summary.experiment.id === selected ? detail : null;
  const experiment = active?.summary.experiment;
  const workload = active?.summary.workload;
  const matches = experiments?.filter((item) => `${item.id} ${item.title} ${item.optimization ?? ''}`.toLowerCase().includes(query.trim().toLowerCase()));

  async function copySummary() {
    if (!active) return;
    setCopyState('loading');
    setCopyError('');
    try {
      const summary = await api<Summary>(`${endpoint(active.summary.experiment.id)}/summary`);
      const text = JSON.stringify(summary, null, 2);
      if (navigator.clipboard) {
        await navigator.clipboard.writeText(text);
      } else {
        const field = document.createElement('textarea');
        field.value = text;
        field.className = 'clipboard-field';
        document.body.append(field);
        field.select();
        const copied = document.execCommand('copy');
        field.remove();
        if (!copied) throw new Error('浏览器未允许复制，请打开摘要 JSON。');
      }
      setCopyState('copied');
    } catch (error) {
      setCopyState('idle');
      setCopyError(error instanceof Error ? error.message : '复制未完成');
    }
  }

  return <div className="shell">
    <aside className="sidebar">
      <a className="brand" href="#/" aria-label="profile 首页">
        <span className="brand-icon"><Activity size={22} strokeWidth={2.2} /></span>
        <span>profile</span>
      </a>
      <div className="experiment-nav">
        <div className="nav-heading">实验 <span>{experiments?.length ?? '—'}</span></div>
        <label className="search"><Search size={16} /><input aria-label="搜索实验" placeholder="搜索实验" value={query} onChange={(event) => setQuery(event.target.value)} /></label>
        <nav className="experiment-list" aria-label="实验">
          {matches?.map((item) => <a className={selected === item.id ? 'selected' : ''} href={href(item.id, 'overview')} key={item.id} aria-current={selected === item.id ? 'page' : undefined}>
            <span className="experiment-icon"><FlaskConical size={17} /></span>
            <span className="experiment-link"><strong>{item.id} {item.optimization && <small>· {item.optimization}</small>} {item.state === 'draft' && <small>· 待测试</small>}</strong><span title={item.title}>{item.title}</span></span>
          </a>)}
          {matches?.length === 0 && query && <span className="nav-empty">无匹配实验</span>}
        </nav>
      </div>
      <div className="sidebar-foot"><GitCommitHorizontal size={15} />Dragonfly DB</div>
    </aside>
    <main>
      <header className="topbar">
        <div className="breadcrumbs"><span>性能实验</span>{(selected || page.reference) && <><ChevronRight size={14} /><strong>{page.reference ? '对比参照' : selected}</strong></>}</div>
        <div className="topbar-actions">{active && <button className="button copy-button" onClick={copySummary} disabled={copyState === 'loading'}>
          {copyState === 'copied' ? <Check size={15} /> : <Clipboard size={15} />}
          {copyState === 'copied' ? '已复制' : copyState === 'loading' ? '读取中' : '复制实验摘要'}
        </button>}
          {canDelete && experiment && <button className="button delete-button" aria-label="删除实验" title="删除实验"
            onClick={() => setDeleting(experiment)}><Trash2 size={16} /></button>}
          <button className="button primary" onClick={() => setCreating(true)}><Plus size={16} />创建实验</button>
        </div>
      </header>
      <div className="workspace">
        {deleteWarning && <div role="status" className="notice">{deleteWarning} <button className="text-link" onClick={() => setDeleteWarning('')}>关闭</button></div>}
        {selected && copyError && <div role="alert" className="notice">{copyError} <a href={`${endpoint(selected)}/summary`} target="_blank" rel="noreferrer">打开摘要 JSON</a></div>}
        {listError && <div role="alert" className="notice">{listError} <button className="text-link" onClick={() => setRequest(request + 1)}>重试</button></div>}
        {page.reference && <ReferenceView key={page.reference} id={page.reference} />}
        {!active && !page.reference && <div className="empty-state">
          <FlaskConical size={28} />
          <h1>{detailError ? '无法读取实验' : experiments?.length === 0 ? '还没有实验记录' : '正在读取实验…'}</h1>
          {detailError ? <p>{detailError} <button className="text-link" onClick={() => setRequest(request + 1)}>重试</button></p> :
            experiments?.length === 0 ? <p>用 <code>profile import</code> 导入已有 benchmark 与诊断归档。</p> : null}
        </div>}
        {active && experiment && workload && <>
          <div className="hero">
            <div className="experiment-meta">
              <span className="id-label">{experiment.id}</span>
              {experiment.optimization && <span>{experiment.optimization}</span>}
              {experiment.date && <span>{experiment.date}</span>}
            </div>
            <div className="title-row"><h1>{experiment.title}</h1><span className={`decision ${experiment.decision}`}><span />{experiment.state === 'draft' ? '待测试' : decisionNames[experiment.decision]}</span></div>
            {experiment.description && <p className="description">{experiment.description}</p>}
            <div className="workload-chips">
              <span className="mode-chip"><span className="dot current" />Raft {workload.ack_mode}</span>
              <span>{workload.clients} 连接</span><span>Pipeline {workload.pipeline_depth}</span>
              <span>{workload.payload_mib} MiB</span><span>{workload.value_kib} KiB/value</span>
            </div>
          </div>
          {experiment.state === 'draft' ? <Draft key={experiment.id} summary={active.summary} refresh={refresh} /> : <>
          <nav className="tabs" aria-label="实验视图">
            {views.map(({ id, label, icon: Icon }) => <a key={id} href={href(experiment.id, id)} aria-current={page.view === id ? 'page' : undefined}>
              <Icon size={17} />{label}
            </a>)}
          </nav>
          {page.view === 'overview' && <Overview summary={active.summary} configure={() => setConfiguring(true)} />}
          {page.view === 'runs' && <RunsView key={experiment.id} id={experiment.id} />}
          {page.view === 'stages' && <StageAnalysis key={experiment.id} detail={active} requests={page.requestAnalysis} />}
          {(page.view === 'flamegraphs' || page.view === 'trace') && <DiagnosticsView key={experiment.id} id={experiment.id} view={page.view}
            referenceID={!active.summary.comparisons.previous ? active.summary.comparisons.reference?.target.id : undefined} />}
          </>}
        </>}
      </div>
    </main>
    {creating && <CreateExperiment close={() => setCreating(false)} created={(id) => {
      setCreating(false);
      location.hash = href(id, 'overview');
      setRequest((n) => n + 1);
    }} />}
    {configuring && active && <ConfigureComparisons key={active.summary.experiment.id} summary={active.summary}
      close={() => setConfiguring(false)} saved={(summary) => {
        setConfiguring(false);
        setDetail((detail) => detail?.summary.experiment.id === summary.experiment.id ? { ...detail, summary } : detail);
        refresh();
      }} />}
    {deleting && <DeleteExperiment experiment={deleting} close={() => setDeleting(null)} deleted={(warning) => {
      const id = deleting.id;
      setDeleting(null);
      setDeleteWarning(warning ?? '');
      setExperiments((items) => items?.filter((item) => item.id !== id) ?? null);
      if (selected === id) location.hash = '#/';
      refresh();
    }} />}
  </div>;
}

createRoot(document.getElementById('root')!).render(<App />);
