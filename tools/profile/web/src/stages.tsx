import { Fragment, useEffect, useId, useState } from 'react';
import type { KeyboardEvent } from 'react';
import { ChevronRight, Search } from 'lucide-react';
import { api, artifactURL, number } from './model';
import type { Detail, ReferenceDiagnostics, Stage, Workload } from './model';
import { compareStageNodes, measureServerStages } from './stage-metrics';
import type { StageSpan } from './stage-metrics';
import { StageSamplesPanel } from './stage-samples';
import { RequestAnalysis } from './request-analysis';

const roleLabels: Record<string, string> = { client: '客户端', leader: 'Leader', follower: 'Follower' };
const ms = (value?: number) => value === undefined ? '—' : value.toFixed(3);

function signedMS(value: number) {
  const rounded = Number(value.toFixed(3));
  return `${rounded > 0 ? '+' : rounded < 0 ? '−' : ''}${ms(Math.abs(rounded))}`;
}

function RunMetricHead({ label, currentId, previousId, currentNode, previousNode, delta = false }: {
  label: string; currentId: string; previousId?: string; currentNode?: string; previousNode?: string; delta?: boolean;
}) {
  const runs = [{ title: `本轮 · ${currentId}`, node: currentNode }, {
    title: previousId ? `上一轮 · ${previousId}` : '上一轮', node: previousNode,
  }];
  return <>
    <colgroup span={1} /><colgroup span={3} /><colgroup span={3} />
    {delta && <colgroup span={1} />}
    <thead>
      <tr><th scope="col" rowSpan={2}>{label}</th>
        {runs.map(({ title, node }) => <th className="phase-start" colSpan={3} scope="colgroup" key={title}>
          {title}{node && <small className="stage-header-node">{node}</small>}
        </th>)}
        {delta && <th scope="col" rowSpan={2} className="phase-start">本轮 − 上一轮 ms</th>}
      </tr>
      <tr>{runs.map(({ title }, i) => <Fragment key={title}>
        <th scope="col" className="phase-start" aria-sort={i === 0 ? 'descending' : undefined}>并集 ms</th>
        <th scope="col">P95 ms</th>
        <th scope="col" className="stage-event-count">事件数</th>
      </Fragment>)}</tr>
    </thead>
  </>;
}

function RunMetricCells({ value, windowMS, places = 1 }: {
  value?: Partial<Pick<Stage, 'count' | 'union_ms' | 'p95_ms'>>;
  windowMS?: number;
  places?: number;
}) {
  const union = value?.union_ms;
  return <>
    <td className="mono phase-start">{ms(union)}
      {union !== undefined && !!windowMS &&
        <small className="stage-metric-share" title="占所选窗口">{number(100 * union / windowMS, places)}%</small>}
    </td>
    <td className="mono">{ms(value?.p95_ms)}</td>
    <td className="mono stage-event-count">{value?.count === undefined ? '—' : number(value.count, 0)}</td>
  </>;
}

function workloadMatches(a: Workload, b: Workload) {
  return a.clients === b.clients && a.pipeline_depth === b.pipeline_depth &&
    a.payload_mib === b.payload_mib && a.value_kib === b.value_kib &&
    a.warmup_writes === b.warmup_writes && a.settle_seconds === b.settle_seconds;
}

export function StageAnalysis({ detail, requests = false }: { detail: Detail; requests?: boolean }) {
  const { experiment, workload, comparisons } = detail.summary;
  const capture = detail.observations.find(({ variant }) => variant === 'current') ??
    detail.observations.find(({ variant }) => variant === 'candidate');
  const target = comparisons.previous?.target;
  const targetId = target && workloadMatches(workload, target.workload) ? target.id : undefined;
  const [previous, setPrevious] = useState<{ targetId: string; data?: ReferenceDiagnostics; error?: true }>();
  const [phase, setPhase] = useState<Stage>();

  useEffect(() => {
    if (!targetId) return;
    const abort = new AbortController();
    api<ReferenceDiagnostics>(`/api/v1/comparison-targets/${encodeURIComponent(targetId)}/diagnostics`,
      { signal: abort.signal }).then((result) => {
      if (abort.signal.aborted) return;
      setPrevious({ targetId, data: result });
    }).catch(() => { if (!abort.signal.aborted) setPrevious({ targetId, error: true }); });
    return () => abort.abort();
  }, [targetId]);

  const client = capture?.stages.filter(({ role }) => role === 'client')
    .sort((a, b) => b.union_ms - a.union_ms) ?? [];
  const previousResult = previous?.targetId === targetId ? previous : undefined;
  const previousCapture = previousResult?.data?.capture;
  const prior = new Map(previousCapture?.observation.stages
    .filter(({ role }) => role === 'client')
    .map((stage) => [stage.stage, stage]));
  const total = client.reduce((sum, stage) => sum + stage.union_ms, 0);
  const samePhases = client.length === prior.size && client.every((stage) => prior.has(stage.stage));
  const priorTotal = samePhases ? [...prior.values()].reduce((sum, stage) => sum + stage.union_ms, 0) : undefined;
  const timeline = capture?.timeline && artifactURL(experiment.id, capture.timeline);
  const previousTimeline = previousCapture?.observation.timeline &&
    artifactURL(previousCapture.experiment_id, previousCapture.observation.timeline);
  const stageHref = `#/${encodeURIComponent(experiment.id)}/stages`;
  const requestSources = [capture, ...detail.observations.filter((item) => item !== capture)].flatMap((item) =>
    item?.timeline ? [{
      title: item === capture ? `本轮 · ${experiment.id}` : item.variant === 'baseline' ? '同机对照' : item.variant,
      url: artifactURL(experiment.id, item.timeline),
    }] : []);
  if (previousCapture && previousTimeline && !requestSources.some(({ url }) => url === previousTimeline)) {
    requestSources.push({ title: `上一轮 · ${previousCapture.experiment_id}`, url: previousTimeline });
  }

  return <div className="view-content trace-view">
    <div className="section-toolbar">
      <h2>阶段分析</h2>
      <div className="segments" role="group" aria-label="阶段分析视图">
        <button type="button" aria-pressed={!requests} onClick={() => { location.hash = stageHref; }}>指标分析</button>
        <button type="button" aria-pressed={requests} onClick={() => { location.hash = stageHref + '/requests'; }}>请求分析</button>
      </div>
    </div>
    {requests ? <RequestAnalysis sources={requestSources} /> : <>
    <section className="panel data-panel" aria-label="整轮客户端阶段">
      <div className="panel-heading"><div><h2>整轮客户端阶段</h2><p>按本轮覆盖时间排序。选择阶段后查看该阶段内的服务端执行。</p></div>
        {previousResult?.error && <span role="status">上一轮读取失败</span>}
      </div>
      {client.length ?
        <div className="table-scroll"><table className="data-table diagnostic-table stage-overview">
          <RunMetricHead label="阶段" currentId={experiment.id} previousId={previousCapture?.experiment_id} delta />
          <tbody>{client.map((stage) => {
            const other = prior.get(stage.stage);
            const selected = phase?.stage === stage.stage;
            return <tr key={stage.stage} className={selected ? 'stage-row-selected' : undefined}>
              <td><button className="text-link stage-row-link mono" aria-expanded={selected} disabled={!timeline}
                onClick={() => setPhase(selected ? undefined : stage)}>
                <ChevronRight size={15} />{stage.stage}
              </button></td>
              <RunMetricCells value={stage} windowMS={total} places={2} />
              <RunMetricCells value={other} windowMS={priorTotal} places={2} />
              <td className="mono phase-start">{other === undefined ? '—' : signedMS(stage.union_ms - other.union_ms)}</td>
            </tr>;
          })}</tbody>
          <tfoot><tr>
            <th scope="row">已观测阶段合计</th>
            <RunMetricCells value={{ union_ms: total }} windowMS={total} />
            <RunMetricCells value={{ union_ms: priorTotal }} windowMS={priorTotal} />
            <td className="mono phase-start">{priorTotal === undefined ? '—' : signedMS(total - priorTotal)}</td>
          </tr></tfoot>
        </table></div>
        : <div className="inline-empty">本轮暂无客户端阶段观测</div>}
    </section>
    {phase && timeline && <ServerStages key={phase.stage} phase={phase.stage} url={timeline}
      currentId={experiment.id} previousURL={previousTimeline} previousId={previousCapture?.experiment_id} />}
    </>}
  </div>;
}

type WindowMetrics = ReturnType<typeof measureServerStages>;

function useWindowMetrics(url: string | undefined, phase: string) {
  const [result, setResult] = useState<{ url: string; phase: string; data?: WindowMetrics; error?: string }>();
  useEffect(() => {
    if (!url) return;
    const abort = new AbortController();
    api<{ traceEvents: StageSpan[] }>(url, { signal: abort.signal }).then(({ traceEvents }) => {
      if (!abort.signal.aborted) setResult({ url, phase, data: measureServerStages(traceEvents, phase) });
    }).catch((e) => { if (!abort.signal.aborted) setResult({ url, phase, error: e.message }); });
    return () => abort.abort();
  }, [phase, url]);
  return result?.url === url && result?.phase === phase ? result : undefined;
}

function ServerStages({ phase, url, currentId, previousURL, previousId }: {
  phase: string; url: string; currentId: string; previousURL?: string; previousId?: string;
}) {
  const current = useWindowMetrics(url, phase);
  const previous = useWindowMetrics(previousURL, phase);
  const data = current?.data;
  const queryId = useId();
  const [query, setQuery] = useState('');
  const [showAll, setShowAll] = useState(false);
  const [nodeId, setNodeId] = useState('');
  const [metric, setMetric] = useState<string>();
  const nodes = compareStageNodes(data?.stages ?? [], previous?.data?.stages ?? []);
  const node = nodes.find(({ id }) => id === nodeId) ?? nodes[0];
  const rowsFor = (capture: WindowMetrics | undefined, selected: string | undefined) =>
    new Map(capture?.stages.filter((stage) => stage.role === node?.role && stage.node === selected)
      .map((stage) => [stage.stage, stage]));
  const currentRows = rowsFor(data, node?.current);
  const priorRows = rowsFor(previous?.data, node?.previous);
  const search = query.trim().toLowerCase();
  const matches = [...new Set([...currentRows.keys(), ...priorRows.keys()])]
    .filter((name) => name.toLowerCase().includes(search))
    .map((name) => ({ name, current: currentRows.get(name), previous: priorRows.get(name) }))
    .sort((a, b) => (b.current?.union_ms ?? b.previous?.union_ms ?? 0) - (a.current?.union_ms ?? a.previous?.union_ms ?? 0));
  const row = matches.find(({ name }) => name === metric);

  function selectNode(id: string) {
    setNodeId(id);
    setMetric(undefined);
    setShowAll(false);
  }

  function moveTab(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    let next: number;
    switch (event.key) {
      case 'ArrowLeft': next = (index + nodes.length - 1) % nodes.length; break;
      case 'ArrowRight': next = (index + 1) % nodes.length; break;
      case 'Home': next = 0; break;
      case 'End': next = nodes.length - 1; break;
      default: return;
    }
    event.preventDefault();
    selectNode(nodes[next].id);
    event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>('button')[next]?.focus();
  }

  return <section className="panel data-panel" aria-label={`${phase} 明细`}>
    <div className="panel-heading phase-heading">
      {nodes.length > 0 && <div className="segments" role="tablist" aria-label="节点">
        {nodes.map((tab, index) => <button type="button" role="tab" key={tab.id}
          id={`${queryId}-${tab.id}`} aria-selected={node?.id === tab.id}
          aria-controls={`${queryId}-metrics`} tabIndex={node?.id === tab.id ? 0 : -1}
          onKeyDown={(event) => moveTab(event, index)} onClick={() => selectNode(tab.id)}>
          {roleLabels[tab.role]} {tab.current ?? tab.previous}
        </button>)}
      </div>}
      {data && <div className="phase-controls">
        <label className="search"><Search size={15} />
          <input aria-label="筛选指标名" placeholder="筛选指标名" value={query}
            onChange={(event) => { setQuery(event.target.value); setShowAll(false); setMetric(undefined); }} />
        </label>
      </div>}
    </div>
    {current?.error ? <div className="inline-empty" role="alert">{current.error}</div> : !data ?
      <div className="inline-empty" role="status">正在计算指标覆盖时间…</div> :
      <div id={`${queryId}-metrics`} role={node ? 'tabpanel' : undefined} aria-labelledby={node ? `${queryId}-${node.id}` : undefined}>
      {previous?.error ? <p className="stage-comparison-source" role="status">上一轮时间线读取失败</p> :
        previousURL && !previous && <p className="stage-comparison-source" role="status">正在读取上一轮时间线…</p>}
      {matches.length === 0 ? <div className="inline-empty">{query ? '没有匹配的指标' : '该窗口暂无观测事件'}</div> : <>
        <div className="table-scroll"><table className="data-table diagnostic-table">
          <RunMetricHead label="指标名" currentId={currentId} previousId={previousId}
            currentNode={node?.current && `${roleLabels[node.role]} ${node.current}`}
            previousNode={node?.previous && `${roleLabels[node.role]} ${node.previous}`} />
          <tbody>{(showAll ? matches : matches.slice(0, 12)).map((row) => <tr key={row.name}
            className={metric === row.name ? 'stage-row-selected' : undefined}>
            <td><button type="button" className="text-link stage-row-link mono" aria-expanded={metric === row.name}
              aria-controls={metric === row.name ? `${queryId}-samples` : undefined}
              onClick={() => setMetric(metric === row.name ? undefined : row.name)}><ChevronRight size={15} />{row.name}</button></td>
            <RunMetricCells value={row.current} windowMS={data.windowMS} />
            <RunMetricCells value={row.previous} windowMS={previous?.data?.windowMS} />
          </tr>)}</tbody>
        </table></div>
        {matches.length > 12 && <button className="show-more" onClick={() => setShowAll(!showAll)}>
          {showAll ? '收起' : `显示全部 ${matches.length} 个指标`}
        </button>}
        {row && <StageSamplesPanel key={row.name} id={`${queryId}-samples`} metric={row.name} phase={phase}
          current={{ id: currentId, stage: row.current, events: data.events }}
          previous={{ id: previousId, stage: row.previous, events: previous?.data?.events,
            status: previous?.error ? '上一轮时间线读取失败' : previousURL && !previous ? '正在读取上一轮时间线…' : undefined }} />}
      </>}
      </div>}
  </section>;
}
