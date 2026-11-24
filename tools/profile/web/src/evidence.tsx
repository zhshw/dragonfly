import { useEffect, useState } from 'react';
import { Check, FileText } from 'lucide-react';
import { api, artifactURL, endpoint, number, percent } from './model';
import type { Run, RunComparison, RunSource, Workload } from './model';

function RunLogLink({ id, run }: { id?: string; run?: Run }) {
  if (!id || !run?.log) return null;
  return <a className="icon-link" href={artifactURL(id, run.log)} aria-label={`${id} ${run.trial ? `第 ${run.trial} 轮` : '诊断'}日志`} target="_blank" rel="noreferrer"><FileText size={17} /></a>;
}

function RunTable({ id, runs }: { id: string; runs: Run[] }) {
  return <div className="table-scroll"><table className="data-table">
    <thead><tr><th>顺序</th><th>轮次</th><th>吞吐 MiB/s</th><th>SET MiB/s</th><th>WAIT ms</th><th>批次 P95 ms</th><th>恢复 ms</th><th>验证</th><th>日志</th></tr></thead>
    <tbody>{runs.map((run) => <tr key={run.id}>
      <td className="mono">{run.order ? String(run.order).padStart(2, '0') : '—'}</td>
      <td>{run.trial ? `第 ${run.trial} 轮` : '独立诊断'}</td>
      <td className="mono strong">{number(run.replication_mib_per_s)}</td><td className="mono">{number(run.set_mib_per_s)}</td>
      <td className="mono">{number(run.wait_tail_ms)}</td><td className="mono">{number(run.batch_p95_ms)}</td><td className="mono">{number(run.recovery_ms)}</td>
      <td><span className="pass"><Check size={13} />通过</span></td>
      <td>{run.log ? <RunLogLink id={id} run={run} /> : '—'}</td>
    </tr>)}</tbody>
  </table></div>;
}

const groups = [ ['baseline', 'baseline'], ['previous', '上一轮'], ['current', '本轮'] ] as const;

const metrics = [
  ['replication_mib_per_s', '吞吐 MiB/s'],
  ['set_mib_per_s', 'SET MiB/s'],
  ['wait_tail_ms', 'WAIT ms'],
  ['batch_p95_ms', '批次 P95 ms'],
  ['recovery_ms', '恢复 ms'],
] as const;

type Metric = typeof metrics[number][0];

function mean(runs: Run[] | undefined, metric: Metric) {
  if (runs?.length !== 3 || runs.some((run) => run.status !== 'passed' || !Number.isFinite(run[metric]))) return undefined;
  return runs.reduce((sum, run) => sum + run[metric], 0) / runs.length;
}

function sourceLabel(label: string, source?: RunSource) {
  return source?.experiment_id ? `${label} · ${source.experiment_id}` : label;
}

function primarySet(source?: RunSource) {
  return source?.benchmarks?.find((set) => set.id === 'main');
}

function sameWorkload(previous?: Workload, current?: Workload) {
  return !!previous && !!current && previous.clients === current.clients && previous.pipeline_depth === current.pipeline_depth
    && previous.payload_mib === current.payload_mib && previous.value_kib === current.value_kib && previous.ack_mode === current.ack_mode
    && previous.warmup_writes === current.warmup_writes && previous.settle_seconds === current.settle_seconds;
}

function MeanDiff({ id, comparison }: { id: string; comparison: RunComparison }) {
  const primary = groups.map(([key, label]) => {
    const source = comparison[key];
    return { key, title: sourceLabel(label, source), sourceID: source?.experiment_id, set: primarySet(source) };
  });
  const previous = primary[1].set?.workload;
  const current = primary[2].set?.workload;
  const comparable = sameWorkload(previous, current);
  return <section className="panel data-panel">
    <div className="panel-heading">
      <div><h2>均值 Diff</h2><p>{current && `${current.clients} 连接 / Pipeline ${current.pipeline_depth} · `}各组取三轮算术平均值。差值为本轮减上一轮。</p></div>
    </div>
    <div className="table-scroll"><table className="data-table run-means">
      <thead><tr><th scope="col">指标</th>{primary.map(({ key, title }) => <th scope="col" key={key}>{title}</th>)}<th scope="col">较上一轮差值</th></tr></thead>
      <tbody>{metrics.map(([metric, label]) => {
        const values = primary.map(({ set }) => mean(set?.runs, metric));
        const [, previousMean, currentMean] = values;
        const diff = comparable && previousMean !== undefined && currentMean !== undefined ? currentMean - previousMean : undefined;
        const relative = diff !== undefined && previousMean !== undefined && previousMean > 0 ? 100 * diff / previousMean : undefined;
        return <tr key={metric}><th scope="row">{label}</th>
          {values.map((value, i) => <td key={primary[i].key} className="mono">{value === undefined ? '—' : number(value)}</td>)}
          <td className="mono mean-delta">{diff === undefined ? '—' : `${diff > 0 ? '+' : diff < 0 ? '−' : ''}${number(Math.abs(diff))}`}{relative !== undefined && <small>{percent(relative)}</small>}</td>
        </tr>;
      })}</tbody>
      <tfoot><tr><th scope="row">日志</th>{primary.map(({ key, title, sourceID, set }) => {
        const hasLogs = sourceID && set?.runs.length === 3 && set.runs.every((run) => run.log);
        return <td key={key}>{hasLogs ? <a className="text-link" href={`${endpoint(id)}/run-logs?group=${key}`} aria-label={`${title}三轮日志`} target="_blank" rel="noreferrer"><FileText size={14} />三轮日志</a> : '—'}</td>;
      })}<td aria-hidden="true" /></tr></tfoot>
    </table></div>
  </section>;
}

export function RunsView({ id }: { id: string }) {
  const [comparison, setComparison] = useState<RunComparison>();
  const [error, setError] = useState('');
  useEffect(() => {
    const abort = new AbortController();
    api<RunComparison>(`${endpoint(id)}/run-comparison`, { signal: abort.signal })
      .then(setComparison).catch((e) => { if (!abort.signal.aborted) setError(e.message); });
    return () => abort.abort();
  }, [id]);
  return <div className="view-content">
    <div className="section-toolbar">
      <h2>运行记录</h2>
    </div>
    {error ? <div role="alert" className="notice">{error}</div> : !comparison ?
      <div className="panel inline-empty" role="status">正在读取运行记录…</div> :
      <>
        <MeanDiff id={id} comparison={comparison} />
        <VerificationCard source={comparison.current} />
      </>}
  </div>;
}

function VerificationCard({ source }: { source?: RunSource }) {
  const id = source?.experiment_id;
  const validation = Object.entries(source?.validation ?? {});
  const diagnostics = source?.diagnostics ?? [];
  return <section className="panel data-panel" aria-label="验证与诊断">
    <header className="panel-heading"><div><h2>验证与诊断</h2><p>{sourceLabel('本轮', source)}</p></div></header>
    <div className="check-group">
      <div className="panel-heading"><h3>正确性验证</h3></div>
      {id && validation.length ? <div className="table-scroll"><table className="data-table">
        <thead><tr><th>构建</th><th>单测</th><th>集群回归</th><th>结果</th><th>日志</th></tr></thead>
        <tbody>{validation.map(([build, counts]) => <tr key={build}>
          <td>{({ dbg: 'Debug', opt: 'Release', sanitize: 'ASAN + UBSAN' } as Record<string, string>)[build] || build}</td>
          <td className="mono">{counts.unit_tests || '—'}</td><td className="mono">{counts.integration_tests || '—'}</td>
          <td><span className="pass"><Check size={13} />通过</span></td>
          <td><details className="check-logs"><summary>各测试套件</summary>
            {source?.validation_checks?.filter((check) => check.build === build).map((check) => <a key={check.log} target="_blank" rel="noreferrer" href={artifactURL(id, check.log)}>{check.suite} · {check.tests}</a>)}
          </details></td>
        </tr>)}</tbody>
      </table></div> : <div className="inline-empty">暂无本轮验证记录</div>}
    </div>
    <div className="check-group">
      <div className="panel-heading"><div><h3>独立诊断</h3><p>插桩采集 · CPU 与 Trace</p></div>{diagnostics.length > 0 && <span className="count-label">{diagnostics.length} 次</span>}</div>
      {id && diagnostics.length ? <RunTable id={id} runs={diagnostics} /> : <div className="inline-empty">暂无本轮诊断记录</div>}
    </div>
  </section>;
}
