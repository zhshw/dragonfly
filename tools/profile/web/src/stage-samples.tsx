import { useEffect, useId, useMemo, useRef, useState } from 'react';
import { ArrowDown, ArrowUp, ChevronRight } from 'lucide-react';
import { number } from './model';
import { StagePathChart } from './stage-path-chart';
import { traceSlowPath } from './stage-path';
import { summarizeEventTimes } from './stage-metrics';
import type { StageSample, StageSpan, StageWithSamples } from './stage-metrics';

const milliseconds = (microseconds: number) => (microseconds / 1000).toFixed(3);
const columns = {
  start: { label: 'Start ms', value: ({ span }: StageSample) => span.ts, descending: false },
  window: { label: 'Window ms', value: ({ coveredMS }: StageSample) => coveredMS, descending: true },
  duration: { label: 'Total ms', value: ({ span }: StageSample) => span.dur, descending: true },
  tid: { label: 'TID', value: ({ span }: StageSample) => span.tid, descending: false },
};
type SortKey = keyof typeof columns;
type EventSort = { key: SortKey; descending: boolean };
const sortColumns = Object.keys(columns) as SortKey[];

type SampleRun = { id?: string; stage?: StageWithSamples; events?: StageSpan[]; status?: string };

export function StageSamplesPanel({ id, metric, phase, current, previous }: {
  id: string; metric: string; phase: string; current: SampleRun; previous: SampleRun;
}) {
  const title = useId();
  const panel = useRef<HTMLElement>(null);
  const [source, setSource] = useState<'current' | 'previous'>(current.stage ? 'current' : 'previous');
  const run = source === 'current' ? current : previous;

  useEffect(() => { panel.current?.scrollIntoView({ block: 'start' }); }, []);

  return <section id={id} ref={panel} className="stage-samples-panel" aria-labelledby={title}>
    <header className="panel-heading stage-samples-heading">
      <div className="stage-samples-title">
        <h2 id={title}><code>{metric}</code></h2>
        {run.stage && <span className="count-label">{number(run.stage.count, 0)} 个事件</span>}
      </div>
      <div className="segments" role="group" aria-label="诊断轮次">
        {(['current', 'previous'] as const).map((choice) => {
          const runId = choice === 'current' ? current.id : previous.id;
          return <button type="button" key={choice} aria-pressed={source === choice} onClick={() => setSource(choice)}>
            {choice === 'current' ? '本轮' : '上一轮'}{runId && ` · ${runId}`}
          </button>;
        })}
      </div>
    </header>
    {run.stage && run.events ? <StageSamples key={source} stage={run.stage} phase={phase} events={run.events} /> :
      <div className="inline-empty" role="status">{run.status ?? '该轮暂无此指标的事件'}</div>}
  </section>;
}

function StageSamples({ stage, phase, events }: {
  stage: StageWithSamples; phase: string; events: StageSpan[];
}) {
  const windows = useMemo(() => events.filter(({ cat, name }) => cat === 'client' && name === phase)
    .sort((a, b) => a.ts - b.ts), [events, phase]);
  const [picked, setPicked] = useState<StageSpan | undefined>(() => stage.samples[0]?.span);
  const [sort, setSort] = useState<EventSort>({ key: 'window', descending: true });
  const eventView = useRef<HTMLDivElement>(null);
  const eventPath = useMemo(() => picked && traceSlowPath(events, phase, picked), [events, phase, picked]);
  const stats = useMemo(() => summarizeEventTimes(stage.samples), [stage.samples]);

  useEffect(() => { eventView.current?.scrollTo(0, 0); }, [picked]);

  return <div className="stage-event-layout">
    <div className="stage-event-pane">
      <MetricEvents samples={stage.samples} picked={picked} sort={sort} onSort={setSort} onPick={(span) => {
        const next = picked === span ? undefined : span;
        setPicked(next);
        if (next) eventView.current?.scrollIntoView({ block: 'nearest' });
      }} />
    </div>
    <div className="stage-path-pane">
      {stats && <div className="stage-event-stats">
        <p>Time in window · all events</p>
        <dl aria-label="Time in window statistics">
          <div><dt>Max (ms)</dt><dd>{stats.max.toFixed(3)}</dd></div>
          <div><dt>Min (ms)</dt><dd>{stats.min.toFixed(3)}</dd></div>
          <div><dt>Mean (ms)</dt><dd>{stats.mean.toFixed(3)}</dd></div>
          <div><dt>Variance (ms²)</dt><dd>{stats.variance > 0 && stats.variance < 0.001 ?
            stats.variance.toExponential(2) : stats.variance.toFixed(3)}</dd></div>
        </dl>
      </div>}
      <div className="stage-path-content" ref={eventView} role="region" aria-label="指标事件慢路径"
        tabIndex={eventPath ? 0 : undefined}>
        {eventPath && picked ?
          <StagePathChart key={stage.samples.findIndex(({ span }) => span === picked)} path={eventPath}
            picked={picked} windows={windows} /> :
          <div className="inline-empty">选择一条指标事件查看 Slow Path</div>}
      </div>
    </div>
  </div>;
}

function MetricEvents({ samples, picked, sort, onSort, onPick }: {
  samples: StageSample[]; picked?: StageSpan; sort: EventSort;
  onSort: (sort: EventSort) => void; onPick: (span: StageSpan) => void;
}) {
  const ordered = useMemo(() => {
    const value = columns[sort.key].value;
    const direction = sort.descending ? -1 : 1;
    return samples.map((sample, id) => ({ sample, id })).sort((a, b) => {
      const left = value(a.sample), right = value(b.sample);
      if (left === undefined) return right === undefined ? a.id - b.id : 1;
      if (right === undefined) return -1;
      return direction * (left - right) || a.id - b.id;
    });
  }, [samples, sort]);

  return <div className="table-scroll stage-event-scroll" role="region" aria-label="指标原始事件" tabIndex={0}>
    <table className="data-table">
      <thead><tr>{sortColumns.map((key) =>
        <th key={key} scope="col" aria-sort={sort.key === key ? (sort.descending ? 'descending' : 'ascending') : undefined}>
          <button type="button" className="event-sort-button"
            onClick={() => onSort({ key, descending: sort.key === key ? !sort.descending : columns[key].descending })}>
            {columns[key].label}
            {sort.key === key && (sort.descending ? <ArrowDown size={12} aria-hidden="true" /> : <ArrowUp size={12} aria-hidden="true" />)}
          </button>
        </th>)}</tr></thead>
      <tbody>{ordered.map(({ sample: { span, coveredMS }, id }, index) =>
        <tr key={id} className={picked === span ? 'stage-row-selected' : undefined}>
          <td className="mono"><button type="button" className="text-link stage-row-link mono"
            aria-pressed={picked === span} aria-label={`Show slow path for event ${index + 1}`} onClick={() => onPick(span)}>
            <ChevronRight size={15} />{milliseconds(span.ts)}
          </button></td>
          <td className="mono">{coveredMS.toFixed(3)}</td>
          <td className="mono">{milliseconds(span.dur)}</td>
          <td className="mono">{span.tid ?? '—'}</td>
        </tr>)}</tbody>
    </table>
  </div>;
}
