import { useEffect, useRef, useState } from 'react';
import type { MouseEvent } from 'react';
import { api, number } from './model';
import type { StageSpan } from './stage-metrics';

export type TimelinePhase = { key: string; role: string; stage: string };

type CaptureSource = { title: string; url?: string };
export type Span = StageSpan;
type Archive = { end: number; tracks: Map<string, Span[]> };
type ArchiveState = { url: string; data?: Archive; error?: string };
type TimeWindow = { start: number; length: number };

const focusStages = [
  'client.pipeline', 'client.final_wait', 'raft.log.append', 'raft.folly.fsync',
  'raft.fetch.rpc', 'raft.worker.queue', 'raft.apply', 'raft.set.await_apply',
  'cluster.journal.enqueue', 'journal.apply',
];
const roleLabels: Record<string, string> = { leader: 'Leader', follower: 'Follower', client: '客户端' };
const colors = ['#5559e1', '#3f8c90'];

function readArchive(events: Span[]): Archive {
  if (!Array.isArray(events)) throw new Error('时间线格式无效');
  const tracks = new Map<string, Span[]>();
  let end = 0;
  for (const span of events) {
    if (span.ph !== 'X' || !Number.isFinite(span.ts + span.dur) || span.dur < 0) continue;
    const key = `${span.cat}:${span.name}`;
    const track = tracks.get(key);
    if (track) track.push(span);
    else tracks.set(key, [span]);
    end = Math.max(end, span.ts + span.dur);
  }
  for (const spans of tracks.values()) spans.sort((a, b) => a.ts - b.ts);
  return { end, tracks };
}

export function useArchive(url?: string) {
  const [state, setState] = useState<ArchiveState>();
  const [request, setRequest] = useState(0);
  useEffect(() => {
    if (!url) return;
    const abort = new AbortController();
    api<{ traceEvents: Span[] }>(url, { signal: abort.signal }).then(({ traceEvents }) => {
      if (!abort.signal.aborted) setState({ url, data: readArchive(traceEvents) });
    }).catch((error) => {
      if (!abort.signal.aborted) setState({ url, error: error.message });
    });
    return () => abort.abort();
  }, [url, request]);
  return {
    data: state?.url === url ? state?.data : undefined,
    error: state?.url === url ? state?.error : undefined,
    retry: () => { setState(undefined); setRequest((n) => n + 1); },
  };
}

const milliseconds = (microseconds: number) => number(microseconds / 1000, 3);

export function TimelineTrack({ spans, color, range, label, picked, onPick }: {
  spans: Span[];
  color: string;
  range: TimeWindow;
  label: string;
  picked?: Span;
  onPick: (span: Span) => void;
}) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const { start, length } = range;
  useEffect(() => {
    const plot = canvas.current;
    const context = plot?.getContext('2d');
    if (!plot || !context) return;
    function draw() {
      if (!plot || !context) return;
      const width = plot.clientWidth;
      const height = plot.clientHeight;
      const ratio = window.devicePixelRatio || 1;
      plot.width = Math.round(width * ratio);
      plot.height = Math.round(height * ratio);
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
      const scale = width / length;
      const end = start + length;
      context.fillStyle = color;
      context.globalAlpha = 0.68;
      for (const span of spans) {
        if (span.ts > end) break;
        if (span.ts + span.dur < start) continue;
        const left = Math.max(0, (span.ts - start) * scale);
        const right = Math.min(width, (span.ts + span.dur - start) * scale);
        context.fillRect(left, 3, Math.max(right - left, 1), height - 6);
      }
      if (picked) {
        context.globalAlpha = 1;
        context.fillStyle = '#1a2234';
        context.fillRect((picked.ts - start) * scale, 1, Math.max(picked.dur * scale, 2), 2);
      }
    }
    draw();
    const observer = new ResizeObserver(draw);
    observer.observe(plot);
    return () => observer.disconnect();
  }, [spans, start, length, color, picked]);

  function select(event: MouseEvent<HTMLButtonElement>) {
    const bounds = event.currentTarget.getBoundingClientRect();
    const time = start + (event.detail === 0 ? 0.5 : (event.clientX - bounds.left) / bounds.width) * length;
    const tolerance = 2 * length / bounds.width;
    let nearest: Span | undefined;
    let distance = event.detail === 0 ? Infinity : tolerance;
    for (const span of spans) {
      if (span.ts > start + length) break;
      if (span.ts + span.dur < start) continue;
      const gap = Math.max(span.ts - time, time - span.ts - span.dur, 0);
      if (gap < distance || (gap === distance && (!nearest || span.dur < nearest.dur))) {
        nearest = span;
        distance = gap;
      }
    }
    if (nearest) onPick(nearest);
  }

  return spans.length ? <button type="button" className="timeline-track" aria-label={label} onClick={select}>
    <canvas ref={canvas} aria-hidden="true" />
  </button> : <div className="timeline-track" aria-label={`${label}无事件`} />;
}

const emptyTrack: Span[] = [];

export function TraceTimeline({ phases, phase, setPhase, sources }: {
  phases: TimelinePhase[];
  phase: string;
  setPhase: (phase: string) => void;
  sources: CaptureSource[];
}) {
  const archives = [useArchive(sources[0]?.url), useArchive(sources[1]?.url)];
  const [viewport, setViewport] = useState<TimeWindow>();
  const [picked, setPicked] = useState<{ span: Span; source: number }>();
  const end = Math.max(...archives.map(({ data }) => data?.end ?? 0));
  const length = Math.min(viewport?.length ?? end, end);
  const start = Math.min(viewport?.start ?? 0, end - length);
  const range = { start, length };
  const minimumLength = Math.min(50, end);
  const byName = phases.slice().sort((a, b) => a.stage.localeCompare(b.stage) || a.role.localeCompare(b.role));
  const focused = focusStages.flatMap((stage) => byName.filter((item) => item.stage === stage));
  const visible = phase ? phases.filter(({ key }) => key === phase) : focused.length ? focused : byName.slice(0, 8);
  const selected = picked && visible.some(({ key }) => key === `${picked.span.cat}:${picked.span.name}`) ? picked : undefined;

  function zoom(factor: number) {
    const nextLength = Math.max(minimumLength, Math.min(end, length * factor));
    const center = selected && selected.span.ts >= start && selected.span.ts <= start + length ? selected.span.ts : start + length / 2;
    setViewport({ length: nextLength, start: Math.max(0, Math.min(end - nextLength, center - nextLength / 2)) });
  }

  return <section className="panel timeline-panel" aria-label="Trace 时间线">
    <header className="panel-heading phase-heading">
      <div><h2>时间线</h2><p>每次采集从各自的测量起点计时，共用毫秒刻度。点击事件查看详情。</p></div>
      <div className="phase-controls"><select aria-label="时间线阶段" value={phase} onChange={(event) => setPhase(event.target.value)}>
        <option value="">关键阶段</option>
        {byName.map(({ key, role, stage }) => <option key={key} value={key}>{stage} · {roleLabels[role] ?? role}</option>)}
      </select></div>
    </header>
    <div className="timeline-body">
      <div className="timeline-sources">
        {sources.map(({ title, url }, i) => {
          const { data, error, retry } = archives[i];
          return <div key={title}><span className="dot" style={{ backgroundColor: colors[i] }} /><strong>{title}</strong>
            {!url ? <span>暂无时间线</span> : error ? <span role="alert">{error} <button className="text-link" onClick={retry}>重试</button></span> :
              !data ? <span role="status">正在读取时间线…</span> : data.end > 0 ? <span>{milliseconds(data.end)} ms</span> : <span>暂无事件</span>}
          </div>;
        })}
      </div>
      {end > 0 && visible.length > 0 && <>
        <div className="timeline-controls">
          <div className="segments" role="group" aria-label="时间线缩放">
            <button onClick={() => zoom(0.5)} disabled={length <= minimumLength}>放大</button>
            <button onClick={() => zoom(2)} disabled={length >= end}>缩小</button>
            <button onClick={() => setViewport(undefined)} disabled={length >= end}>全程</button>
          </div>
          {length < end && <input type="range" aria-label="移动时间窗口" min={0} max={end - length} step="any" value={start}
            onChange={(event) => setViewport({ start: Number(event.target.value), length })} />}
        </div>
        <div className="timeline-grid">
          <div className="timeline-axis" aria-label="时间刻度（ms）">
            {[0, .25, .5, .75, 1].map((fraction) => <span key={fraction}>{milliseconds(start + length * fraction)}{fraction === 1 ? ' ms' : ''}</span>)}
          </div>
          {visible.map(({ key, role, stage }) => <div className="timeline-lane" key={key}>
            <div className="timeline-label" title={`${roleLabels[role] ?? role} · ${stage}`}><strong>{stage}</strong><span>{roleLabels[role] ?? role}</span></div>
            <div className="timeline-lane-tracks">{sources.map(({ title }, index) => <TimelineTrack key={title}
              spans={archives[index].data?.tracks.get(key) ?? emptyTrack} color={colors[index]} range={range}
              label={`${title} ${stage} ${roleLabels[role] ?? role} 选择事件`}
              picked={selected?.source === index && `${selected.span.cat}:${selected.span.name}` === key ? selected.span : undefined}
              onPick={(span) => setPicked({ span, source: index })} />)}</div>
          </div>)}
        </div>
      </>}
      {selected && <div className="timeline-detail" role="status">
        <strong>{selected.span.name}</strong><span>{sources[selected.source].title} · {roleLabels[selected.span.cat] ?? selected.span.cat}</span>
        <span>节点 {selected.span.pid}{selected.span.tid === undefined ? '' : ` / TID ${selected.span.tid}`}</span>
        <span>{milliseconds(selected.span.ts)}–{milliseconds(selected.span.ts + selected.span.dur)} ms</span>
        <span>耗时 {milliseconds(selected.span.dur)} ms</span>
        {Object.entries(selected.span.args ?? {}).map(([key, value]) => Number.isSafeInteger(value) && <span key={key}>{key} = {number(value, 0)}</span>)}
      </div>}
    </div>
  </section>;
}
