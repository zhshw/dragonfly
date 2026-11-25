import { useEffect, useState } from 'react';
import { api, number } from './model';
import { EventTimeline } from './event-timeline';
import type { TimelineBand } from './event-timeline';
import type { Span } from './timeline';

type Phase = { phase: string; ms: number; percent: number };
type ReplyGap = { offset: number; begin_ns: number; end_ns: number; phases: Phase[] };
type Fetch = {
  first_offset: number; last_offset: number; rpc_return_ns: number; receive_start_ns: number;
  rpc_to_receive_ms: number; await_io_overlaps_fsync: boolean; rpc: Span; receive: Span;
  await_io: Span[] | null; fsync: Span[] | null;
};
type RequestPath = { phases: Phase[]; reply_gaps: ReplyGap[] | null; fetches?: Fetch[] };
const phaseNames: Record<string, string> = {
  'send-to-group': '客户端发送 → GroupLoop 取走',
  'group-to-wal': 'GroupLoop 取走 → Leader WAL 写完',
  'wal-to-follower-append': 'Leader WAL 写完 → Follower 追加完成',
  'follower-append-to-hw': 'Follower 追加完成 → Leader HW 推进',
  'hw-to-reply': 'Leader HW 推进 → 回复水位推进',
  'group-to-reply': 'GroupLoop 取走 → 回复水位推进',
  'reply-to-client': '回复水位推进 → 客户端收齐回复',
};
const phaseName = (name: string) => phaseNames[name] ?? name;
const ms = (value: number) => number(value, 3);
const gapMS = (gap: ReplyGap) => (gap.end_ns - gap.begin_ns) / 1e6;

export function useRequestPath(url: string | undefined) {
  const [loaded, setLoaded] = useState<{ url: string; path?: RequestPath; error?: string }>();

  useEffect(() => {
    if (!url) return;
    const abort = new AbortController();
    api<RequestPath>(url, { signal: abort.signal }).then((path) => {
      if (!abort.signal.aborted) setLoaded({ url, path });
    }).catch((error: Error) => {
      if (!abort.signal.aborted) setLoaded({ url, error: error.message });
    });
    return () => abort.abort();
  }, [url]);

  const result = loaded?.url === url ? loaded : undefined;
  return { path: result?.path, error: result?.error, requested: !!url };
}

export function RequestSlowPath({ anchor, events, batch, analysis }: {
  anchor: Span; events: Span[]; batch: number; analysis: ReturnType<typeof useRequestPath>;
}) {
  const [filter, setFilter] = useState('');
  const [offset, setOffset] = useState<number>();
  const { path, error, requested } = analysis;
  const gaps = path?.reply_gaps ?? [];
  const gap = gaps.find((item) => item.offset === offset) ?? gaps[0];
  const fetch = gap && path?.fetches?.find((item) => item.first_offset <= gap.offset && gap.offset <= item.last_offset);
  const bands: TimelineBand[] = [];
  if (gap) bands.push({ label: `回复间隔 #${gap.offset}`, start: gap.begin_ns / 1000, stop: gap.end_ns / 1000, tone: 'accent' });
  if (fetch && fetch.rpc_to_receive_ms > 0) {
    bands.push({ label: 'Fetch 返回 → 开始处理', start: fetch.rpc_return_ns / 1000, stop: fetch.receive_start_ns / 1000, tone: 'warning' });
  }
  const related = fetch && [fetch.rpc, ...(fetch.await_io ?? []), ...(fetch.fsync ?? []), fetch.receive];

  return <EventTimeline anchor={anchor} events={events} requestNumber={batch}
    filter={filter} onFilter={setFilter} bands={bands} related={related}>
    {requested && (path ? <div className="request-path-summary">
      {!!path.phases.length && <details className="request-path-phases">
        <summary>整批阶段 · {phaseName(path.phases[0].phase)} {ms(path.phases[0].ms)} ms
          <span className="mono"> · {number(path.phases[0].percent, 2)}%</span></summary>
        <dl>{path.phases.map((row) => <div key={row.phase}>
          <dt>{phaseName(row.phase)}</dt><dd className="mono">{ms(row.ms)} ms · {number(row.percent, 2)}%</dd>
        </div>)}</dl>
      </details>}
      {gap && <>
        <label className="phase-controls request-gap-select">
          <span>回复间隔</span>
          <select value={gap.offset} onChange={(event) => setOffset(Number(event.target.value))}>
            {gaps.map((item) => <option key={item.offset} value={item.offset}>
              offset #{item.offset} · {ms(gapMS(item))} ms
            </option>)}
          </select>
        </label>
        <div className="request-gap-phases" aria-label="所选回复间隔的阶段">
          {gap.phases.map((row) => <span key={row.phase}>{phaseName(row.phase)} <code>{ms(row.ms)} ms</code></span>)}
        </div>
        {fetch && <p className="request-fetch-evidence">
          Fetch 返回 → 开始处理 <strong className="mono">{ms(fetch.rpc_to_receive_ms)} ms</strong>
          {' · '}其中 <strong className="mono">{ms(Math.max(0,
            Math.min(gap.end_ns, fetch.receive_start_ns) - Math.max(gap.begin_ns, fetch.rpc_return_ns)) / 1e6)} ms</strong> 落在这段回复间隔
          {fetch.await_io_overlaps_fsync && <span> · <code>await_io</code> 与 <code>fsync</code> 重叠</span>}
        </p>}
      </>}
    </div> : <p className="request-path-status" role="status" title={error}>
      {error ? '该归档无法按 offset 对齐回复，仍可查看原始事件。' : '正在按 offset 对齐回复…'}
    </p>)}
  </EventTimeline>;
}
