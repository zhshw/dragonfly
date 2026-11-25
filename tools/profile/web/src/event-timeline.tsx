import { useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { ArrowDownUp, ChevronRight, Search } from 'lucide-react';
import { number } from './model';
import { TimelineTrack } from './timeline';
import type { Span } from './timeline';

const roles: Record<string, string> = { client: '客户端', leader: 'Leader', follower: 'Follower' };
const roleLabel = (role: string) => roles[role] ?? role;
const ms = (us: number) => number(us / 1000, 3);
const end = (span: Span) => span.ts + span.dur;
const laneKey = ({ cat, pid, name }: Span) => JSON.stringify([cat, String(pid), name]);
const eventKey = ({ cat, pid, tid, name, ts, dur }: Span) => JSON.stringify([cat, String(pid), tid, name, ts, dur]);
const nodeOrder: Record<string, number> = { client: 0, leader: 1, follower: 2 };

type Lane = { spans: Span[]; longest: number };
type LaneGroup = { title: string; order: number; lanes: Map<string, Lane> };
type LaneSort = 'duration' | 'start';

function collectLanes(events: Span[], start: number, stop: number, related?: Span[]) {
  const linked = new Set(related?.map(eventKey));
  const groups = new Map<string, LaneGroup>();
  for (const event of events) {
    const covered = Math.min(stop, end(event)) - Math.max(start, event.ts);
    if (covered <= 0 && (event.dur !== 0 || event.ts < start || event.ts >= stop)) continue;
    const isRelated = linked.size > 0 && linked.has(eventKey(event));
    const groupKey = isRelated ? 'related' : JSON.stringify([event.cat, String(event.pid)]);
    let group = groups.get(groupKey);
    if (!group) {
      const node = event.cat === 'client' ? roleLabel(event.cat) : `${roleLabel(event.cat)} · 节点 ${event.pid}`;
      group = { title: isRelated ? '关联事件' : node, order: isRelated ? -1 : nodeOrder[event.cat] ?? 3, lanes: new Map() };
      groups.set(groupKey, group);
    }
    const key = laneKey(event);
    const lane = group.lanes.get(key);
    if (!lane) group.lanes.set(key, { spans: [event], longest: covered });
    else {
      lane.spans.push(event);
      lane.longest = Math.max(lane.longest, covered);
    }
  }
  for (const { lanes } of groups.values()) {
    for (const { spans } of lanes.values()) spans.sort((a, b) => a.ts - b.ts);
  }
  return [...groups].sort(([, a], [, b]) => a.order - b.order || a.title.localeCompare(b.title, undefined, { numeric: true }));
}

type Edge = 'start' | 'end';
type Mark = { event: Span; edge: Edge };
const markTime = ({ event, edge }: Mark) => edge === 'start' ? event.ts : end(event);

export type TimelineBand = { label: string; start: number; stop: number; tone: 'accent' | 'warning' };

export function EventTimeline({ events, anchor, filter, onFilter, requestNumber, bands, related, children }: {
  events: Span[]; anchor: Span; filter: string; onFilter: (value: string) => void;
  requestNumber: number;
  bands: TimelineBand[]; related?: Span[]; children?: ReactNode;
}) {
  const [focus, setFocus] = useState(anchor);
  const [edge, setEdge] = useState<Edge>('start');
  const [from, setFrom] = useState<Mark>();
  const [to, setTo] = useState<Mark>();
  const [sort, setSort] = useState<LaneSort>('duration');
  const range = { start: anchor.ts, length: Math.max(anchor.dur, 1) };
  const stop = range.start + range.length;
  const anchorKey = laneKey(anchor);
  const groups = useMemo(() => collectLanes(events, anchor.ts, stop, related), [events, anchor.ts, stop, related]);
  const keywords = filter.toLowerCase().split(/[\s,]+/).filter(Boolean);
  const first = ({ spans: [event] }: Lane) => event.ts - anchor.ts;
  const visibleGroups = groups.flatMap(([key, { title, order, lanes }]) => {
    const visible = [...lanes.values()].filter(({ spans: [event] }) =>
      !keywords.length || keywords.some((term) => event.name.toLowerCase().includes(term)));
    return visible.length ? [{ key, title, order, visible }] : [];
  });
  const fromTime = from && markTime(from);
  const toTime = to && markTime(to);
  const offset = (time: number) => 100 * Math.min(1, Math.max(0, (time - range.start) / range.length));

  return <div className="stage-path-content" tabIndex={0} aria-label="Slow Path 时序图">
    <div className="panel-heading phase-heading">
      <div><h2>Slow Path</h2><p className="mono">{anchor.name} #{requestNumber} · {ms(anchor.dur)} ms</p></div>
    </div>
    <div className="slow-path-timeline">
      {children}
      <details className="slow-path-measure-tools">
        <summary>
          <span>事件详情与测量</span>
          <span className="mono slow-path-focus" role="status">{focus.name} · {ms(focus.dur)} ms</span>
        </summary>
        <div className="timeline-detail">
          <strong>{focus.name}</strong>
          <span>{roleLabel(focus.cat)} · 节点 {focus.pid}{focus.tid == null ? '' : ` · TID ${focus.tid}`}</span>
          <span className="mono">{ms(focus.ts)}–{ms(end(focus))} ms · 耗时 {ms(focus.dur)} ms</span>
          {Object.entries(focus.args ?? {}).map(([key, value]) => <code key={key}>{key}={JSON.stringify(value)}</code>)}
        </div>
        <div className="phase-controls slow-path-measure-controls">
          <select aria-label="所选事件时间点" value={edge} onChange={(event) => setEdge(event.target.value === 'end' ? 'end' : 'start')}>
            <option value="start">事件开始</option><option value="end">事件结束</option>
          </select>
          <button type="button" className="button" onClick={() => setFrom({ event: focus, edge })}>设为起点</button>
          <button type="button" className="button" onClick={() => setTo({ event: focus, edge })}>设为终点</button>
          {(from || to) && <button type="button" className="text-link" onClick={() => { setFrom(undefined); setTo(undefined); }}>清除</button>}
        </div>
      </details>
      {(from || to) && <div className="timeline-detail slow-path-measure" aria-label="时间差" role="status">
        {from && <span>起点 {from.event.name} · {ms(markTime(from))} ms</span>}
        {to && <span>终点 {to.event.name} · {ms(markTime(to))} ms</span>}
        {fromTime !== undefined && toTime !== undefined && <strong className="mono">时间差 {ms(toTime - fromTime)} ms</strong>}
      </div>}
      <div className="phase-controls slow-path-plot-controls">
        <label className="search slow-path-lane-filter"><Search size={15} aria-hidden="true" />
          <input type="search" aria-label="筛选时间线轨道" placeholder="筛选轨道（空格分隔）" value={filter}
            onChange={(event) => onFilter(event.target.value)} />
        </label>
        <label className="slow-path-sort">
          <ArrowDownUp size={14} aria-hidden="true" />
          <span>组内排序</span>
          <select aria-label="时间线轨道排序" value={sort} onChange={(event) => setSort(event.target.value === 'start' ? 'start' : 'duration')}>
            <option value="duration">最长单次 ↓</option>
            <option value="start">开始时间 ↑</option>
          </select>
        </label>
      </div>
      <div className="timeline-grid slow-path-grid" role="group" aria-label="同时间窗原始事件">
        <div className="timeline-axis" aria-label="相对所选事件起点的时间刻度">
          {[0, .25, .5, .75, 1].map((fraction) => <span key={fraction}>{ms(range.length * fraction)}{fraction === 1 ? ' ms' : ''}</span>)}
        </div>
        {bands.length > 0 && <section className="slow-path-block" aria-label="关键区间">
          <h3 className="slow-path-block-title">关键区间</h3>
          {bands.map((band) => <div className={`timeline-lane slow-path-band ${band.tone}`} key={band.label}>
            <div className="timeline-label"><strong>{band.label}</strong><span className="mono">{ms(band.stop - band.start)} ms</span></div>
            <div className="slow-path-band-track">
              <span style={{ left: `${offset(band.start)}%`, width: `${offset(band.stop) - offset(band.start)}%` }} />
            </div>
          </div>)}
        </section>}
        {visibleGroups.map(({ key: groupKey, title, order, visible }) => {
          visible.sort((left, right) => (sort === 'duration'
            ? right.longest - left.longest || first(left) - first(right)
            : first(left) - first(right) || right.longest - left.longest) ||
            laneKey(left.spans[0]).localeCompare(laneKey(right.spans[0])));
          return <details key={groupKey} className={`slow-path-block${order < 0 ? ' related' : ''}`} aria-label={title}
            open={order <= 0 || !!keywords.length}>
            <summary className="slow-path-block-title">
              <ChevronRight size={14} aria-hidden="true" />{title}<span>{visible.length} 条轨道</span>
            </summary>
            {visible.map((lane) => {
              const { spans, longest } = lane;
              const event = spans[0];
              const { cat, pid, name } = event;
              const key = laneKey(event);
              const start = first(lane);
              const value = sort === 'duration' ? `窗口内最长 ${ms(longest)} ms` :
                `开始 ${start >= 0 ? '+' : ''}${ms(start)} ms`;
              return <div className="timeline-lane" key={key}>
                <div className="timeline-label" title={name}>
                  <strong>{name}</strong>
                  <span className="mono">{value}{order < 0 && ` · ${roleLabel(cat)} ${pid}`}</span>
                </div>
                <TimelineTrack spans={spans} color={key === anchorKey ? '#5559e1' : order < 0 ? '#af471a' : '#3f8c90'} range={range}
                  label={`${roleLabel(cat)} · ${pid} · ${name}`} picked={spans.includes(focus) ? focus : undefined} onPick={setFocus} />
              </div>;
            })}
          </details>;
        })}
        {!visibleGroups.length && <p className="request-path-status">没有匹配的事件轨道。</p>}
        {(from || to) && <div className="slow-path-ruler" aria-hidden="true">
          {fromTime !== undefined && toTime !== undefined &&
            <span className="slow-path-ruler-range" style={{ left: `${offset(Math.min(fromTime, toTime))}%`,
              width: `${Math.abs(offset(toTime) - offset(fromTime))}%` }} />}
          {[fromTime, toTime].map((time, i) => time !== undefined &&
            <span key={i} className="slow-path-ruler-mark" style={{ left: `${offset(time)}%` }} />)}
        </div>}
      </div>
    </div>
  </div>;
}
