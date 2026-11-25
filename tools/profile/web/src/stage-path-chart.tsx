import { useState } from 'react';
import type { StageSpan } from './stage-metrics';
import type { SlowPath, PathEntry } from './stage-path';

const ms = (microseconds: number) => (microseconds / 1000).toFixed(3);
const end = (span: StageSpan) => span.ts + span.dur;
const ticks = [0, .25, .5, .75, 1];

export function StagePathChart({ path, picked, windows }: { path: SlowPath; picked: StageSpan; windows: StageSpan[] }) {
  const [active, setActive] = useState<PathEntry>();
  const wait = path.interval;
  const from = Math.min(...path.entries.map(({ at }) => at));
  const to = Math.max(from + 1, ...path.entries.map(({ at }) => at));
  const position = (at: number) => 100 * (Math.min(to, Math.max(from, at)) - from) / (to - from);
  const range = (begin: number, finish: number) => ({ left: `${position(begin)}%`, width: `${position(finish) - position(begin)}%` });
  const gap = wait && range(wait.from.at, wait.to.at);
  const waitCenter = wait && `${position((wait.from.at + wait.to.at) / 2)}%`;
  const waitMS = wait && ms(wait.to.at - wait.from.at);
  const lanes = new Map<string, PathEntry[]>();
  for (const entry of path.entries) {
    const { cat, pid, tid, name } = entry.span;
    const key = `${cat}:${pid}:${tid ?? ''}:${name}`;
    const lane = lanes.get(key);
    if (lane) lane.push(entry);
    else lanes.set(key, [entry]);
  }
  const selected = active ?? wait?.from ?? path.entries[0];
  return <>
    <div className="panel-heading">
      <div><h2>Slow path</h2>{path.description && <p>{path.description}</p>}
        {path.context.length > 0 && <p className="mono">{path.context.map((span) =>
          `${span.name} #${windows.indexOf(span) + 1} · ${ms(span.ts)}–${ms(end(span))} ms`
        ).join(' / ')}</p>}
      </div>
    </div>
    {wait && <div className="event-path-wait-summary" aria-label={wait.title}>
      <div className="event-path-wait-duration"><span>{wait.title}</span><strong>{waitMS} ms</strong>
        {wait.title === 'Blocking wait' && picked.name === 'raft.set.admission.wait' &&
          <small>of {ms(picked.dur)} ms admission wait</small>}
      </div>
      <div className="event-path-wait-details">
        <p>{wait.label}</p>
        <dl>{([['From', wait.from], ['To', wait.to]] as const).map(([direction, entry]) =>
          <div key={direction}>
            <dt>{direction} · <span className="mono">{ms(entry.at)} ms</span></dt>
            <dd>{entry.detail}</dd>
          </div>)}</dl>
      </div>
    </div>}
    <div className="table-scroll event-path-frame">
      <div className="event-path-chart" role="group" aria-label="Slow path timeline">
        <div className="event-path-row event-path-scale">
          <div>Trace time (ms)</div>
          <div className="event-path-track">
            {ticks.map((fraction) => <span className="event-path-tick" key={fraction}
              style={{ left: `${fraction * 100}%` }}>{ms(from + (to - from) * fraction)}</span>)}
          </div>
        </div>
        {wait && <div className="event-path-row event-path-interval">
          <div>{waitMS} ms</div>
          <div className="event-path-track">
            <span aria-hidden="true" className="event-path-wait-bar" style={gap} />
            <span aria-hidden="true" className="event-path-wait-leader" style={{ left: waitCenter }} />
            <span className="event-path-wait-tag" style={{ left: `clamp(43px, ${waitCenter}, calc(100% - 43px))` }}>
              {wait.title.toLowerCase()}
            </span>
          </div>
        </div>}
        {[...lanes].map(([key, entries]) => {
          const { cat, pid, tid, name } = entries[0].span;
          const spans = [...new Set(entries.map(({ span }) => span))];
          return <div className="event-path-row event-path-lane" key={key}>
            <div className="event-path-label">
              <strong className="mono" title={name}>{name}</strong>
              <span>{cat} {pid}{tid === undefined ? '' : ` · TID ${tid}`}</span>
            </div>
            <div className="event-path-track">
              {ticks.slice(1, -1).map((fraction) => <span className="event-path-gridline" key={fraction}
                aria-hidden="true" style={{ left: `${fraction * 100}%` }} />)}
              {gap && <span className="event-path-shade" aria-hidden="true" style={gap} />}
              {spans.map((span, index) => <span key={index} aria-hidden="true"
                className={`event-path-span${span === picked ? ' selected' : ''}`} style={range(span.ts, end(span))} />)}
              {entries.map((entry, index) => {
                const label = `${entry.detail ?? name} · ${ms(entry.at)} ms`;
                const boundary = entry === wait?.from || entry === wait?.to;
                return <button type="button" key={index} className={`event-path-point${entry.span === picked ? ' selected' : ''}${boundary ? ' boundary' : ''}`}
                  style={{ left: `${position(entry.at)}%` }} aria-label={label} title={label}
                  aria-pressed={entry === selected} onClick={() => setActive(entry)} />;
              })}
            </div>
          </div>;
        })}
      </div>
    </div>
    <div className="event-path-inspector" role="status">
      <code>{selected.span.name}</code>
      <span>{selected.detail ?? 'Span starts'} · {ms(selected.at)} ms</span>
      <span>Span duration {ms(selected.span.dur)} ms</span>
      {Object.entries(selected.span.args ?? {}).map(([key, value]) => <code key={key}>{key}={value}</code>)}
    </div>
  </>;
}
