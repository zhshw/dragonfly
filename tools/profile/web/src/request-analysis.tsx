import { useMemo, useState } from 'react';
import { ChevronRight } from 'lucide-react';
import { number } from './model';
import { RequestSlowPath, useRequestPath } from './request-slow-path';
import { useArchive } from './timeline';
import type { Span } from './timeline';

type Source = { title: string; url: string };
const milliseconds = (microseconds: number) => number(microseconds / 1000, 3);

export function RequestAnalysis({ sources }: { sources: Source[] }) {
  const [chosen, setChosen] = useState('');
  const url = sources.find((source) => source.url === chosen)?.url ?? sources[0]?.url;
  const { data, error, retry } = useArchive(url);
  return <section className="panel data-panel request-analysis" aria-label="请求分析">
    <header className="panel-heading phase-heading">
      <h2>客户端请求</h2>
      {sources.length > 1 && <div className="phase-controls">
        <select aria-label="请求分析诊断归档" value={url} onChange={(event) => setChosen(event.target.value)}>
          {sources.map((source) => <option key={source.url} value={source.url}>{source.title}</option>)}
        </select>
      </div>}
    </header>
    {!url ? <div className="inline-empty">暂无请求时间线</div> :
      error ? <div className="inline-empty" role="alert">{error}<button className="text-link" onClick={retry}>重试</button></div> :
        data ? <RequestList key={url} url={url} tracks={data.tracks} /> :
          <div className="inline-empty" role="status">正在读取客户端请求…</div>}
  </section>;
}

function RequestList({ url, tracks }: { url: string; tracks: Map<string, Span[]> }) {
  const events = useMemo(() => [...tracks.values()].flat(), [tracks]);
  const requests = useMemo(() => [...tracks.entries()].flatMap(([key, spans]) => key.startsWith('client:')
    ? spans.map((span, index) => ({ id: `${key}:${index + 1}`, span, number: index + 1 }))
    : []).sort((a, b) => b.span.dur - a.span.dur || a.span.ts - b.span.ts), [tracks]);
  const phases = useMemo(() => [...new Set(requests.map(({ span }) => span.name))], [requests]);
  const [phase, setPhase] = useState('');
  const [picked, setPicked] = useState('');
  const rows = phase ? requests.filter(({ span }) => span.name === phase) : requests;
  const selected = rows.find(({ id }) => id === picked) ?? rows[0];
  const analysis = useRequestPath(selected?.span.name === 'client.pipeline' ?
    `${url}/request-path?batch=${selected.number}` : undefined);
  const longest = analysis.path?.reply_gaps?.[0];

  if (requests.length === 0) return <div className="inline-empty">该归档暂无客户端请求事件</div>;
  return <>
    {longest && <div className="request-path-longest" role="status">
      <span>最长回复间隔</span>
      <strong className="mono">offset {longest.offset} · {milliseconds((longest.end_ns - longest.begin_ns) / 1000)} ms</strong>
    </div>}
    <div className="stage-event-layout">
      <div className="stage-event-pane">
        <div className="panel-heading phase-heading">
          <div className="phase-controls">
            <select aria-label="客户端阶段" value={phase} onChange={(event) => {
              setPhase(event.target.value);
              setPicked('');
            }}>
              <option value="">所有客户端阶段</option>
              {phases.map((name) => <option key={name} value={name}>{name}</option>)}
            </select>
          </div>
          <span className="count-label">{number(rows.length, 0)} 条请求</span>
        </div>
        <div className="table-scroll stage-event-scroll" role="region" aria-label="按耗时降序的请求" tabIndex={0}>
          <table className="data-table">
            <thead><tr><th scope="col">请求</th><th scope="col" aria-sort="descending">耗时 ms</th>
              <th scope="col">开始 ms</th></tr></thead>
            <tbody>{rows.map(({ id, span, number }) => <tr key={id}
              className={selected?.id === id ? 'stage-row-selected' : undefined}>
              <td><button type="button" className="text-link mono" aria-pressed={selected?.id === id}
                onClick={() => setPicked(id)}><ChevronRight size={14} aria-hidden="true" />{span.name} #{number}</button></td>
              <td className="mono">{milliseconds(span.dur)}</td>
              <td className="mono">{milliseconds(span.ts)}</td>
            </tr>)}</tbody>
          </table>
        </div>
      </div>
      <div className="stage-path-pane">
        {selected && <RequestSlowPath key={selected.id} anchor={selected.span}
          events={events} batch={selected.number} analysis={analysis} />}
      </div>
    </div>
  </>;
}
