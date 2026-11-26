import { Fragment, useEffect, useMemo, useState } from 'react';
import { ArrowDown, ArrowUp, ChevronRight, Search } from 'lucide-react';
import { number } from './model';
import type { NodeProfile } from './model';
import { aggregateHotspots, callersFor, parseFoldedCPU } from './flamegraph-profile';
import type { CPUProfile, CPUStack, HotspotView } from './flamegraph-profile';

type Capture = { data: CPUProfile } | { missing: true } | { error: string };
const pageSize = 10;
const modes: { key: HotspotView; label: string }[] = [
  { key: 'leaf', label: '函数自身' }, { key: 'inclusive', label: '含子调用' }, { key: 'stacks', label: '完整调用栈' },
];

export function FlamegraphHotspots({ url, profile }: { url: string; profile?: NodeProfile }) {
  const [capture, setCapture] = useState<Capture>();
  const [request, setRequest] = useState(0);
  const [view, setView] = useState<HotspotView>('leaf');
  const [query, setQuery] = useState('');
  const [ascending, setAscending] = useState(false);
  const [limit, setLimit] = useState(pageSize);
  const [selected, setSelected] = useState<string>();

  useEffect(() => {
    const abort = new AbortController();
    fetch(url, { signal: abort.signal }).then(async (response): Promise<Capture> => {
      if (response.status === 404) return { missing: true };
      if (!response.ok) throw new Error(`请求失败（HTTP ${response.status}）`);
      return { data: parseFoldedCPU(await response.text()) };
    }).catch((error): Capture => ({ error: error.message })).then((result) => {
      if (!abort.signal.aborted) setCapture(result);
    });
    return () => abort.abort();
  }, [url, request]);

  const data = capture && 'data' in capture ? capture.data : undefined;
  const missing = capture && 'missing' in capture;
  const archived = missing && view === 'leaf' && !!profile?.hotspots.length;
  const hotspots = useMemo(() => data ? aggregateHotspots(data, view) : archived ? profile?.hotspots ?? [] : [], [data, view, archived, profile]);
  const search = query.trim().toLowerCase();
  const rows = hotspots.filter((entry) => entry.function.toLowerCase().includes(search))
    .sort((a, b) => (a.samples - b.samples) * (ascending ? 1 : -1) || (a.function < b.function ? -1 : a.function > b.function ? 1 : 0));
  const selectedPaths = useMemo(() => data && selected !== undefined ? callersFor(data, view, selected) : [], [data, view, selected]);

  function resetList() {
    setSelected(undefined);
    setLimit(pageSize);
  }

  return <section className="panel data-panel hotspot-ranking">
    <div className="panel-heading phase-heading">
      <div><h2>CPU 热点排行</h2><p>{view === 'inclusive' ? '同一个样本可计入多个函数，每个函数只计一次。' : '点击一行查看调用路径。'}</p></div>
      <div className="phase-controls hotspot-controls">
        <div className="segments" role="group" aria-label="热点视角">
          {modes.map(({ key, label }) => <button key={key} aria-pressed={view === key} onClick={() => { setView(key); resetList(); }}>{label}</button>)}
        </div>
        <label className="search"><Search size={15} /><input type="search" placeholder="搜索函数或调用栈" aria-label="搜索 CPU 热点" value={query} onChange={(event) => { setQuery(event.target.value); resetList(); }} /></label>
        {data && <span className="count-label">{number(data.samples, 0)} 样本{profile && ` · ${number(profile.cpu_ms, 0)} ms CPU`}</span>}
      </div>
    </div>
    {!capture && <div className="hotspot-status" role="status">正在读取 CPU 样本…</div>}
    {capture && 'error' in capture && <div className="hotspot-status" role="alert">{capture.error} <button className="text-link" onClick={() => { setCapture(undefined); setRequest((n) => n + 1); }}>重试</button></div>}
    {missing && <div className="hotspot-status">{archived ? '仅有归档的叶子热点' : '归档缺少原始调用栈'}</div>}
    {rows.length > 0 && <div className="table-scroll"><table className="data-table hotspot-table">
      <thead><tr>
        <th scope="col">{view === 'stacks' ? '调用栈（从根到叶子）' : '函数'}</th>
        <th scope="col" aria-sort={ascending ? 'ascending' : 'descending'} className="hotspot-measure">
          <button className="event-sort-button" onClick={() => { setAscending(!ascending); resetList(); }}>样本 {ascending ? <ArrowUp size={13} /> : <ArrowDown size={13} />}</button>
        </th>
        <th scope="col" className="hotspot-measure">占比</th>
      </tr></thead>
      <tbody>{rows.slice(0, limit).map(({ function: name, percent, samples }) => {
        const expanded = selected === name;
        const label = view === 'stacks' ? stackLabel(name.split(';')) : name;
        return <Fragment key={name}>
          <tr className={expanded ? 'stage-row-selected' : undefined}>
            <td>{data ? <button className="hotspot-link" title={label} aria-expanded={expanded} onClick={() => setSelected(expanded ? undefined : name)}><ChevronRight size={14} /><span className="mono">{label}</span></button> : <span className="mono" title={name}>{label}</span>}</td>
            <td className="hotspot-measure mono">{number(samples, 0)}</td>
            <td className="hotspot-measure mono">{number(percent, 2)}%</td>
          </tr>
          {expanded && <tr><td colSpan={3} className="hotspot-expanded"><CallerPaths paths={selectedPaths} view={view} /></td></tr>}
        </Fragment>;
      })}</tbody>
    </table></div>}
    {!rows.length && (data || archived) && <div className="inline-empty">没有匹配的 CPU 热点</div>}
    {rows.length > limit && <button className="show-more" onClick={() => setLimit((n) => n + pageSize)}>显示更多（{limit} / {rows.length}）</button>}
  </section>;
}

function stackLabel(frames: string[]) {
  const relevant = frames.filter((frame) => /^(dfly|facade|cmn|util)::/.test(frame));
  const preview = [...new Set([...relevant.slice(-3), frames.at(-1)!])];
  return (preview.length > 1 ? preview : frames.slice(-3)).map((frame) => frame.replaceAll('(anonymous namespace)::', '').split('(')[0]).join(' → ');
}

function CallerPaths({ paths, view }: { paths: CPUStack[]; view: HotspotView }) {
  const [showAll, setShowAll] = useState(false);
  return <section className="hotspot-paths" aria-label="调用路径">
    {(showAll ? paths : paths.slice(0, 6)).map(({ frames, samples }, index) => <details open={view === 'stacks'} key={index}>
      <summary><strong>{number(samples, 0)} 样本</strong><span className="mono" title={frames.join(' → ')}>{stackLabel(frames)}</span></summary>
      <ol className="mono">{frames.map((frame, i) => <li key={i}>{frame}</li>)}</ol>
    </details>)}
    {paths.length > 6 && <button className="text-link" onClick={() => setShowAll(!showAll)}>{showAll ? '仅显示前 6 条路径' : `显示全部 ${number(paths.length, 0)} 条路径`}</button>}
  </section>;
}
