import type { StageSpan } from './stage-metrics';

export type PathEntry = { span: StageSpan; at: number; detail?: string };
type PathInterval = { from: PathEntry; to: PathEntry; label: string; title: 'Blocking wait' | 'Observed wait' };
type PathDetails = { entries: PathEntry[]; description: string; interval?: PathInterval };
export type SlowPath = PathDetails & { context: StageSpan[] };

const end = (span: StageSpan) => span.ts + span.dur;
const overlap = (span: StageSpan, start: number, stop: number) =>
  Math.max(0, Math.min(end(span), stop) - Math.max(span.ts, start));
const finish = (span: StageSpan, detail: string): PathEntry => ({ span, at: end(span), detail });
const start = (span: StageSpan, detail?: string): PathEntry => ({ span, at: span.ts, detail });

function latestBefore(spans: StageSpan[], time: number) {
  let best: StageSpan | undefined;
  for (const span of spans) {
    if (end(span) <= time && (!best || end(span) > end(best))) best = span;
  }
  return best;
}

function admissionPath(events: StageSpan[], sample: StageSpan, context: StageSpan[]): PathDetails | undefined {
  const sameNode = events.filter((span) => span.pid === sample.pid && span.cat === sample.cat);
  const tickets = sameNode.filter((span) => span.name === 'raft.set.ticket_lifetime' &&
    sample.tid !== undefined && span.tid === sample.tid &&
    end(span) >= sample.ts && end(span) <= end(sample)).sort((a, b) => end(a) - end(b));
  const batch = context.find((span) => span.ts <= sample.ts && end(span) >= end(sample));
  const wal = sameNode.filter((span) => span.name === 'raft.wal.append.complete');
  const writes = wal.length ? wal : sameNode.filter((span) => span.name === 'raft.folly.write');
  const lastWrite = batch && latestBefore(writes.filter((span) => end(span) >= batch.ts), end(sample));
  if (!lastWrite || !tickets.length) return;
  const retirement = finish(tickets[0], 'First ticket retired');
  const walCompletion = finish(lastWrite, 'Last WAL completion before admission');
  const admitted = finish(sample, 'Admission resumes');
  const from = walCompletion.at >= retirement.at ? walCompletion : retirement;
  if (from.at >= admitted.at) return;
  const entries = [start(sample, 'Admission wait starts'), retirement, walCompletion];
  if (tickets.length > 1) entries.push(finish(tickets[tickets.length - 1], 'Last ticket retired'));
  entries.push(admitted);
  const interval: PathInterval = {
    from,
    to: admitted,
    label: 'Ticket credits released; no further WAL completion before admission',
    title: 'Blocking wait',
  };
  return { entries, description: `${tickets.length} tickets retired on the same thread`, interval };
}

function fetchPath(events: StageSpan[], sample: StageSpan): PathDetails | undefined {
  const follower = events.filter((span) => span.cat === 'follower' && span.pid === sample.pid);
  const calls = follower.filter((span) => span.name === 'raft.fetch.rpc');
  const receives = follower.filter((span) => span.name === 'raft.fetch.receive').sort((a, b) => a.ts - b.ts);
  const receive = sample.name === 'raft.fetch.receive' ? sample : sample.name === 'raft.fetch.appended' ?
    latestBefore(receives, sample.ts) : receives.find((span) => span.ts >= end(sample));
  if (!receive || (sample.name === 'raft.fetch.appended' &&
    (sample.args?.arg0 === undefined || receive.args?.arg0 !== sample.args.arg0))) return;
  const rpc = latestBefore(calls, receive.ts);
  if (!rpc || (sample.name === 'raft.fetch.rpc' && rpc !== sample)) return;
  const appended = sample.name === 'raft.fetch.appended' ? sample : follower.find((span) =>
    span.name === 'raft.fetch.appended' && span.ts >= end(receive) && receive.args?.arg0 !== undefined &&
    span.args?.arg0 === receive.args.arg0 &&
    latestBefore(receives, span.ts) === receive
  );
  const interval: PathInterval = {
    from: finish(rpc, 'RPC returns'), to: start(receive, 'Response processing starts'),
    label: 'I/O wait overlaps follower fsync before response processing',
    title: 'Blocking wait',
  };
  const waiting = follower.filter((span) => overlap(span, interval.from.at, interval.to.at) > 0);
  const io = waiting.filter((span) => span.name === 'raft.await_io' && rpc.tid !== undefined && span.tid === rpc.tid);
  const syncs = waiting.filter((span) => span.name === 'raft.folly.fsync');
  const overlaps = (wait: StageSpan, sync: StageSpan) =>
    overlap(wait, Math.max(interval.from.at, sync.ts), Math.min(interval.to.at, end(sync))) > 0;
  const blocked = io.filter((wait) => syncs.some((sync) => overlaps(wait, sync)));
  if (!blocked.length) return;
  const activeSyncs = syncs.filter((sync) => blocked.some((wait) => overlaps(wait, sync)));
  const entries = [interval.from, ...[...blocked, ...activeSyncs].map((span) => ({
    span, at: Math.max(interval.from.at, span.ts),
    detail: span.ts < interval.from.at ? 'Active at RPC return' : undefined,
  })), interval.to];
  const offset = appended?.args?.arg1;
  return { entries, interval, description: offset === undefined ? '' : `Log offset ${offset}` };
}

const waitName = /(?:^|[._])(?:wait|await|queue|queued|backpressure|backoff)(?:[._]|$)/;

// Containment on one thread provides timing context. It does not establish a dependency.
function recordedPath(events: StageSpan[], sample: StageSpan): PathDetails {
  const entered = start(sample, sample.dur === 0 ? 'Instant event' : 'Event starts');
  const finished = finish(sample, 'Event ends');
  const entries = sample.dur === 0 ? [entered] : [entered, finished];
  if (sample.dur > 0 && waitName.test(sample.name)) {
    return {
      entries, description: 'Recorded wait event',
      interval: { from: entered, to: finished, label: sample.name, title: 'Observed wait' },
    };
  }

  let nested: StageSpan | undefined;
  let waiting: StageSpan | undefined;
  for (const span of events) {
    if (sample.tid === undefined || span.tid !== sample.tid || span.pid !== sample.pid || span.cat !== sample.cat ||
        span.dur <= 0 || span.ts < sample.ts || end(span) > end(sample) ||
        (span.ts === sample.ts && end(span) === end(sample))) continue;
    if (!nested || span.dur > nested.dur) nested = span;
    if (waitName.test(span.name) && (!waiting || span.dur > waiting.dur)) waiting = span;
  }
  if (waiting) {
    const from = start(waiting, 'Wait starts');
    const to = finish(waiting, 'Wait ends');
    return {
      entries: [...entries, from, to], description: 'Longest recorded wait contained in the same-thread event',
      interval: { from, to, label: waiting.name, title: 'Observed wait' },
    };
  }
  if (nested) {
    entries.push(start(nested, 'Nested span starts'), finish(nested, 'Nested span ends'));
    return { entries, description: 'Event timing and its longest contained span on the same thread' };
  }
  return { entries, description: 'Recorded event timing' };
}

export function traceSlowPath(events: StageSpan[], phase: string, sample: StageSpan): SlowPath {
  const context = events.filter((span) => span.cat === 'client' && span.name === phase &&
    (overlap(span, sample.ts, end(sample)) > 0 || (sample.dur === 0 && sample.ts >= span.ts && sample.ts < end(span))))
    .sort((a, b) => a.ts - b.ts);
  let linked: PathDetails | undefined;
  if (sample.cat === 'leader' && sample.name === 'raft.set.admission.wait') {
    linked = admissionPath(events, sample, context);
  } else if (sample.cat === 'follower' && ['raft.fetch.rpc', 'raft.fetch.receive', 'raft.fetch.appended'].includes(sample.name)) {
    linked = fetchPath(events, sample);
  }
  const path = linked ?? recordedPath(events, sample);
  return { context, ...path, entries: path.entries.sort((a, b) => a.at - b.at) };
}
