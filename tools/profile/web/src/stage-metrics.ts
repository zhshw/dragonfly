import type { Role, Stage } from './model';

export type StageSpan = {
  name: string; cat: string; ph: string; pid: string | number; tid?: number;
  ts: number; dur: number; args?: { arg0?: number; arg1?: number };
};
export type StageSample = { span: StageSpan; coveredMS: number };
export type StageWithSamples = Stage & { samples: StageSample[] };
type Interval = [number, number];
type MeasuredStage = { identity: Pick<Stage, 'stage' | 'role' | 'node'>; intervals: Interval[]; samples: StageSample[] };
type StageNode = { id: string; role: Role; current?: string; previous?: string };

export function compareStageNodes(current: Stage[], previous: Stage[]): StageNode[] {
  const nodesFor = (rows: Stage[], role: Role) => [...new Set(rows.filter((row) => row.role === role).map((row) => row.node))];
  const nodes: StageNode[] = [];
  for (const role of ['leader', 'follower'] as const) {
    const currentNodes = nodesFor(current, role);
    const previousNodes = nodesFor(previous, role);
    // A single active node per role can be compared across leadership changes.
    if (currentNodes.length <= 1 && previousNodes.length <= 1) {
      if (currentNodes.length || previousNodes.length) {
        nodes.push({ id: role, role, current: currentNodes[0], previous: previousNodes[0] });
      }
      continue;
    }
    for (const node of [...new Set([...currentNodes, ...previousNodes])].sort()) {
      nodes.push({ id: `${role}:${node}`, role,
        current: currentNodes.includes(node) ? node : undefined,
        previous: previousNodes.includes(node) ? node : undefined });
    }
  }
  return nodes;
}

function merge(intervals: Interval[]): Interval[] {
  const merged: Interval[] = [];
  for (const [start, end] of intervals.sort((a, b) => a[0] - b[0])) {
    const last = merged.at(-1);
    if (last && start <= last[1]) last[1] = Math.max(last[1], end);
    else merged.push([start, end]);
  }
  return merged;
}

const duration = (intervals: Interval[]) => intervals.reduce((total, [start, end]) => total + (end - start), 0);

export function summarizeEventTimes(samples: StageSample[]) {
  if (samples.length === 0) return;
  let min = Infinity, max = -Infinity, mean = 0, squaredDeviations = 0;
  for (const [index, { coveredMS }] of samples.entries()) {
    min = Math.min(min, coveredMS);
    max = Math.max(max, coveredMS);
    const delta = coveredMS - mean;
    mean += delta / (index + 1);
    squaredDeviations += delta * (coveredMS - mean);
  }
  return { max, min, mean, variance: squaredDeviations / samples.length };
}

export function measureServerStages(events: StageSpan[], clientPhase: string): {
  windowMS: number; stages: StageWithSamples[]; events: StageSpan[];
} {
  if (!Array.isArray(events)) throw new Error('时间线格式无效');
  const spans = events.filter(({ ph, ts, dur }) => ph === 'X' && Number.isFinite(ts + dur) && dur >= 0);
  const windows = merge(spans.filter(({ cat, name }) => cat === 'client' && name === clientPhase)
    .map(({ ts, dur }) => [ts, ts + dur]));
  const windowMS = duration(windows) / 1000;
  const measured = new Map<string, MeasuredStage>();
  if (windowMS === 0) return { windowMS, stages: [], events: spans };
  for (const span of spans) {
    if (span.cat !== 'leader' && span.cat !== 'follower') continue;
    const clipped: Interval[] = [];
    for (const [start, end] of windows) {
      if (span.ts >= end) continue;
      const left = Math.max(start, span.ts), right = Math.min(end, span.ts + span.dur);
      if (left < right || (span.dur === 0 && span.ts >= start)) clipped.push([left, right]);
    }
    if (clipped.length === 0) continue;
    const node = String(span.pid);
    const key = `${span.cat}:${node}:${span.name}`;
    let row = measured.get(key);
    if (!row) {
      row = { identity: { stage: span.name, role: span.cat, node }, intervals: [], samples: [] };
      measured.set(key, row);
    }
    row.intervals.push(...clipped);
    row.samples.push({ span, coveredMS: duration(clipped) / 1000 });
  }
  const stages = [...measured.values()].map(({ identity, intervals, samples }) => {
    samples.sort((a, b) => b.coveredMS - a.coveredMS || a.span.ts - b.span.ts);
    return {
      ...identity, samples, union_ms: duration(merge(intervals)) / 1000, count: samples.length,
      p95_ms: samples[samples.length - Math.ceil(samples.length * .95)].coveredMS,
    };
  }).sort((a, b) => b.union_ms - a.union_ms);
  return { windowMS, stages, events: spans };
}
