export type Variant = 'baseline' | 'current' | 'cluster';
export type Role = 'leader' | 'follower';
export type View = 'overview' | 'runs' | 'flamegraphs' | 'stages' | 'trace';

export const decisionNames = { retained: '已保留', reverted: '已回退', pending: '待评估' } as const;

export interface ExperimentLabel {
  id: string;
  title: string;
  optimization?: string;
  date?: string;
  decision: keyof typeof decisionNames;
  description?: string;
  state: 'draft' | 'complete';
}

export interface Distribution { median: number; min: number; max: number }

export interface Metrics {
  replication_mib_per_s: Distribution;
  set_mib_per_s: Distribution;
  wait_tail_ms: Distribution;
  batch_p95_ms: Distribution;
}

export interface Workload {
  ack_mode: string;
  clients: number;
  pipeline_depth: number;
  payload_mib: number;
  value_kib: number;
  warmup_writes: number;
  settle_seconds: number;
  rounds_per_variant: number;
}

export interface Hotspot { function: string; samples: number; percent: number }

export interface NodeProfile {
  cpu_ms: number;
  cpu_ms_per_mib: number;
  peak_rss_mib?: number;
  fsync_await_ms: number;
  worker_queue_ms: number;
  starts_during_fsync: number;
  hotspots: Hotspot[];
}

export interface DiagnosticSummary {
  span_count: number;
  fsync_union_ms: number;
  nodes: Record<Role, NodeProfile>;
  trace_url?: string;
  verified_observations?: number;
}

export interface Target {
  id: string;
  title: string;
  experiment_id?: string;
  variant: string;
  cumulative_baseline_id?: string;
  reference_id?: string;
  benchmark_archive?: string;
  sha256: string;
  harness_sha256: string;
  mode: 'raft' | 'yes';
  ack_mode?: string;
  workload: Workload;
  metrics: Metrics;
}

export function targetTitle(target?: Target) {
  if (target?.experiment_id && (target.variant === 'current' || target.variant === 'candidate')) {
    return `${target.experiment_id} · 本轮`;
  }
  return target?.title;
}

export interface TargetComparison {
  target: Target;
  result?: { throughput_pct: number; p95_pct: number; target_throughput_pct: number };
}

export interface Summary {
  experiment: ExperimentLabel;
  workload: Workload;
  variants: Partial<Record<Variant, { sha256: string; metrics: Metrics }>>;
  delta: { throughput_pct: number; p95_pct: number; paired_throughput_pct: number[] | null; paired_p95_pct?: number[] };
  evidence: { confirmed_runs: number; benchmark_runs: number; diagnostics: number; flamegraphs: number };
  diagnostics?: Partial<Record<Variant, DiagnosticSummary>>;
  comparisons: { cumulative_baseline?: TargetComparison; reference?: TargetComparison; previous?: TargetComparison };
  validation?: Record<string, { unit_tests: number; integration_tests: number }>;
}

export interface Run {
  id: string;
  kind: 'benchmark' | 'diagnostic';
  variant: string;
  benchmark_set?: string;
  order?: number;
  trial?: number;
  status: 'passed';
  replication_mib_per_s: number;
  set_mib_per_s: number;
  wait_tail_ms: number;
  batch_p95_ms: number;
  recovery_ms: number;
  log?: string;
}

export interface Stage {
  node: string;
  role: Role | 'client';
  stage: string;
  count: number;
  union_ms: number;
  p95_ms: number;
}

export interface Observation {
  variant: Variant | 'candidate';
  flamegraphs: Partial<Record<Role, string>>;
  timeline?: string;
  stages: Stage[];
}

export interface DiagnosticCapture {
  experiment_id: string;
  profile: DiagnosticSummary;
  observation: Observation;
}

export interface DiagnosticComparison {
  current?: DiagnosticCapture;
  previous?: DiagnosticCapture;
  previous_id?: string;
}

export interface ReferenceDiagnostics {
  target: Target;
  capture?: DiagnosticCapture;
}

export interface Detail {
  summary: Summary;
  runs: Run[];
  observations: Observation[];
  benchmark_sets?: {id: string; title: string; workload: Workload}[];
  validation_checks?: { build: string; suite: string; tests: number; log: string }[];
}

export interface RunSource {
  experiment_id?: string;
  source_title?: string;
  sha256?: string;
  benchmarks?: { id: string; title: string; workload: Workload; runs: Run[] }[];
  diagnostics?: Run[];
  validation?: Summary['validation'];
  validation_checks?: Detail['validation_checks'];
}

export type RunComparison = Partial<Record<'baseline' | 'previous' | 'current', RunSource>>;

export const referenceHref = (targetID: string) => `#/reference/${encodeURIComponent(targetID)}/diagnostics`;

export const endpoint = (id: string) => `/api/v1/experiments/${encodeURIComponent(id)}`;

export const artifactURL = (id: string, artifact: string) => `${endpoint(id)}/artifacts/${encodeURIComponent(artifact)}`;

export async function api<T>(url: string, options?: RequestInit): Promise<T> {
  const headers = new Headers(options?.headers);
  headers.set('Accept', 'application/json');
  const response = await fetch(url, { ...options, headers });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || `请求失败（HTTP ${response.status}）`);
  }
  return response.json() as Promise<T>;
}

export const number = (n: number, places = 2) => n.toLocaleString('en-US', {
  minimumFractionDigits: places,
  maximumFractionDigits: places,
});

export const percent = (n: number) => `${n >= 0 ? '+' : '−'}${number(Math.abs(n), 1)}%`;
