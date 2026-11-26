import { useEffect, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { X } from 'lucide-react';
import { api, endpoint, number, targetTitle } from './model';
import type { Summary, Target } from './model';
import { TargetDetails } from './create';

export function ConfigureComparisons({ summary, close, saved }: {
  summary: Summary; close: () => void; saved: (summary: Summary) => void;
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  const [targets, setTargets] = useState<Target[]>();
  const [baseline, setBaseline] = useState(summary.comparisons.cumulative_baseline?.target.id ?? '');
  const [previous, setPrevious] = useState(summary.comparisons.previous?.target.id ?? '');
  const [reference, setReference] = useState(summary.comparisons.reference?.target.id ?? '');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const id = summary.experiment.id;

  useEffect(() => {
    dialog.current?.showModal();
    const abort = new AbortController();
    api<{ targets: Target[] }>('/api/v1/comparison-targets', { signal: abort.signal })
      .then(({ targets }) => setTargets(targets))
      .catch((e) => { if (!abort.signal.aborted) setError(e.message); });
    return () => abort.abort();
  }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (saving || !targets) return;
    setSaving(true);
    setError('');
    try {
      saved(await api<Summary>(endpoint(id) + '/comparisons', {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cumulative_baseline: baseline, previous, reference }),
      }));
    } catch (e) {
      setError(e instanceof Error ? e.message : '保存失败');
      setSaving(false);
    }
  }

  const fields = [
    { label: '基准', value: baseline, change: setBaseline, eligible: (t: Target) => t.mode === 'raft' && t.ack_mode === 'memory' },
    { label: '上一轮', value: previous, change: setPrevious, eligible: (t: Target) => !!t.experiment_id && t.experiment_id !== id && (t.variant === 'current' || t.variant === 'candidate') && t.mode === 'raft' && t.ack_mode === 'memory' },
    { label: '对比参照', value: reference, change: setReference, eligible: (_t: Target) => true },
  ];
  return <dialog ref={dialog} className="create-dialog" aria-labelledby="comparisons-title"
    onCancel={(event) => { event.preventDefault(); if (!saving) close(); }}>
    <form onSubmit={submit}>
      <header className="dialog-heading">
        <div><h2 id="comparisons-title">配置对照 · {id}</h2><p>选择已有实验或基准记录，保存后更新比较结果。</p></div>
        <button type="button" className="icon-button" onClick={close} disabled={saving} aria-label="关闭"><X size={18} /></button>
      </header>
      <div className="dialog-fields">
        {fields.map(({ label, value, change, eligible }) => {
          const target = targets?.find((item) => item.id === value);
          const differentWorkload = target && (['clients', 'pipeline_depth', 'payload_mib', 'value_kib', 'warmup_writes', 'settle_seconds', 'rounds_per_variant'] as const)
            .some((key) => target.workload[key] !== summary.workload[key]);
          return <div key={label}>
            <label>{label}<select aria-label={label} value={value} disabled={!targets || saving} onChange={(event) => change(event.target.value)}>
              <option value="">未配置</option>
              {targets?.filter(eligible).map((target) => <option key={target.id} value={target.id}>
                {targetTitle(target)} · {number(target.metrics.replication_mib_per_s.median)} MiB/s
              </option>)}
            </select></label>
            {target && <><TargetDetails target={target} />
              {(differentWorkload || target.harness_sha256 !== summary.harness_sha256) &&
                <p className="dialog-help">{differentWorkload ? '负载参数不同' : '压测版本不同'}，允许保存；差值仅供参考。</p>}
            </>}
          </div>;
        })}
        {error && <p role="alert" className="notice">{error}</p>}
      </div>
      <footer className="dialog-footer">
        <button type="button" className="button" onClick={close} disabled={saving}>取消</button>
        <button type="submit" className="button primary" disabled={!targets || saving}>{saving ? '保存中…' : '保存对照'}</button>
      </footer>
    </form>
  </dialog>;
}
