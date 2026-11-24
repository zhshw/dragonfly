import { useEffect, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { FlaskConical, X } from 'lucide-react';
import { api, number, targetTitle } from './model';
import type { Summary, Target } from './model';

function compatible(base: Target, target: Target) {
  const keys = ['clients', 'pipeline_depth', 'payload_mib', 'value_kib', 'warmup_writes', 'settle_seconds', 'rounds_per_variant'] as const;
  return base.harness_sha256 === target.harness_sha256 && keys.every((key) => base.workload[key] === target.workload[key]);
}

export function TargetDetails({ target }: { target: Target }) {
  const w = target.workload;
  return <div className="target-details">
    <strong>{targetTitle(target)}</strong><span className="mono">{number(target.metrics.replication_mib_per_s.median)} MiB/s</span>
    <small>{target.mode === 'raft' ? `Raft ${target.ack_mode}` : '原 Cluster'} · {w.clients} 连接 / Pipeline {w.pipeline_depth} · {w.payload_mib} MiB</small>
    <code title={target.sha256}>{target.sha256.slice(0, 12)}</code>
  </div>;
}

export function CreateExperiment({ close, created }: { close: () => void; created: (id: string) => void }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const [targets, setTargets] = useState<Target[] | null>(null);
  const [previous, setPrevious] = useState('');
  const [baseline, setBaseline] = useState('');
  const [reference, setReference] = useState('');
  const [error, setError] = useState('');
  const [saving, setSaving] = useState(false);
  const base = targets?.find((target) => target.id === baseline);
  const ref = targets?.find((target) => target.id === reference);

  useEffect(() => {
    dialog.current?.showModal();
    const abort = new AbortController();
    api<{ targets: Target[] }>('/api/v1/comparison-targets', { signal: abort.signal })
      .then(({ targets }) => setTargets(targets))
      .catch((e) => { if (!abort.signal.aborted) setError(e.message); });
    return () => abort.abort();
  }, []);

  function changeBaseline(id: string) {
    const selected = targets?.find((target) => target.id === id);
    setBaseline(id);
    if (!selected || (ref && !compatible(selected, ref))) setReference('');
  }

  function changePrevious(id: string) {
    setPrevious(id);
    const target = targets?.find((item) => item.id === id);
    if (target?.cumulative_baseline_id) setBaseline(target.cumulative_baseline_id);
    if (target?.reference_id) setReference(target.reference_id);
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!base || !ref || saving) return;
    const fields = new FormData(event.currentTarget);
    setSaving(true);
    setError('');
    try {
      const summary = await api<Summary>('/api/v1/experiments', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...Object.fromEntries(fields),
          targets: { cumulative_baseline: baseline, reference, previous },
        }),
      });
      created(summary.experiment.id);
    } catch (e) {
      setError(e instanceof Error ? e.message : '创建失败');
      setSaving(false);
    }
  }

  return <dialog ref={dialog} className="create-dialog" aria-labelledby="create-title" onCancel={(e) => { e.preventDefault(); if (!saving) close(); }}>
    <form onSubmit={submit}>
      <header className="dialog-heading">
        <span className="evidence-icon"><FlaskConical size={18} /></span>
        <div><h2 id="create-title">创建实验</h2><p>选择上一轮实验后可沿用它的基线和对比参照。</p></div>
        <button type="button" className="icon-button" onClick={close} disabled={saving} aria-label="关闭"><X size={18} /></button>
      </header>
      <div className="dialog-fields">
        <div className="field-row">
          <label>实验 ID <input required name="id" placeholder="例如 v11" pattern={'[a-z0-9][a-z0-9_\\-]{0,63}'} maxLength={64} autoComplete="off" /></label>
          <label>优化项 <input name="optimization" placeholder="例如 P1-3" maxLength={30} /></label>
        </div>
        <label>标题 <input required name="title" placeholder="本轮试验内容" maxLength={120} /></label>
        <label>说明 <textarea name="description" placeholder="实验假设或验证重点" rows={2} maxLength={320} /></label>
        <label>上一轮
          <select aria-label="上一轮" value={previous} onChange={(event) => changePrevious(event.target.value)}>
            <option value="">选择已完成实验</option>
            {targets?.filter((target) => target.experiment_id && target.variant === 'current').map((target) =>
              <option key={target.id} value={target.id}>{target.experiment_id} · {number(target.metrics.replication_mib_per_s.median)} MiB/s</option>)}
          </select>
        </label>
        <label>基线
          <select aria-label="基线" required value={baseline} onChange={(e) => changeBaseline(e.target.value)}>
            <option value="">选择 Raft memory 基线</option>
            {targets?.filter((target) => target.mode === 'raft' && target.ack_mode === 'memory').map((target) =>
              <option key={target.id} value={target.id}>{targetTitle(target)} · {number(target.metrics.replication_mib_per_s.median)} MiB/s</option>)}
          </select>
        </label>
        {base && <TargetDetails target={base} />}
        <label>对比参照
          <select aria-label="对比参照" required disabled={!base} value={reference} onChange={(e) => setReference(e.target.value)}>
            <option value="">选择同负载的对照记录</option>
            {base && targets?.filter((target) => compatible(base, target)).map((target) =>
              <option key={target.id} value={target.id}>{targetTitle(target)} · {number(target.metrics.replication_mib_per_s.median)} MiB/s</option>)}
          </select>
        </label>
        {ref && <TargetDetails target={ref} />}
        {targets?.length === 0 && <p className="dialog-help">先导入已有实验或用 <code>import-target</code> 登记 benchmark。</p>}
        {error && <p role="alert" className="notice">{error}</p>}
      </div>
      <footer className="dialog-footer">
        <button className="button" type="button" disabled={saving} onClick={close}>取消</button>
        <button className="button primary" type="submit" disabled={saving || !base || !ref}>{saving ? '创建中…' : '创建实验'}</button>
      </footer>
    </form>
  </dialog>;
}
