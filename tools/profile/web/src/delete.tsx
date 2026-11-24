import { useEffect, useRef, useState } from 'react';
import { api, endpoint } from './model';
import type { ExperimentLabel } from './model';

export function DeleteExperiment({ experiment, close, deleted }: {
  experiment: ExperimentLabel;
  close: () => void;
  deleted: (warning?: string) => void;
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => { dialog.current?.showModal(); }, []);

  async function remove() {
    setPending(true);
    setError('');
    try {
      const result = await api<{ warning?: string }>(endpoint(experiment.id), { method: 'DELETE' });
      deleted(result.warning);
    } catch (e) {
      setError(e instanceof Error ? e.message : '删除失败');
      setPending(false);
    }
  }

  return <dialog ref={dialog} className="create-dialog" aria-labelledby="delete-title"
    onCancel={(event) => { event.preventDefault(); if (!pending) close(); }}>
    <header className="dialog-heading">
      <div><h2 id="delete-title">删除实验</h2><p>{experiment.id} · {experiment.title}</p></div>
    </header>
    <div className="dialog-fields">
      <p>将删除平台上的实验记录、指标和关联归档。</p>
      {error && <p className="notice" role="alert">{error}</p>}
    </div>
    <footer className="dialog-footer">
      <button type="button" className="button" disabled={pending} onClick={close} autoFocus>取消</button>
      <button type="button" className="button danger" disabled={pending} onClick={remove}>{pending ? '删除中…' : '确认删除'}</button>
    </footer>
  </dialog>;
}
