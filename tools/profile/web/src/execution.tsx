import { useEffect, useState } from 'react';
import { Play } from 'lucide-react';
import { api, endpoint } from './model';

type Step = { name: string; status: 'pending' | 'running' | 'done' | 'failed' };
type Progress = {
  enabled: boolean;
  busy?: string;
  execution?: { status: 'running' | 'failed' | 'complete'; error?: string };
  steps?: Step[];
  has_log: boolean;
};

const stepLabels: Record<string, string> = {
  candidate: '构建本轮 Release',
  compare: '运行三轮 Benchmark',
  'candidate-probe': '构建独立插桩二进制',
  'observe-after': '采集 Leader / Follower Trace 和 CPU 栈',
  'report-after': '生成时间线和火焰图',
  summarize: '汇总指标',
  'publish-after': '核对 Langfuse Trace 并发布火焰图',
  archive: '录入实验结果',
};

const stepStates = { pending: '等待', running: '运行中', done: '完成', failed: '失败' };

export function ExecutionPanel({ id, refresh }: { id: string; refresh: () => void }) {
  const url = `${endpoint(id)}/execution`;
  const [progress, setProgress] = useState<Progress | null>(null);
  const [revision, setRevision] = useState(0);
  const [pending, setPending] = useState(false);
  const [loadError, setLoadError] = useState('');
  const [startError, setStartError] = useState('');

  useEffect(() => {
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        const next = await api<Progress>(url, { signal: abort.signal });
        if (abort.signal.aborted) return;
        setProgress(next);
        setLoadError('');
        if (next.execution?.status === 'complete') refresh();
        if (next.execution?.status === 'running' || next.busy) timer = setTimeout(poll, 2000);
      } catch (e) {
        if (abort.signal.aborted) return;
        setLoadError(e instanceof Error ? e.message : '无法读取运行进度');
        timer = setTimeout(poll, 4000);
      }
    }
    void poll();
    return () => { abort.abort(); clearTimeout(timer); };
  }, [url, revision, refresh]);

  async function start(restart = false) {
    setPending(true);
    setStartError('');
    try {
      await api(url, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ restart }),
      });
    } catch (e) {
      setStartError(e instanceof Error ? e.message : '启动失败');
    } finally {
      setPending(false);
      setRevision((n) => n + 1);
    }
  }

  const running = progress?.execution?.status === 'running';
  const failed = progress?.execution?.status === 'failed';
  const disabled = pending || running || !!progress?.busy || progress?.execution?.status === 'complete';
  return <section className="panel execution-panel">
    <div className="panel-heading">
      <div>
        <h2>本轮 Benchmark</h2>
        <p>{progress?.enabled ? '三轮正式测试，另采集 Trace 和火焰图。' :
          progress ? '执行机尚未配置。导入本轮结果后可刷新记录。' : '正在读取执行状态…'}</p>
      </div>
      <div className="execution-actions">
        {progress?.has_log && <a className="button" href={`${url}/log`} target="_blank" rel="noreferrer">执行日志</a>}
        {failed && <button className="button" disabled={disabled} onClick={() => start(true)}>从头运行</button>}
        {progress?.enabled && <button className="button primary" disabled={disabled} onClick={() => start()}>
          <Play size={15} />{pending ? '启动中…' : running ? '运行中' : failed ? '继续运行' : '运行 Benchmark'}
        </button>}
        {progress && !progress.enabled && <button className="button" onClick={refresh}>刷新记录</button>}
      </div>
    </div>
    {progress?.busy && <p className="execution-message" role="status">测试机正在运行 {progress.busy}</p>}
    {(startError || loadError || progress?.execution?.error) && <p className="notice execution-message" role="alert">
      {startError || loadError || progress?.execution?.error}
    </p>}
    {progress?.steps && <ol className="execution-steps" aria-label="执行进度">
      {progress.steps.map(({ name, status }) => <li key={name}>
        <span>{stepLabels[name] ?? (name.startsWith('validate-') ? `正确性验证 ${name.slice(9)}` : name)}</span>
        <span className={`execution-state ${status}`}>{stepStates[status]}</span>
      </li>)}
    </ol>}
  </section>;
}
