import { randomUUID } from 'node:crypto';

const WAKE_TYPES = new Set(['terminal', 'supervision_lost', 'worker_exit', 'worker_error', 'persistence_failed']);

/** Subscribe before arming. The initial snapshot and any racing events are both observed. */
export async function waitForSubagents(client, { coordinatorId, workflowId, jobIds, mode, timeoutMs, signal }) {
  const waitId = randomUUID();
  const targets = new Set(jobIds);
  let finished = false;
  let pendingResolve;
  let pendingReject;
  let timer;
  let onAbort;
  const completed = new Promise((resolve, reject) => { pendingResolve = resolve; pendingReject = reject; });
  const wake = async () => {
    if (finished) return;
    try {
      const status = await client.request('wait_status', { waitId });
      if (status?.ready) { finished = true; pendingResolve({ ...status, timedOut: false }); }
    } catch (error) { finished = true; pendingReject(error); }
  };
  let armed = false;
  let raced = false;
  const unsubscribe = await client.subscribe(0, event => {
    if (!targets.has(event.job_id) || !WAKE_TYPES.has(event.event_type)) return;
    if (armed) void wake();
    else raced = true;
  });
  try {
    const initial = await client.request('watch', { wait: { waitId, coordinatorId, workflowId, jobIds, mode } });
    if (initial.ready) return { ...initial, timedOut: false };
    armed = true;
    if (raced) void wake();
    if (signal?.aborted) return { ...initial, interrupted: true, timedOut: false };
    onAbort = () => { if (!finished) { finished = true; pendingResolve({ ...initial, interrupted: true, timedOut: false }); } };
    signal?.addEventListener('abort', onAbort, { once: true });
    timer = setTimeout(() => {
      if (!finished) { finished = true; pendingResolve({ ...initial, timedOut: true }); }
    }, timeoutMs);
    return await completed;
  } finally {
    if (timer) clearTimeout(timer);
    if (onAbort) signal?.removeEventListener('abort', onAbort);
    unsubscribe();
    void client.request('cancel_wait', { waitId }).catch(() => {});
  }
}
