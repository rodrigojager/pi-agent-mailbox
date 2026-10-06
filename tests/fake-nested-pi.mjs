import { writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { connectOrStartSupervisor } from '../src/client.mjs';

const baseDir = process.cwd();
const sessionId = randomUUID();
const jobId = randomUUID();
const client = await connectOrStartSupervisor({ sessionId, baseDir });
const adapterPath = resolve(import.meta.dirname, 'fake-adapter.mjs');
await client.request('start', {
  job: { jobId, coordinatorId: sessionId, workflowId: 'nested', depth: 1, delayMs: 60000 },
  adapterPath,
});
const job = await client.request('job', { jobId });
const supervisorPid = await new Promise((resolvePid, reject) => {
  const timer = setTimeout(() => reject(new Error('No nested supervisor heartbeat')), 20000);
  client.subscribe(0, event => {
    if (event.event_type === 'heartbeat') {
      clearTimeout(timer);
      resolvePid(event.payload.supervisorPid);
    }
  }).catch(reject);
});
writeFileSync(join(baseDir, 'nested-activity.json'), JSON.stringify({
  piPid: process.pid, supervisorPid, workerPid: job.worker_pid, sessionId, jobId,
}), 'utf8');
setInterval(() => {}, 60000);
