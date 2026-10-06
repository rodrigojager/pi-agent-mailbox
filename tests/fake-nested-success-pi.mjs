import { spawn } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { MailboxClient, ensureToken } from '../src/client.mjs';
import { waitForSubagents } from '../src/wait.mjs';

const baseDir = process.cwd();
const sessionId = randomUUID();
const jobId = randomUUID();
const token = ensureToken(sessionId, baseDir);
const supervisorPath = fileURLToPath(new URL('../src/supervisor.mjs', import.meta.url));
const supervisor = spawn(process.execPath, [supervisorPath, sessionId, baseDir], {
  stdio: 'ignore', windowsHide: true,
});
let client;
try {
  const deadline = Date.now() + 10000;
  while (!client && Date.now() < deadline) {
    try { client = await MailboxClient.connect({ sessionId, baseDir, token, timeoutMs: 500 }); }
    catch (error) {
      if (!['ENOENT', 'ECONNREFUSED', 'ETIMEDOUT'].includes(error.code)) throw error;
      await new Promise(resolveDelay => setTimeout(resolveDelay, 50));
    }
  }
  if (!client) throw new Error('Nested supervisor did not start');
  const adapterPath = resolve(import.meta.dirname, 'fake-adapter.mjs');
  await client.request('start', {
    job: { jobId, coordinatorId: sessionId, workflowId: 'nested-success', depth: 1 },
    adapterPath,
  });
  const wait = await waitForSubagents(client, {
    coordinatorId: sessionId, workflowId: 'nested-success', jobIds: [jobId], mode: 'all', timeoutMs: 15000,
  });
  if (!wait.ready || wait.timedOut || wait.interrupted) throw new Error(`Nested wait failed: ${JSON.stringify(wait)}`);
  const grandchild = await client.request('artifact', { jobId });
  const state = await client.request('job', { jobId });
  const events = await client.request('events', { after: 0, limit: 100 });
  const terminal = events.find(event => event.job_id === jobId && event.event_type === 'terminal');
  if (state?.state !== 'succeeded' || grandchild?.state !== 'succeeded' || !terminal) {
    throw new Error(`Nested result was incomplete: ${JSON.stringify({ state, grandchild, terminal })}`);
  }
  writeFileSync(join(baseDir, 'nested-success.json'), JSON.stringify({
    sessionId, jobId, state: state.state, summary: grandchild.summary, terminalEventId: terminal.event_id,
    depth: process.env.PI_SUBAGENT_DEPTH,
  }), 'utf8');
  const message = {
    role: 'assistant', content: [{ type: 'text', text: `child received grandchild ${jobId}: ${grandchild.summary}` }],
    api: 'fixture', provider: 'fixture', model: 'fixture-model',
    usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { total: 0 } },
    stopReason: 'stop', timestamp: Date.now(),
  };
  process.stdout.write(`${JSON.stringify({ type: 'message_end', message })}\n`);
  process.stdout.write(`${JSON.stringify({ type: 'agent_end', messages: [message] })}\n`);
} finally {
  client?.close();
  if (supervisor.exitCode === null) {
    supervisor.kill();
    await Promise.race([
      new Promise(resolveExit => supervisor.once('exit', resolveExit)),
      new Promise(resolveTimeout => setTimeout(resolveTimeout, 5000)),
    ]);
  }
}
