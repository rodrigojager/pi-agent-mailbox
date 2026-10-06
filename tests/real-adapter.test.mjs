import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join, resolve, basename } from 'node:path';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { createSupervisor } from '../src/supervisor.mjs';
import { MailboxClient, ensureToken } from '../src/client.mjs';

const adapterPath = resolve(import.meta.dirname, '../../pi-subagent/src/mailbox/worker-adapter.ts');

test('real pi-subagent adapter launches the supplied Pi CLI entrypoint and returns through the mailbox', {
  skip: !existsSync(adapterPath) && 'Run beside a pi-subagent checkout to exercise the real adapter',
  timeout: 60000,
}, async () => {
  const baseDir = mkdtempSync(join(tmpdir(), 'pi-mailbox-adapter-test-'));
  const target = resolve(baseDir);
  const sessionId = randomUUID();
  const oldAgentDir = process.env.PI_CODING_AGENT_DIR;
  const agentDir = join(baseDir, 'pi-agent');
  mkdirSync(agentDir);
  process.env.PI_CODING_AGENT_DIR = agentDir;
  let supervisor;
  let client;
  try {
    const token = ensureToken(sessionId, baseDir);
    supervisor = createSupervisor({ sessionId, baseDir });
    await supervisor.listen();
    client = await MailboxClient.connect({ sessionId, baseDir, token });
    const jobId = randomUUID();
    const fakePi = fileURLToPath(new URL('./fake-pi-cli.mjs', import.meta.url));
    const terminal = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Real adapter did not return')), 50000);
      client.subscribe(0, event => {
        if (['terminal', 'worker_exit', 'worker_error'].includes(event.event_type)) {
          clearTimeout(timer);
          resolve(event);
        }
      }).catch(reject);
    });
    const job = {
        jobId, coordinatorId: sessionId, workflowId: 'workflow', depth: 0,
        cwd: baseDir, instanceName: 'fixture #1', task: 'Return a portable offline result',
        agent: {
          name: 'fixture', description: 'Offline child', systemPrompt: 'Complete the task.',
          source: 'user', filePath: join(baseDir, 'fixture.md'),
          provider: 'fixture', model: 'fixture-model', thinking: 'off',
          tools: ['complete'], skills: false, extensions: [],
        },
        role: { requested: { kind: 'none' }, origin: 'invocation', status: 'none' },
        agentScope: 'user', projectAgentsDir: null,
        parentModel: { provider: 'fixture', id: 'fixture-model' }, parentThinking: 'off',
        debug: false,
        piInvocation: { command: process.execPath, args: [fakePi] },
      };
    await client.request('start', { job, adapterPath });
    const event = await terminal;
    assert.equal(event.event_type, 'terminal', JSON.stringify(event.payload));
    assert.equal(event.payload.state, 'succeeded');
    const artifact = JSON.parse(readFileSync(event.payload.resultRef, 'utf8'));
    assert.match(artifact.summary, /offline child completed/);
    const argv = JSON.parse(readFileSync(join(baseDir, 'mailbox-child-argv.json'), 'utf8'));
    assert.ok(argv.includes('--mode'));
    assert.ok(argv.includes('json'));
    assert.ok(argv.some(arg => arg.includes('Return a portable offline result')));
    const emptyId = randomUUID();
    const noFinal = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Empty child did not return')), 50000);
      client.subscribe(0, candidate => {
        if (candidate.job_id === emptyId && candidate.event_type === 'terminal') {
          clearTimeout(timer);
          resolve(candidate);
        }
      }).catch(reject);
    });
    const emptyPi = fileURLToPath(new URL('./fake-pi-empty.mjs', import.meta.url));
    await client.request('start', {
      job: { ...job, jobId: emptyId, piInvocation: { command: process.execPath, args: [emptyPi] } }, adapterPath,
    });
    const emptyEvent = await noFinal;
    assert.equal(emptyEvent.payload.state, 'failed');
    assert.match(emptyEvent.payload.summary, /without a final result/i);
  } finally {
    client?.close();
    await supervisor?.close();
    if (oldAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = oldAgentDir;
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-adapter-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true });
  }
});

test('cancelling a real adapter terminates its nested mailbox process tree on Windows', {
  skip: process.platform !== 'win32' || (!existsSync(adapterPath) && 'Requires a pi-subagent checkout'),
  timeout: 90000,
}, async () => {
  const baseDir = mkdtempSync(join(tmpdir(), 'pi-mailbox-nested-test-'));
  const target = resolve(baseDir);
  const sessionId = randomUUID();
  const jobId = randomUUID();
  const oldAgentDir = process.env.PI_CODING_AGENT_DIR;
  const agentDir = join(baseDir, 'pi-agent');
  mkdirSync(agentDir);
  process.env.PI_CODING_AGENT_DIR = agentDir;
  let supervisor;
  let client;
  let nested;
  const isAlive = pid => {
    try { process.kill(pid, 0); return true; }
    catch (error) { if (error.code === 'ESRCH') return false; throw error; }
  };
  try {
    const token = ensureToken(sessionId, baseDir);
    supervisor = createSupervisor({ sessionId, baseDir });
    await supervisor.listen();
    client = await MailboxClient.connect({ sessionId, baseDir, token });
    const nestedPi = fileURLToPath(new URL('./fake-nested-pi.mjs', import.meta.url));
    const job = {
      jobId, coordinatorId: sessionId, workflowId: 'nested-root', depth: 0,
      cwd: baseDir, instanceName: 'nested fixture #1', task: 'Run nested fixture',
      agent: {
        name: 'fixture', description: 'Offline nested child', systemPrompt: 'Complete the task.',
        source: 'user', filePath: join(baseDir, 'fixture.md'), provider: 'fixture',
        model: 'fixture-model', thinking: 'off', tools: ['complete'], skills: false, extensions: [],
      },
      role: { requested: { kind: 'none' }, origin: 'invocation', status: 'none' },
      agentScope: 'user', projectAgentsDir: null,
      parentModel: { provider: 'fixture', id: 'fixture-model' }, parentThinking: 'off',
      debug: false, piInvocation: { command: process.execPath, args: [nestedPi] },
    };
    await client.request('start', { job, adapterPath });
    const activityFile = join(baseDir, 'nested-activity.json');
    const deadline = Date.now() + 35000;
    while (!existsSync(activityFile) && Date.now() < deadline) {
      const state = await client.request('job', { jobId });
      assert.ok(!['failed', 'cancelled', 'unknown'].includes(state.state), `Root failed before nesting: ${state.state}`);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.ok(existsSync(activityFile), 'Nested mailbox did not become ready');
    nested = JSON.parse(readFileSync(activityFile, 'utf8'));
    for (const pid of [nested.piPid, nested.supervisorPid, nested.workerPid]) {
      assert.ok(Number.isSafeInteger(pid) && pid > 0 && isAlive(pid), `Invalid live fixture PID: ${pid}`);
    }
    const terminal = new Promise((resolveEvent, reject) => {
      const timer = setTimeout(() => reject(new Error('Root cancellation did not finish')), 30000);
      client.subscribe(0, event => {
        if (event.job_id === jobId && event.event_type === 'terminal') {
          clearTimeout(timer);
          resolveEvent(event);
        }
      }).catch(reject);
    });
    await client.request('cancel', { jobId });
    const event = await terminal;
    assert.equal(event.payload.state, 'cancelled');
    const exitDeadline = Date.now() + 5000;
    while ([nested.piPid, nested.supervisorPid, nested.workerPid].some(isAlive) && Date.now() < exitDeadline) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.deepEqual([nested.piPid, nested.supervisorPid, nested.workerPid].filter(isAlive), [],
      'Cancellation left a nested Pi process, supervisor or worker alive');
  } finally {
    if (client) {
      try { await client.request('cancel', { jobId }); } catch { /* Already terminal. */ }
      client.close();
    }
    await supervisor?.close();
    if (nested) for (const pid of [nested.piPid, nested.supervisorPid, nested.workerPid]) {
      if (Number.isSafeInteger(pid) && pid > 0 && isAlive(pid)) {
        spawnSync('taskkill', ['/PID', String(pid), '/T', '/F'], { windowsHide: true, stdio: 'ignore' });
      }
    }
    if (oldAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = oldAgentDir;
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-nested-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true });
  }
});
