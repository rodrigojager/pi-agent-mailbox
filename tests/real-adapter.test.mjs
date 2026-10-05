import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync } from 'node:fs';
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
