import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, basename } from 'node:path';
import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { fileURLToPath } from 'node:url';
import { createJiti } from 'jiti';
import { createSupervisor } from '../src/supervisor.mjs';
import { MailboxClient, ensureToken } from '../src/client.mjs';
import { statePath } from '../src/protocol.mjs';

const jiti = createJiti(import.meta.url);
const { default: registerAgentMailbox } = await jiti.import('../src/index.ts');

test('legacy mode remains selectable for new jobs while the mailbox stays loaded', async () => {
  const branch = [];
  const commands = new Map();
  const pi = {
    events: new EventEmitter(),
    on() {},
    appendEntry(customType, data) { branch.push({ id: randomUUID(), type: 'custom', customType, data }); },
    registerCommand(name, command) { commands.set(name, command); },
    registerTool() {},
  };
  const ctx = {
    ui: { notify() {} },
    sessionManager: { getSessionId: () => randomUUID(), getBranch: () => branch },
  };
  registerAgentMailbox(pi);
  let acceptCount = 0;
  const probe = () => {
    pi.events.emit('rodrigojager:pi-agent-mailbox:request:v1', {
      context: ctx, accept: () => { acceptCount++; },
    });
  };
  probe();
  assert.equal(acceptCount, 1);
  await commands.get('mailbox').handler('legacy', ctx);
  probe();
  assert.equal(acceptCount, 1);
  await commands.get('mailbox').handler('durable', ctx);
  probe();
  assert.equal(acceptCount, 2);
});

async function until(check, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for fixture event');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}

test('a result stays with its branch and ACK follows recorded history', async () => {
  const baseDir = mkdtempSync(join(tmpdir(), 'pi-mailbox-extension-test-'));
  const target = resolve(baseDir);
  const sessionId = randomUUID();
  let supervisor;
  let inspector;
  try {
    ensureToken(sessionId, baseDir);
    supervisor = createSupervisor({ sessionId, baseDir });
    await supervisor.listen();
    const handlers = new Map();
    const sent = [];
    let branch = [];
    const originalBranch = branch;
    const ctx = {
      cwd: baseDir,
      hasUI: false,
      ui: { setStatus() {}, notify() {} },
      sessionManager: {
        getSessionId: () => sessionId,
        getBranch: () => branch,
        getLeafId: () => branch.at(-1)?.id ?? null,
      },
    };
    const pi = {
      events: new EventEmitter(),
      on(name, handler) { handlers.set(name, [...(handlers.get(name) ?? []), handler]); },
      appendEntry(customType, data) { branch.push({ id: randomUUID(), type: 'custom', customType, data }); },
      sendMessage(message, options) { sent.push({ message, options }); },
      registerCommand() {},
      registerTool() {},
    };
    registerAgentMailbox(pi, { baseDir });
    let start;
    pi.events.emit('rodrigojager:pi-agent-mailbox:request:v1', {
      context: ctx, accept: handler => { start = handler; },
    });
    assert.equal(typeof start, 'function');
    const jobId = randomUUID();
    const adapterPath = fileURLToPath(new URL('./fake-adapter.mjs', import.meta.url));
    await start({ jobId, delayMs: 100 }, adapterPath);
    assert.equal(originalBranch.some(entry => entry.customType === 'mailbox-workflow'), true);

    // A different branch of the same session must not receive this result.
    branch = [];
    inspector = await MailboxClient.connect({ sessionId, baseDir, token: ensureToken(sessionId, baseDir) });
    await until(() => supervisor.store.getJob(jobId)?.state === 'succeeded');
    await new Promise(resolve => setTimeout(resolve, 20));
    assert.equal(sent.length, 0);
    assert.equal(supervisor.store.db.prepare('SELECT state FROM deliveries WHERE event_id=?').get(`result:${jobId}`).state, 'pending');

    branch = originalBranch;
    for (const handler of handlers.get('session_tree') ?? []) handler({ oldLeafId: null, newLeafId: null }, ctx);
    await until(() => sent.length === 1);
    assert.equal(sent[0].message.customType, 'subagent-result');
    assert.equal(sent[0].message.details.mailboxEventId, `result:${jobId}`);
    await new Promise(resolve => setTimeout(resolve, 20));
    assert.equal(supervisor.store.db.prepare('SELECT state FROM deliveries WHERE event_id=?').get(`result:${jobId}`).state, 'pending');

    branch.push({ id: 'history-result', type: 'custom_message', customType: 'subagent-result', details: sent[0].message.details });
    for (const handler of handlers.get('message_end') ?? []) handler({}, ctx);
    await until(() => supervisor.store.db.prepare('SELECT state FROM deliveries WHERE event_id=?').get(`result:${jobId}`).state === 'recorded');
    assert.equal(supervisor.store.db.prepare('SELECT entry_id FROM deliveries WHERE event_id=?').get(`result:${jobId}`).entry_id, 'history-result');
    const largeJobId = randomUUID();
    await start({ jobId: largeJobId, summaryLength: 1200000 }, adapterPath);
    await until(() => sent.length === 2);
    assert.ok(sent[1].message.content.length < 10000, 'the model-facing result must stay bounded');
    assert.match(sent[1].message.content, /Full result artifact:/);
    const artifact = join(statePath(baseDir, sessionId), 'artifacts', `${largeJobId}.json`);
    assert.equal(JSON.parse(readFileSync(artifact, 'utf8')).summary.length, 1200000);
    await until(() => sent.length === 3, 8000);
    assert.equal(sent[2].message.details.mailboxEventId, `result:${largeJobId}`, 'an unrecorded result is retried by event ID');
    assert.equal(supervisor.store.db.prepare('SELECT COUNT(*) AS count FROM events WHERE event_id=?').get(`result:${largeJobId}`).count, 1);
    branch.push({ id: 'large-history-result', type: 'custom_message', customType: 'subagent-result', details: sent[2].message.details });
    for (const handler of handlers.get('message_end') ?? []) handler({}, ctx);
    await until(() => supervisor.store.db.prepare('SELECT state FROM deliveries WHERE event_id=?').get(`result:${largeJobId}`).state === 'recorded');
    for (const handler of handlers.get('session_shutdown') ?? []) handler({}, ctx);
  } finally {
    inspector?.close();
    await supervisor?.close();
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-extension-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true });
  }
});

test('goal all wait stays quiet on partial completion and sends one grouped result', async () => {
  const baseDir = mkdtempSync(join(tmpdir(), 'pi-mailbox-extension-test-'));
  const target = resolve(baseDir);
  const sessionId = randomUUID();
  const workflowId = randomUUID();
  const goalId = randomUUID();
  const branch = [
    { id: 'workflow-marker', type: 'custom', customType: 'mailbox-workflow', data: { id: workflowId, sessionId } },
    { id: 'goal-marker', type: 'custom', customType: 'goal-state', data: { goal: { id: goalId, status: 'active', waiting: { reason: 'children' } } } },
  ];
  let supervisor;
  let client;
  try {
    const token = ensureToken(sessionId, baseDir);
    supervisor = createSupervisor({ sessionId, baseDir });
    await supervisor.listen();
    client = await MailboxClient.connect({ sessionId, baseDir, token });
    for (const jobId of ['first', 'second']) {
      await client.request('register', { job: { jobId, coordinatorId: sessionId, workflowId } });
    }
    client.close();
    client = undefined;
    await until(() => !supervisor.ownerPresent());
    const handlers = new Map();
    const sent = [];
    const ctx = {
      hasUI: false,
      ui: { setStatus() {}, notify() {} },
      sessionManager: { getSessionId: () => sessionId, getBranch: () => branch },
    };
    const pi = {
      events: new EventEmitter(),
      on(name, handler) { handlers.set(name, [...(handlers.get(name) ?? []), handler]); },
      appendEntry(customType, data) { branch.push({ id: randomUUID(), type: 'custom', customType, data }); },
      sendMessage(message, options) { sent.push({ message, options }); },
      registerCommand() {}, registerTool() {},
    };
    registerAgentMailbox(pi, { baseDir });
    let bridge;
    pi.events.emit('pi:agent-job-wait:v1', {
      context: ctx, accept: value => { bridge = value; },
    });
    const armed = await bridge.arm({ goalId, jobIds: ['first', 'second'], mode: 'all' });
    assert.equal(armed.ready, false);
    supervisor.publish({ eventId: 'first-result', jobId: 'first', eventType: 'terminal', executionState: 'succeeded', payload: { summary: 'first done' } });
    bridge.commit(armed.waitId);
    for (const handler of handlers.get('agent_settled') ?? []) handler({}, ctx);
    await new Promise(resolve => setTimeout(resolve, 30));
    assert.equal(sent.length, 0);
    supervisor.publish({ eventId: 'second-progress', jobId: 'second', eventType: 'progress', payload: { activity: 'still running' } });
    await new Promise(resolve => setTimeout(resolve, 30));
    assert.equal(sent.length, 0, 'progress during an all-wait must not wake the model');
    supervisor.publish({ eventId: 'second-result', jobId: 'second', eventType: 'terminal', executionState: 'succeeded', payload: { summary: 'second done' } });
    await until(() => sent.length === 1);
    assert.deepEqual(sent[0].message.details.mailboxEventIds, ['first-result', 'second-result']);
    assert.match(sent[0].message.content, /first done[\s\S]*second done/);
    assert.equal(sent[0].options.triggerTurn, true);
    branch.push({ id: 'grouped-history', type: 'custom_message', customType: 'subagent-result', details: sent[0].message.details });
    for (const handler of handlers.get('message_end') ?? []) handler({}, ctx);
    await until(() => ['first-result', 'second-result'].every(eventId =>
      supervisor.store.db.prepare('SELECT state FROM deliveries WHERE event_id=?').get(eventId)?.state === 'recorded'));
    branch.push({ id: 'replacement-goal', type: 'custom', customType: 'goal-state', data: { goal: { id: 'new-goal', status: 'active', waiting: { reason: 'new work' } } } });
    for (const handler of handlers.get('session_tree') ?? []) handler({ oldLeafId: null, newLeafId: null }, ctx);
    await until(() => supervisor.store.db.prepare('SELECT state FROM waits WHERE wait_id=?').get(armed.waitId)?.state === 'cancelled');
    supervisor.store.registerJob({ jobId: 'old-goal-job', coordinatorId: sessionId, workflowId, goalId });
    supervisor.publish({ eventId: 'old-goal-result', jobId: 'old-goal-job', eventType: 'terminal', executionState: 'succeeded', payload: { summary: 'late old result' } });
    await until(() => sent.length === 2);
    assert.equal(sent[1].options.triggerTurn, false, 'a result from an old goal must not wake its replacement');
    for (const handler of handlers.get('session_shutdown') ?? []) handler({}, ctx);
  } finally {
    client?.close();
    await supervisor?.close();
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-extension-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true });
  }
});
