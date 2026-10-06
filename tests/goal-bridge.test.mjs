import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, basename } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createJiti } from 'jiti';
import { createSupervisor } from '../src/supervisor.mjs';
import { ensureToken } from '../src/client.mjs';

const goalRoot = resolve(import.meta.dirname, '../../pi-goal-rodrigo');
const goalSource = join(goalRoot, 'src/goal.ts');
const mockSource = join(goalRoot, 'test/support.ts');
const jiti = createJiti(import.meta.url);

test('real pi-goal goal_wait arms the mailbox bridge and one completion wakes the owned goal', {
  skip: (!existsSync(goalSource) || !existsSync(mockSource)) && 'Run beside a pi-goal source checkout',
  timeout: 30000,
}, async () => {
  const baseDir = mkdtempSync(join(tmpdir(), 'pi-mailbox-goal-test-'));
  const target = resolve(baseDir);
  const sessionId = randomUUID();
  const workflowId = randomUUID();
  const settingsPath = join(baseDir, 'goal-settings.json');
  writeFileSync(settingsPath, '{}\n');
  let supervisor;
  try {
    const [{ default: goal }, { createMockPi, createMockContext }, { default: mailbox }] = await Promise.all([
      jiti.import(goalSource), jiti.import(mockSource), jiti.import('../src/index.ts'),
    ]);
    ensureToken(sessionId, baseDir);
    supervisor = createSupervisor({ sessionId, baseDir });
    await supervisor.listen();
    const mock = createMockPi();
    const branch = () => mock.entries.map((entry, index) => ({ ...entry, type: 'custom', id: `entry-${index}` }));
    const context = createMockContext({
      sessionManager: {
        getSessionId: () => sessionId,
        getBranch: branch,
        getEntries: branch,
        getLeafId: () => branch().at(-1)?.id ?? null,
        getSessionFile: () => null,
      },
    });
    goal(mock.pi, { settingsPath });
    mailbox(mock.pi, { baseDir });
    mock.pi.setActiveTools(['goal_complete', 'goal_blocked', 'goal_wait']);
    mock.pi.appendEntry('mailbox-workflow', { id: workflowId, sessionId });
    for (const handler of mock.events.get('session_start') ?? []) await handler({}, context.ctx);
    await mock.commands.get('goal').handler('Complete the offline fixture', context.ctx);
    const latest = branch().findLast(entry => entry.customType === 'goal-state');
    const goalId = latest?.data?.goal?.id;
    assert.ok(goalId, JSON.stringify({ entries: mock.entries, notifications: context.notifications, userMessages: mock.sentUserMessages }));
    const jobId = randomUUID();
    supervisor.store.registerJob({ jobId, coordinatorId: sessionId, workflowId, goalId });
    const waitTool = mock.tools.find(tool => tool.name === 'goal_wait');
    assert.ok(waitTool);
    const wait = await waitTool.execute('goal-bridge-test', {
      goal_id: goalId, reason: 'Waiting for the offline child',
      subagents: { job_ids: [jobId], mode: 'all' },
    }, new AbortController().signal, () => undefined, context.ctx);
    assert.equal(wait.terminate, true);
    assert.equal(branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal?.waiting?.reason, 'Waiting for the offline child');
    supervisor.publish({ eventId: 'goal-result', jobId, eventType: 'terminal', executionState: 'succeeded', payload: { summary: 'offline child done' } });
    for (const handler of mock.events.get('agent_settled') ?? []) await handler({}, context.ctx);
    const deadline = Date.now() + 5000;
    while (!mock.sentMessages.some(item => item.message?.customType === 'subagent-result') && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    const result = mock.sentMessages.find(item => item.message?.customType === 'subagent-result');
    assert.ok(result);
    assert.equal(result.options?.triggerTurn, true);
    assert.match(result.message.content, /offline child done/);
    for (const handler of mock.events.get('session_shutdown') ?? []) await handler({}, context.ctx);
  } finally {
    await supervisor?.close();
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-goal-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true });
  }
});

test('real /goal pause delivers a late mailbox result without resuming the model', {
  skip: (!existsSync(goalSource) || !existsSync(mockSource)) && 'Run beside a pi-goal source checkout',
  timeout: 30000,
}, async () => {
  const baseDir = mkdtempSync(join(tmpdir(), 'pi-mailbox-goal-test-'));
  const target = resolve(baseDir);
  const sessionId = randomUUID();
  const workflowId = randomUUID();
  const settingsPath = join(baseDir, 'goal-settings.json');
  writeFileSync(settingsPath, '{}\n');
  let supervisor;
  try {
    const [{ default: goal }, { createMockPi, createMockContext }, { default: mailbox }] = await Promise.all([
      jiti.import(goalSource), jiti.import(mockSource), jiti.import('../src/index.ts'),
    ]);
    ensureToken(sessionId, baseDir);
    supervisor = createSupervisor({ sessionId, baseDir });
    await supervisor.listen();
    const mock = createMockPi();
    const branch = () => mock.entries.map((entry, index) => ({ ...entry, type: 'custom', id: `entry-${index}` }));
    const context = createMockContext({
      sessionManager: {
        getSessionId: () => sessionId, getBranch: branch, getEntries: branch,
        getLeafId: () => branch().at(-1)?.id ?? null, getSessionFile: () => null,
      },
    });
    goal(mock.pi, { settingsPath });
    mailbox(mock.pi, { baseDir });
    mock.pi.setActiveTools(['goal_complete', 'goal_blocked', 'goal_wait']);
    mock.pi.appendEntry('mailbox-workflow', { id: workflowId, sessionId });
    for (const handler of mock.events.get('session_start') ?? []) await handler({}, context.ctx);
    await mock.commands.get('goal').handler('Complete the paused fixture', context.ctx);
    const goalId = branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal?.id;
    assert.ok(goalId);
    const jobId = randomUUID();
    supervisor.store.registerJob({ jobId, coordinatorId: sessionId, workflowId, goalId });
    const waitTool = mock.tools.find(tool => tool.name === 'goal_wait');
    const wait = await waitTool.execute('goal-pause-test', {
      goal_id: goalId, reason: 'Waiting for the offline child',
      subagents: { job_ids: [jobId], mode: 'all' },
    }, new AbortController().signal, () => undefined, context.ctx);
    assert.equal(wait.terminate, true);
    await mock.commands.get('goal').handler('pause', context.ctx);
    assert.equal(branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal?.status, 'paused');
    supervisor.publish({ eventId: 'late-paused-result', jobId, eventType: 'terminal', executionState: 'succeeded',
      payload: { summary: 'offline child finished after pause' } });
    const deadline = Date.now() + 5000;
    while (!mock.sentMessages.some(item => item.message?.customType === 'subagent-result') && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    const results = mock.sentMessages.filter(item => item.message?.customType === 'subagent-result');
    assert.equal(results.length, 1);
    assert.equal(results[0].options?.triggerTurn, false);
    assert.match(results[0].message.content, /finished after pause/);
    for (const handler of mock.events.get('session_shutdown') ?? []) await handler({}, context.ctx);
  } finally {
    await supervisor?.close();
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-goal-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true });
  }
});
