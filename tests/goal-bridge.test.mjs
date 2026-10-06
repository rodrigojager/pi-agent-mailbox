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
  timeout: 90000,
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

test('user input during a mailbox goal wait resumes the goal without losing the later child result', {
  skip: (!existsSync(goalSource) || !existsSync(mockSource)) && 'Run beside a pi-goal source checkout',
  timeout: 90000,
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
    const context = createMockContext({ sessionManager: {
      getSessionId: () => sessionId, getBranch: branch, getEntries: branch,
      getLeafId: () => branch().at(-1)?.id ?? null, getSessionFile: () => null,
    } });
    goal(mock.pi, { settingsPath });
    mailbox(mock.pi, { baseDir });
    mock.pi.setActiveTools(['goal_complete', 'goal_blocked', 'goal_wait']);
    mock.pi.appendEntry('mailbox-workflow', { id: workflowId, sessionId });
    for (const handler of mock.events.get('session_start') ?? []) await handler({}, context.ctx);
    await mock.commands.get('goal').handler('Finish after the offline child', context.ctx);
    const goalId = branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal?.id;
    assert.ok(goalId);
    const jobId = randomUUID();
    supervisor.store.registerJob({ jobId, coordinatorId: sessionId, workflowId, goalId });
    const wait = await mock.tools.find(tool => tool.name === 'goal_wait').execute('goal-input-test', {
      goal_id: goalId, reason: 'Waiting for the offline child',
      subagents: { job_ids: [jobId], mode: 'all' },
    }, new AbortController().signal, () => undefined, context.ctx);
    assert.equal(wait.terminate, true);
    const waitId = branch().findLast(entry => entry.customType === 'mailbox-goal-wait')?.data?.waitId;
    assert.ok(waitId);
    for (const handler of mock.events.get('agent_settled') ?? []) await handler({}, context.ctx);
    assert.equal(supervisor.store.getWait(waitId).state, 'armed');

    for (const handler of mock.events.get('input') ?? []) {
      await handler({ source: 'interactive', text: 'Confira a situação enquanto o filho trabalha.' }, context.ctx);
    }
    const resumedGoal = branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal;
    assert.equal(resumedGoal?.id, goalId);
    assert.equal(resumedGoal?.status, 'active');
    assert.equal(resumedGoal?.waiting, undefined);
    assert.equal(mock.sentMessages.filter(item => item.message?.customType === 'subagent-result').length, 0);
    assert.equal(supervisor.store.getJob(jobId).state, 'registered');

    supervisor.publish({ eventId: 'result-after-user-input', jobId, eventType: 'terminal',
      executionState: 'succeeded', payload: { summary: 'offline child finished after user input' } });
    const deadline = Date.now() + 5000;
    while (mock.sentMessages.filter(item => item.message?.customType === 'subagent-result').length < 1 && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    const results = mock.sentMessages.filter(item => item.message?.customType === 'subagent-result');
    assert.equal(results.length, 1);
    assert.equal(results[0].options?.triggerTurn, true);
    assert.match(results[0].message.content, /finished after user input/);
    assert.equal(supervisor.store.getWait(waitId).state, 'cancelled', JSON.stringify({
      resultDetails: results[0].message.details,
      latestGoal: branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal,
      wait: supervisor.store.getWait(waitId),
    }));
    for (const handler of mock.events.get('session_shutdown') ?? []) await handler({}, context.ctx);
  } finally {
    await supervisor?.close();
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-goal-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true });
  }
});

test('real /goal pause and resume do not wake a newer goal with old mailbox results', {
  skip: (!existsSync(goalSource) || !existsSync(mockSource)) && 'Run beside a pi-goal source checkout',
  timeout: 90000,
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
    const resumedLateJobId = randomUUID();
    supervisor.store.registerJob({ jobId, coordinatorId: sessionId, workflowId, goalId });
    supervisor.store.registerJob({ jobId: resumedLateJobId, coordinatorId: sessionId, workflowId, goalId });
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
    await mock.commands.get('goal').handler('resume', context.ctx);
    const resumedGoal = branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal;
    assert.equal(resumedGoal?.status, 'active');
    assert.notEqual(resumedGoal.id, goalId);
    supervisor.publish({ eventId: 'late-old-goal-result', jobId: resumedLateJobId, eventType: 'terminal',
      executionState: 'succeeded', payload: { summary: 'offline child belongs to old goal' } });
    while (mock.sentMessages.filter(item => item.message?.customType === 'subagent-result').length < 2 && Date.now() < deadline + 5000) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    const allResults = mock.sentMessages.filter(item => item.message?.customType === 'subagent-result');
    assert.equal(allResults.length, 2);
    assert.equal(allResults[1].options?.triggerTurn, false);
    assert.match(allResults[1].message.content, /belongs to old goal/);
    for (const handler of mock.events.get('session_shutdown') ?? []) await handler({}, context.ctx);
  } finally {
    await supervisor?.close();
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-goal-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true });
  }
});

for (const transition of ['complete', 'blocked']) {
  test(`real pi-goal ${transition} keeps a late mailbox result from waking the model`, {
    skip: (!existsSync(goalSource) || !existsSync(mockSource)) && 'Run beside a pi-goal source checkout',
    timeout: 90000,
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
      await mock.commands.get('goal').handler(`Verify ${transition} fixture`, context.ctx);
      const goalId = branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal?.id;
      assert.ok(goalId);
      const jobId = randomUUID();
      supervisor.store.registerJob({ jobId, coordinatorId: sessionId, workflowId, goalId });
      const wait = await mock.tools.find(tool => tool.name === 'goal_wait').execute('goal-terminal-test', {
        goal_id: goalId, reason: 'Waiting for the offline child',
        subagents: { job_ids: [jobId], mode: 'all' },
      }, new AbortController().signal, () => undefined, context.ctx);
      assert.equal(wait.terminate, true);
      const params = transition === 'complete'
        ? { goal_id: goalId, summary: 'The monitored work is complete and verified.' }
        : {
            goal_id: goalId,
            reason: 'External system permanently rejected access',
            evidence: 'The same rejection was verified in three separate goal turns.',
            repeated_turns: 3,
          };
      const result = await mock.tools.find(tool => tool.name === `goal_${transition}`).execute(
        `goal-${transition}-test`, params, new AbortController().signal, () => undefined, context.ctx,
      );
      assert.equal(result.terminate, true, JSON.stringify(result));
      supervisor.publish({ eventId: `late-${transition}-result`, jobId, eventType: 'terminal',
        executionState: 'succeeded', payload: { summary: `child finished after ${transition}` } });
      const deadline = Date.now() + 5000;
      while (!mock.sentMessages.some(item => item.message?.customType === 'subagent-result') && Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      const delivered = mock.sentMessages.filter(item => item.message?.customType === 'subagent-result');
      assert.equal(delivered.length, 1);
      assert.equal(delivered[0].options?.triggerTurn, false);
      assert.match(delivered[0].message.content, new RegExp(`finished after ${transition}`));
      for (const handler of mock.events.get('session_shutdown') ?? []) await handler({}, context.ctx);
    } finally {
      await supervisor?.close();
      if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
      if (!basename(target).startsWith('pi-mailbox-goal-test-')) throw new Error('Unexpected fixture name');
      rmSync(target, { recursive: true, force: true });
    }
  });
}

for (const limit of ['usage_limited', 'budget_limited']) {
  test(`real pi-goal ${limit} records a late result without waking the model`, {
    skip: (!existsSync(goalSource) || !existsSync(mockSource)) && 'Run beside a pi-goal source checkout',
    timeout: 90000,
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
      const branch = () => mock.entries.map((entry, index) => ({ id: `entry-${index}`, ...entry, type: entry.type ?? 'custom' }));
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
      await mock.commands.get('goal').handler(`${limit === 'budget_limited' ? '--tokens 1000 ' : ''}Verify ${limit} fixture`, context.ctx);
      const goalId = branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal?.id;
      assert.ok(goalId);
      const prompt = mock.sentUserMessages.at(-1)?.text;
      assert.ok(prompt);
      for (const handler of mock.events.get('before_agent_start') ?? []) {
        await handler({ prompt, systemPrompt: 'fixture' }, context.ctx);
      }
      const jobId = randomUUID();
      supervisor.store.registerJob({ jobId, coordinatorId: sessionId, workflowId, goalId });
      const wait = await mock.tools.find(tool => tool.name === 'goal_wait').execute('goal-limit-test', {
        goal_id: goalId, reason: 'Waiting for the offline child',
        subagents: { job_ids: [jobId], mode: 'all' },
      }, new AbortController().signal, () => undefined, context.ctx);
      assert.equal(wait.terminate, true);
      if (limit === 'budget_limited') {
        mock.entries.push({ type: 'message', message: { role: 'assistant', usage: { totalTokens: 1000 } } });
      }
      const assistant = limit === 'usage_limited'
        ? { role: 'assistant', stopReason: 'error', errorMessage: 'ChatGPT usage limit reached', content: [] }
        : { role: 'assistant', stopReason: 'stop', content: [] };
      for (const handler of mock.events.get('agent_end') ?? []) {
        await handler({ messages: [assistant] }, context.ctx);
      }
      const stoppedGoal = branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal;
      assert.equal(stoppedGoal?.status, limit);
      supervisor.publish({ eventId: `late-${limit}-result`, jobId, eventType: 'terminal',
        executionState: 'succeeded', payload: { summary: `child finished after ${limit}` } });
      const deadline = Date.now() + 5000;
      while (!mock.sentMessages.some(item => item.message?.customType === 'subagent-result') && Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      const delivered = mock.sentMessages.filter(item => item.message?.customType === 'subagent-result');
      assert.equal(delivered.length, 1);
      assert.equal(delivered[0].options?.triggerTurn, false);
      assert.match(delivered[0].message.content, new RegExp(`finished after ${limit}`));
      for (const handler of mock.events.get('session_shutdown') ?? []) await handler({}, context.ctx);
    } finally {
      await supervisor?.close();
      if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
      if (!basename(target).startsWith('pi-mailbox-goal-test-')) throw new Error('Unexpected fixture name');
      rmSync(target, { recursive: true, force: true });
    }
  });
}

test('real pi-goal pauses a waiting goal when profile tools disappear at mailbox wake', {
  skip: (!existsSync(goalSource) || !existsSync(mockSource)) && 'Run beside a pi-goal source checkout',
  timeout: 90000,
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
    await mock.commands.get('goal').handler('Verify profile tool loss', context.ctx);
    const goalId = branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal?.id;
    assert.ok(goalId);
    const jobId = randomUUID();
    supervisor.store.registerJob({ jobId, coordinatorId: sessionId, workflowId, goalId });
    const wait = await mock.tools.find(tool => tool.name === 'goal_wait').execute('goal-no-tools-test', {
      goal_id: goalId, reason: 'Waiting for the offline child',
      subagents: { job_ids: [jobId], mode: 'all' },
    }, new AbortController().signal, () => undefined, context.ctx);
    assert.equal(wait.terminate, true);
    mock.pi.setActiveTools([]);
    for (const handler of mock.events.get('agent_settled') ?? []) await handler({}, context.ctx);
    supervisor.publish({ eventId: 'no-tools-result', jobId, eventType: 'terminal',
      executionState: 'succeeded', payload: { summary: 'child finished after profile switch' } });
    const deadline = Date.now() + 5000;
    while (!mock.sentMessages.some(item => item.message?.customType === 'subagent-result') && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    const delivered = mock.sentMessages.filter(item => item.message?.customType === 'subagent-result');
    assert.equal(delivered.length, 1);
    assert.equal(delivered[0].options?.triggerTurn, true);
    for (const handler of mock.events.get('before_agent_start') ?? []) {
      await handler({ prompt: delivered[0].message.content, systemPrompt: 'fixture' }, context.ctx);
    }
    assert.equal(branch().findLast(entry => entry.customType === 'goal-state')?.data?.goal?.status, 'paused');
    assert.equal(mock.sentUserMessages.length, 1);
    for (const handler of mock.events.get('session_shutdown') ?? []) await handler({}, context.ctx);
  } finally {
    await supervisor?.close();
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-goal-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true });
  }
});
