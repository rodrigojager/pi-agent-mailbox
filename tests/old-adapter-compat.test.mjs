import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, basename } from 'node:path';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { createJiti } from 'jiti';
import { statePath } from '../src/protocol.mjs';

const oldSubagentRoot = process.env.PI_MAILBOX_OLD_SUBAGENT_ROOT
  ?? resolve(import.meta.dirname, '../../../../../.pi/agent/local/pi-subagent-v0.13.0-rodrigo.1');
const oldSource = join(oldSubagentRoot, 'src/index.ts');
const goalMockSource = resolve(import.meta.dirname, '../../pi-goal-rodrigo/test/support.ts');
const jiti = createJiti(import.meta.url);

test('the old pi-subagent adapter still completes a legacy job beside the mailbox', {
  skip: (!existsSync(oldSource) || !existsSync(goalMockSource)) && 'Requires the old adapter and Pi mock',
  timeout: 60000,
}, async () => {
  const baseDir = mkdtempSync(join(tmpdir(), 'pi-mailbox-old-adapter-test-'));
  const target = resolve(baseDir);
  const agentDir = join(baseDir, 'agent');
  const agentsDir = join(agentDir, 'agents');
  mkdirSync(agentsDir, { recursive: true });
  writeFileSync(join(agentsDir, 'fixture.md'), [
    '---',
    'name: fixture',
    'description: Offline compatibility fixture',
    'provider: fixture',
    'model: fixture-model',
    'thinking: off',
    'tools: complete',
    'skills: false',
    'extensions: false',
    '---',
    'Return the fixture result.',
    '',
  ].join('\n'));
  const sessionId = randomUUID();
  const fakePi = fileURLToPath(new URL('./fake-pi-cli.mjs', import.meta.url));
  const oldAgentDir = process.env.PI_CODING_AGENT_DIR;
  const oldArgv1 = process.argv[1];
  process.env.PI_CODING_AGENT_DIR = agentDir;
  process.argv[1] = fakePi;
  let mock;
  let context;
  try {
    const [{ default: oldSubagent }, { default: mailbox }, { createMockPi, createMockContext }] = await Promise.all([
      jiti.import(oldSource), jiti.import('../src/index.ts'), jiti.import(goalMockSource),
    ]);
    mock = createMockPi();
    const branch = () => mock.entries.map((entry, index) => ({ ...entry, type: 'custom', id: `entry-${index}` }));
    context = createMockContext({
      cwd: baseDir,
      hasUI: false,
      model: { provider: 'fixture', id: 'fixture-model' },
      modelRegistry: { find: () => undefined, getAvailable: () => [], getAll: () => [] },
      sessionManager: {
        getSessionId: () => sessionId, getBranch: branch, getEntries: branch,
        getLeafId: () => branch().at(-1)?.id ?? null, getSessionFile: () => null,
      },
    });
    mailbox(mock.pi, { baseDir });
    oldSubagent(mock.pi);
    for (const handler of mock.events.get('session_start') ?? []) await handler({}, context.ctx);
    const tool = mock.tools.find(item => item.name === 'subagent');
    assert.ok(tool);
    const started = await tool.execute('old-adapter-compat', {
      agent: 'fixture', task: 'Return an offline result', agentScope: 'user', role: 'none',
    }, new AbortController().signal, () => undefined, context.ctx);
    assert.match(JSON.stringify(started), /started|fixture/i);
    const deadline = Date.now() + 25000;
    while (!mock.sentMessages.some(item => item.message?.customType === 'subagent-result') && Date.now() < deadline) {
      await new Promise(resolveDelay => setTimeout(resolveDelay, 20));
    }
    const results = mock.sentMessages.filter(item => item.message?.customType === 'subagent-result');
    assert.equal(results.length, 1, JSON.stringify(mock.sentMessages));
    assert.match(results[0].message.content, /offline child completed/);
    assert.equal(existsSync(join(statePath(baseDir, sessionId), 'journal.sqlite')), false);
    for (const handler of mock.events.get('session_shutdown') ?? []) await handler({}, context.ctx);
  } finally {
    if (mock && context) {
      try { await mock.commands.get('cancel-subagent')?.handler('all', context.ctx); } catch { /* Already terminal. */ }
    }
    process.argv[1] = oldArgv1;
    if (oldAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = oldAgentDir;
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-old-adapter-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true, maxRetries: 50, retryDelay: 100 });
  }
});
