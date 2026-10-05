import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, basename } from 'node:path';
import { pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { createJiti } from 'jiti';
import { createSupervisor } from '../src/supervisor.mjs';
import { ensureToken } from '../src/client.mjs';

const jiti = createJiti(import.meta.url);
const { default: registerMailbox } = await jiti.import('../src/index.ts');
const sessionManagerPath = resolve(import.meta.dirname, '../node_modules/@earendil-works/pi-coding-agent/dist/core/session-manager.js');
const { SessionManager } = await import(pathToFileURL(sessionManagerPath).href);

async function until(check, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for compacted mailbox result');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}

test('a compacted Pi session keeps its workflow and records a pending result', async () => {
  const baseDir = mkdtempSync(join(tmpdir(), 'pi-mailbox-compaction-test-'));
  const target = resolve(baseDir);
  const sessionManager = SessionManager.inMemory(baseDir);
  const sessionId = sessionManager.getSessionId();
  const workflowId = randomUUID();
  let supervisor;
  try {
    sessionManager.appendCustomEntry('mailbox-workflow', { id: workflowId, sessionId });
    sessionManager.appendCustomMessageEntry('fixture', 'Earlier conversation context', true);
    const compactId = sessionManager.appendCompaction('Earlier context summarized', null, 100);
    assert.equal(sessionManager.getBranch().at(-1)?.id, compactId);
    assert.equal(sessionManager.getBranch().some(entry =>
      entry.type === 'custom' && entry.customType === 'mailbox-workflow' && entry.data?.id === workflowId), true);

    ensureToken(sessionId, baseDir);
    supervisor = createSupervisor({ sessionId, baseDir });
    await supervisor.listen();
    supervisor.store.registerJob({ jobId: 'compacted-job', coordinatorId: sessionId, workflowId });
    supervisor.publish({ eventId: 'compacted-result', jobId: 'compacted-job', eventType: 'terminal', executionState: 'succeeded', payload: { summary: 'result after compaction' } });

    const handlers = new Map();
    const sent = [];
    const ctx = { hasUI: false, ui: { setStatus() {}, notify() {} }, sessionManager };
    const pi = {
      events: new EventEmitter(),
      on(name, handler) { handlers.set(name, [...(handlers.get(name) ?? []), handler]); },
      appendEntry(customType, data) { sessionManager.appendCustomEntry(customType, data); },
      sendMessage(message, options) { sent.push({ message, options }); },
      registerCommand() {}, registerTool() {},
    };
    registerMailbox(pi, { baseDir });
    for (const handler of handlers.get('session_start') ?? []) handler({}, ctx);
    await until(() => sent.length === 1);
    assert.equal(sent[0].message.details.mailboxEventId, 'compacted-result');
    assert.match(sent[0].message.content, /result after compaction/);
    assert.equal(supervisor.store.db.prepare('SELECT state FROM deliveries WHERE event_id=?').get('compacted-result').state, 'pending');
    const historyId = sessionManager.appendCustomMessageEntry('subagent-result', sent[0].message.content, true, sent[0].message.details);
    for (const handler of handlers.get('message_end') ?? []) handler({}, ctx);
    await until(() => supervisor.store.db.prepare('SELECT state FROM deliveries WHERE event_id=?').get('compacted-result')?.state === 'recorded');
    assert.equal(supervisor.store.db.prepare('SELECT entry_id FROM deliveries WHERE event_id=?').get('compacted-result').entry_id, historyId);
    for (const handler of handlers.get('session_shutdown') ?? []) handler({}, ctx);
  } finally {
    await supervisor?.close();
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-compaction-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true });
  }
});
