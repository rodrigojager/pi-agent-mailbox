import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, basename, dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { spawn } from 'node:child_process';
import { createSupervisor } from '../src/supervisor.mjs';
import { MailboxClient, ensureToken } from '../src/client.mjs';
import { MailboxStore } from '../src/store.mjs';
import { statePath } from '../src/protocol.mjs';
import { waitForSubagents } from '../src/wait.mjs';

function fixture() {
  const baseDir = mkdtempSync(join(tmpdir(), 'pi-mailbox-test-'));
  const sessionId = randomUUID();
  const cleanup = () => {
    const target = resolve(baseDir);
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) {
      throw new Error('Refusing to delete outside the temp directory');
    }
    if (!basename(target).startsWith('pi-mailbox-test-')) throw new Error('Unexpected fixture directory');
    rmSync(target, { recursive: true, force: true });
  };
  return { baseDir, sessionId, cleanup };
}

test('terminal event and pending delivery survive a supervisor restart', async () => {
  const f = fixture();
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    let supervisor = createSupervisor(f);
    await supervisor.listen();
    let client = await MailboxClient.connect({ ...f, token });
    await client.request('register', { job: { jobId: 'job-1', coordinatorId: 'root', workflowId: 'workflow-1' } });
    const event = { eventId: 'event-1', jobId: 'job-1', eventType: 'completed', executionState: 'succeeded', payload: { text: 'done' } };
    const first = await client.request('publish', { event });
    assert.equal(first.seq, 1);
    assert.equal(first.duplicate, false);
    assert.equal((await client.request('publish', { event })).duplicate, true);
    assert.equal((await client.request('job', { jobId: 'job-1' })).state, 'succeeded');
    client.close();
    await supervisor.close();

    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const seen = [];
    const unsubscribe = await client.subscribe(0, item => seen.push(item));
    assert.deepEqual(seen.map(item => item.event_id), ['event-1']);
    assert.equal((await client.request('ack', { eventId: 'event-1', entryId: 'entry-7' })).state, 'recorded');
    assert.equal((await client.request('events', { after: 1 })).length, 0);
    unsubscribe();
    client.close();
    await supervisor.close();
    const db = new MailboxStore(join(statePath(f.baseDir, f.sessionId), 'journal.sqlite'));
    assert.equal(db.db.prepare('SELECT entry_id FROM deliveries WHERE event_id=?').get('event-1').entry_id, 'entry-7');
    db.close();
  } finally { f.cleanup(); }
});

test('subscription sees results published after its replay and rejects wrong identity', async () => {
  const f = fixture();
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    const supervisor = createSupervisor(f);
    await supervisor.listen();
    await assert.rejects(MailboxClient.connect({ ...f, token: 'bad' }), /handshake|closed/i);
    const producer = await MailboxClient.connect({ ...f, token });
    const consumer = await MailboxClient.connect({ ...f, token });
    assert.equal(producer.isOwner, true);
    assert.equal(consumer.isOwner, false);
    await assert.rejects(consumer.request('register', { job: { jobId: 'observer-job', coordinatorId: 'root', workflowId: 'workflow-2' } }), /observer/);
    const events = [];
    const done = new Promise(resolve => {
      consumer.subscribe(0, event => { events.push(event); resolve(); }).catch(resolve);
    });
    await producer.request('register', { job: { jobId: 'job-2', coordinatorId: 'root', workflowId: 'workflow-2' } });
    await producer.request('publish', { event: { eventId: 'event-2', jobId: 'job-2', eventType: 'failed', executionState: 'failed', payload: { error: 'worker exited' } } });
    await done;
    assert.deepEqual(events.map(item => item.event_id), ['event-2']);
    producer.close();
    for (let attempt = 0; attempt < 20 && !(await consumer.claim()).owner; attempt++) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.equal(consumer.isOwner, true);
    await consumer.request('register', { job: { jobId: 'observer-job', coordinatorId: 'root', workflowId: 'workflow-2' } });
    consumer.close();
    await supervisor.close();
  } finally { f.cleanup(); }
});

test('a detached worker completes while the Pi client is disconnected', async () => {
  const f = fixture();
  let supervisor;
  let client;
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const adapterPath = fileURLToPath(new URL('./fake-adapter.mjs', import.meta.url));
    const started = await client.request('start', {
      job: { jobId: 'detached-1', coordinatorId: 'root', workflowId: 'workflow', delayMs: 60 },
      adapterPath,
    });
    assert.equal(started.duplicate, false);
    client.close();
    client = undefined;
    await new Promise(resolve => setTimeout(resolve, 60));
    client = await MailboxClient.connect({ ...f, token });
    const final = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Worker did not publish a terminal event')), 8000);
      client.subscribe(0, event => {
        if (event.event_type === 'terminal' || event.event_type === 'worker_exit') {
          clearTimeout(timer);
          resolve(event);
        }
      }).catch(reject);
    });
    const terminal = await final;
    assert.equal(terminal.payload.summary, 'finished detached-1');
    const events = await client.request('events', { after: 0 });
    assert.equal(events.filter(item => item.event_type === 'terminal').length, 1);
    assert.equal((await client.request('artifact', { jobId: 'detached-1' })).summary, 'finished detached-1');
    assert.equal((await client.request('start', {
      job: { jobId: 'detached-1', coordinatorId: 'root', workflowId: 'workflow' }, adapterPath,
    })).duplicate, true);
  } finally {
    client?.close();
    await supervisor?.close();
    f.cleanup();
  }
});

test('any and all waits arm before completion and wake on errors', async () => {
  const f = fixture();
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    const supervisor = createSupervisor(f);
    await supervisor.listen();
    const client = await MailboxClient.connect({ ...f, token });
    for (const jobId of ['a', 'b']) {
      await client.request('register', { job: { jobId, coordinatorId: 'root', workflowId: 'flow' } });
    }
    const shared = { coordinatorId: 'root', workflowId: 'flow', jobIds: ['a', 'b'] };
    assert.equal((await client.request('watch', { wait: { ...shared, waitId: 'any', mode: 'any' } })).ready, false);
    assert.equal((await client.request('watch', { wait: { ...shared, waitId: 'all', mode: 'all' } })).ready, false);
    await client.request('publish', { event: { eventId: 'a-done', jobId: 'a', eventType: 'terminal', executionState: 'succeeded' } });
    assert.equal((await client.request('wait_status', { waitId: 'any' })).ready, true);
    assert.equal((await client.request('wait_status', { waitId: 'all' })).ready, false);
    await client.request('publish', { event: { eventId: 'b-fail', jobId: 'b', eventType: 'terminal', executionState: 'failed' } });
    const all = await client.request('wait_status', { waitId: 'all' });
    assert.equal(all.ready, true);
    assert.deepEqual(all.statuses, { a: 'succeeded', b: 'failed' });
    await assert.rejects(client.request('watch', { wait: { ...shared, waitId: 'foreign', mode: 'all', coordinatorId: 'other' } }), /does not belong/);
    client.close();
    await supervisor.close();
  } finally { f.cleanup(); }
});

test('event-driven wait returns without repeated status queries', async () => {
  const f = fixture();
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    const supervisor = createSupervisor(f);
    await supervisor.listen();
    const client = await MailboxClient.connect({ ...f, token });
    await client.request('register', { job: { jobId: 'wait-job', coordinatorId: 'root', workflowId: 'flow' } });
    let checks = 0;
    const request = client.request.bind(client);
    client.request = (...args) => { if (args[0] === 'wait_status') checks++; return request(...args); };
    const pending = waitForSubagents(client, {
      coordinatorId: 'root', workflowId: 'flow', jobIds: ['wait-job'], mode: 'all', timeoutMs: 2000,
    });
    await new Promise(resolve => setTimeout(resolve, 80));
    assert.equal(checks, 0);
    await client.request('publish', { event: { eventId: 'wait-done', jobId: 'wait-job', eventType: 'terminal', executionState: 'succeeded' } });
    const result = await pending;
    assert.equal(result.ready, true);
    assert.deepEqual(result.statuses, { 'wait-job': 'succeeded' });
    assert.equal(checks, 1);
    client.close();
    await supervisor.close();
  } finally { f.cleanup(); }
});

test('a claimed start survives restart without a second execution', async () => {
  const f = fixture();
  let supervisor;
  let client;
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const job = { jobId: 'claimed-job', coordinatorId: 'root', workflowId: 'flow' };
    await client.request('register', { job });
    assert.equal(supervisor.store.claimStart(job.jobId), true);
    client.close();
    await supervisor.close();
    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const adapterPath = fileURLToPath(new URL('./fake-adapter.mjs', import.meta.url));
    const repeated = await client.request('start', { job, adapterPath });
    assert.equal(repeated.duplicate, true);
    assert.equal(repeated.job.state, 'unknown');
    assert.equal(repeated.job.worker_pid, null);
    assert.equal((await client.request('events', { after: 0 })).filter(e => e.event_type === 'supervision_lost').length, 1);
  } finally {
    client?.close();
    await supervisor?.close();
    f.cleanup();
  }
});

test('a committed worker artifact is recovered after supervisor loss without relaunching the job', async () => {
  const f = fixture();
  let supervisor;
  let client;
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const job = { jobId: 'artifact-after-crash', coordinatorId: 'root', workflowId: 'flow' };
    await client.request('register', { job });
    assert.equal(supervisor.store.claimStart(job.jobId), true);
    client.close();
    await supervisor.close();
    const artifact = join(statePath(f.baseDir, f.sessionId), 'artifacts', `${job.jobId}.json`);
    writeFileSync(artifact, JSON.stringify({ state: 'succeeded', summary: 'recovered from disk' }));

    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const events = [];
    await client.subscribe(0, event => events.push(event));
    assert.deepEqual(events.map(event => event.event_type), ['terminal']);
    assert.equal(events[0].payload.summary, 'recovered from disk');
    assert.equal((await client.request('job', { jobId: job.jobId })).state, 'succeeded');
    const adapterPath = fileURLToPath(new URL('./fake-adapter.mjs', import.meta.url));
    assert.equal((await client.request('start', { job, adapterPath })).duplicate, true);
    assert.equal((await client.request('events', { after: 0 })).length, 1);
  } finally {
    client?.close();
    await supervisor?.close();
    f.cleanup();
  }
});

test('a corrupt result artifact leaves execution indeterminate instead of reporting success', async () => {
  const f = fixture();
  let supervisor;
  let client;
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const job = { jobId: 'corrupt-result', coordinatorId: 'root', workflowId: 'flow' };
    await client.request('register', { job });
    assert.equal(supervisor.store.claimStart(job.jobId), true);
    client.close();
    await supervisor.close();
    writeFileSync(join(statePath(f.baseDir, f.sessionId), 'artifacts', `${job.jobId}.json`), '{invalid-json');

    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const events = await client.request('events', { after: 0 });
    assert.deepEqual(events.map(event => event.event_type), ['supervision_lost']);
    assert.equal((await client.request('job', { jobId: job.jobId })).state, 'unknown');
  } finally {
    client?.close();
    await supervisor?.close();
    f.cleanup();
  }
});

test('a worker result survives an abrupt supervisor exit on Windows', {
  skip: process.platform !== 'win32', timeout: 20000,
}, async () => {
  const f = fixture();
  const token = ensureToken(f.sessionId, f.baseDir);
  const supervisorPath = fileURLToPath(new URL('../src/supervisor.mjs', import.meta.url));
  const child = spawn(process.execPath, [supervisorPath, f.sessionId, f.baseDir], {
    stdio: 'ignore', windowsHide: true,
  });
  let client;
  let recovered;
  try {
    const deadline = Date.now() + 5000;
    while (!client && Date.now() < deadline) {
      try { client = await MailboxClient.connect({ ...f, token, timeoutMs: 300 }); }
      catch { await new Promise(resolve => setTimeout(resolve, 20)); }
    }
    assert.ok(client, 'isolated supervisor did not start');
    const jobId = 'survive-parent-exit';
    const progress = new Promise((resolveProgress, reject) => {
      const timer = setTimeout(() => reject(new Error('Worker did not start')), 5000);
      client.subscribe(0, event => {
        if (event.event_type === 'progress' && event.job_id === jobId) {
          clearTimeout(timer); resolveProgress();
        }
      }).catch(reject);
    });
    const adapterPath = fileURLToPath(new URL('./fake-adapter.mjs', import.meta.url));
    await client.request('start', {
      job: { jobId, coordinatorId: 'root', workflowId: 'flow', delayMs: 1800 }, adapterPath,
    });
    await progress;
    child.kill();
    await new Promise((resolveExit, reject) => {
      const timer = setTimeout(() => reject(new Error('Isolated supervisor did not exit')), 5000);
      child.once('exit', () => { clearTimeout(timer); resolveExit(); });
    });
    client.close();
    client = undefined;

    recovered = createSupervisor(f);
    await recovered.listen();
    client = await MailboxClient.connect({ ...f, token });
    const events = [];
    const terminal = new Promise((resolveTerminal, reject) => {
      const timer = setTimeout(() => reject(new Error('Detached worker result was not recovered')), 10000);
      client.subscribe(0, event => {
        events.push(event);
        if (event.event_type === 'terminal' && event.job_id === jobId) {
          clearTimeout(timer); resolveTerminal(event);
        }
      }).catch(reject);
    });
    assert.equal((await terminal).payload.state, 'succeeded');
    assert.equal(events.filter(event => event.event_type === 'terminal' && event.job_id === jobId).length, 1);
    assert.equal((await client.request('job', { jobId })).state, 'succeeded');
  } finally {
    client?.close();
    await recovered?.close();
    if (child.exitCode === null) child.kill();
    f.cleanup();
  }
});

test('schema v1 migrates once and an incompatible schema is refused', () => {
  const f = fixture();
  try {
    const filename = join(statePath(f.baseDir, f.sessionId), 'journal.sqlite');
    mkdirSync(dirname(filename), { recursive: true });
    const old = new DatabaseSync(filename);
    old.exec(`CREATE TABLE schema_meta (version INTEGER NOT NULL); INSERT INTO schema_meta VALUES (1);
      CREATE TABLE jobs (job_id TEXT PRIMARY KEY, coordinator_id TEXT NOT NULL,
        workflow_id TEXT NOT NULL, state TEXT NOT NULL, worker_pid INTEGER,
        worker_started_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);`);
    old.close();
    const migrated = new MailboxStore(filename);
    assert.equal(migrated.db.prepare('SELECT version FROM schema_meta').get().version, 2);
    assert.ok(migrated.db.prepare('PRAGMA table_info(jobs)').all().some(column => column.name === 'goal_id'));
    migrated.close();
    const newer = new DatabaseSync(filename);
    newer.exec('UPDATE schema_meta SET version=99');
    newer.close();
    assert.throws(() => new MailboxStore(filename), /Unsupported mailbox schema/);
  } finally { f.cleanup(); }
});

test('subscription replays a backlog beyond one page exactly once', async () => {
  const f = fixture();
  let supervisor;
  let client;
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    await client.request('register', { job: { jobId: 'backlog', coordinatorId: 'root', workflowId: 'flow' } });
    for (let index = 0; index < 1105; index++) {
      await client.request('publish', { event: { eventId: `progress-${index}`, jobId: 'backlog', eventType: 'progress', payload: { index, text: 'X'.repeat(1024) } } });
    }
    const seen = [];
    const unsubscribe = await client.subscribe(0, event => seen.push(event));
    assert.equal(seen.length, 1105);
    assert.equal(new Set(seen.map(event => event.event_id)).size, 1105);
    assert.equal(seen.at(-1).event_id, 'progress-1104');
    unsubscribe();
  } finally {
    client?.close();
    await supervisor?.close();
    f.cleanup();
  }
});

test('a large result stays intact in its artifact while the event frame remains bounded', async () => {
  const f = fixture();
  let supervisor;
  let client;
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const adapterPath = fileURLToPath(new URL('./fake-adapter.mjs', import.meta.url));
    await client.request('start', {
      job: { jobId: 'large', coordinatorId: 'root', workflowId: 'flow', summaryLength: 100000 }, adapterPath,
    });
    const terminal = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Large result did not complete')), 8000);
      client.subscribe(0, event => {
        if (event.event_type === 'terminal') { clearTimeout(timeout); resolve(event); }
      }).catch(reject);
    });
    assert.equal(terminal.payload.summary.length, 8192);
    const serialized = readFileSync(terminal.payload.resultRef, 'utf8');
    assert.equal(JSON.parse(serialized).summary.length, 100000);
    assert.equal(terminal.payload.resultSha256, createHash('sha256').update(serialized).digest('hex'));
  } finally {
    client?.close();
    await supervisor?.close();
    f.cleanup();
  }
});

test('explicit cancellation aborts a running worker and publishes a terminal outcome', async () => {
  const f = fixture();
  let supervisor;
  let client;
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    supervisor = createSupervisor(f);
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const adapterPath = fileURLToPath(new URL('./fake-adapter.mjs', import.meta.url));
    await client.request('start', {
      job: { jobId: 'cancel-me', coordinatorId: 'root', workflowId: 'flow', delayMs: 5000 }, adapterPath,
    });
    const terminal = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Cancellation did not finish')), 8000);
      client.subscribe(0, event => {
        if (event.event_type === 'terminal') { clearTimeout(timeout); resolve(event); }
      }).catch(reject);
    });
    await client.request('cancel', { jobId: 'cancel-me' });
    assert.equal((await terminal).payload.state, 'cancelled');
    assert.equal((await client.request('job', { jobId: 'cancel-me' })).state, 'cancelled');
  } finally {
    client?.close();
    await supervisor?.close();
    f.cleanup();
  }
});

test('an idle supervisor can exit only after disconnected workers finish', async () => {
  const f = fixture();
  let supervisor;
  let client;
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    let idleCalls = 0;
    supervisor = createSupervisor({ ...f, idleTimeoutMs: 40, onIdle: () => { idleCalls++; } });
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const adapterPath = fileURLToPath(new URL('./fake-adapter.mjs', import.meta.url));
    await client.request('start', {
      job: { jobId: 'idle-worker', coordinatorId: 'root', workflowId: 'flow', delayMs: 250 }, adapterPath,
    });
    client.close();
    client = undefined;
    await new Promise(resolve => setTimeout(resolve, 90));
    assert.equal(idleCalls, 0);
    const deadline = Date.now() + 8000;
    while (idleCalls === 0 && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 20));
    assert.equal(supervisor.store.getJob('idle-worker').state, 'succeeded');
    assert.equal(idleCalls, 1);
  } finally {
    client?.close();
    await supervisor?.close();
    f.cleanup();
  }
});

test('worker silence emits an ephemeral stall warning without completing or waking a wait', async () => {
  const f = fixture();
  let supervisor;
  let client;
  try {
    const token = ensureToken(f.sessionId, f.baseDir);
    supervisor = createSupervisor({ ...f, stallTimeoutMs: 80 });
    await supervisor.listen();
    client = await MailboxClient.connect({ ...f, token });
    const seen = [];
    await client.subscribe(0, event => seen.push(event));
    const adapterPath = fileURLToPath(new URL('./fake-adapter.mjs', import.meta.url));
    await client.request('start', {
      job: { jobId: 'quiet-worker', coordinatorId: 'root', workflowId: 'flow', delayMs: 300 }, adapterPath,
    });
    const armed = await client.request('watch', {
      wait: { waitId: 'quiet-wait', coordinatorId: 'root', workflowId: 'flow', jobIds: ['quiet-worker'], mode: 'all' },
    });
    assert.equal(armed.ready, false);
    const deadline = Date.now() + 5000;
    while (!seen.some(event => event.event_type === 'suspected_stall') && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.ok(seen.some(event => event.event_type === 'suspected_stall'));
    assert.equal((await client.request('wait_status', { waitId: 'quiet-wait' })).ready, false);
    assert.equal((await client.request('events', { after: 0 })).some(event => event.event_type === 'suspected_stall'), false);
    while (!seen.some(event => event.event_type === 'terminal') && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.ok(seen.some(event => event.event_type === 'terminal'));
  } finally {
    client?.close();
    await supervisor?.close();
    f.cleanup();
  }
});
