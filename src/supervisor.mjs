import { createServer } from 'node:net';
import { readFileSync, existsSync, mkdirSync, unlinkSync, watch } from 'node:fs';
import { join, isAbsolute } from 'node:path';
import { timingSafeEqual, createHash } from 'node:crypto';
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { endpointFor, encodeFrame, createFrameParser, PROTOCOL_VERSION, statePath } from './protocol.mjs';
import { MailboxStore } from './store.mjs';
import { detachMailboxProcess } from './process-group.mjs';

const MAX_SOCKET_BACKLOG_BYTES = 4 * 1024 * 1024;
const DROP_EPHEMERAL_AT_BYTES = 512 * 1024;
const EPHEMERAL_EVENTS = new Set(['progress', 'heartbeat', 'suspected_stall']);
const RETENTION_INTERVAL_MS = 24 * 60 * 60 * 1000;
const RETENTION_BATCH_LIMIT = 1000;

function sameToken(left, right) {
  if (typeof left !== 'string' || typeof right !== 'string') return false;
  const a = Buffer.from(left, 'utf8');
  const b = Buffer.from(right, 'utf8');
  return a.length === b.length && timingSafeEqual(a, b);
}

function isStorageFailure(error) {
  const code = error?.code;
  return typeof code === 'string' &&
    (code.startsWith('SQLITE_') || code.startsWith('ERR_SQLITE_') || ['ENOSPC', 'EIO', 'EROFS', 'EACCES', 'EPERM'].includes(code));
}

function isTransientLock(error) {
  return ['SQLITE_BUSY', 'SQLITE_LOCKED', 'ERR_SQLITE_ERROR'].includes(error?.code)
    && /database (?:is )?locked|database is busy/i.test(error?.message ?? '');
}

export function createSupervisor({ sessionId, baseDir, idleTimeoutMs = 60000, stallTimeoutMs = 300000,
  retentionMs = Number(process.env.PI_AGENT_MAILBOX_RETENTION_DAYS ?? 30) * RETENTION_INTERVAL_MS, onIdle }) {
  if (!Number.isSafeInteger(retentionMs) || retentionMs < 0 || retentionMs > 3650 * RETENTION_INTERVAL_MS) {
    throw new Error('Invalid Pi mailbox retention period');
  }
  const directory = statePath(baseDir, sessionId);
  const token = readFileSync(join(directory, 'token'), 'utf8').trim();
  const store = new MailboxStore(join(directory, 'journal.sqlite'));
  const artifactDir = join(directory, 'artifacts');
  mkdirSync(artifactDir, { recursive: true, mode: 0o700 });
  const subscribers = new Set();
  const sockets = new Set();
  const workers = new Map();
  const stallTimers = new Map();
  let ownerSocket;
  let ownerGeneration = 0;
  let closed = false;
  let storageError;
  let maintenanceWarning;
  let maintenanceContinuation;
  let idleTimer;
  const clearIdle = () => {
    if (idleTimer) clearTimeout(idleTimer);
    idleTimer = undefined;
  };
  const scheduleIdle = () => {
    if (!onIdle || closed || sockets.size || workers.size || idleTimer) return;
    idleTimer = setTimeout(() => {
      idleTimer = undefined;
      if (!closed && sockets.size === 0 && workers.size === 0) onIdle();
    }, idleTimeoutMs);
  };
  const heartbeat = setInterval(() => {
    if (closed) return;
    for (const listener of subscribers) listener({
      event_type: 'heartbeat', payload: { supervisorPid: process.pid, at: Date.now() },
    });
  }, 15000);
  const clearStall = jobId => {
    const timer = stallTimers.get(jobId);
    if (timer) clearTimeout(timer);
    stallTimers.delete(jobId);
  };
  const armStall = jobId => {
    clearStall(jobId);
    const timer = setTimeout(() => {
      stallTimers.delete(jobId);
      if (closed || !workers.has(jobId)) return;
      for (const listener of subscribers) listener({
        event_type: 'suspected_stall', job_id: jobId,
        payload: { quietMs: stallTimeoutMs, at: Date.now() },
      });
    }, stallTimeoutMs);
    stallTimers.set(jobId, timer);
  };

  function noteStorageFailure(error, jobId) {
    if (storageError) return;
    storageError = error instanceof Error ? error.message : String(error);
    for (const listener of subscribers) listener({
      event_id: `storage-unavailable:${jobId ?? 'session'}`,
      event_type: 'storage_unavailable', job_id: jobId ?? '',
      payload: { error: storageError, at: Date.now() },
    });
  }

  function maintainJournal() {
    let moreCandidates = false;
    if (retentionMs > 0) {
      const { jobIds } = store.pruneRecorded(Date.now() - retentionMs, RETENTION_BATCH_LIMIT);
      moreCandidates = jobIds.length === RETENTION_BATCH_LIMIT;
      for (const jobId of jobIds) {
        if (!/^[a-zA-Z0-9_-]{1,128}$/.test(jobId)) continue;
        try { unlinkSync(join(artifactDir, `${jobId}.json`)); }
        catch (error) {
          if (error?.code !== 'ENOENT') maintenanceWarning = `Could not remove retained artifact for ${jobId}: ${error}`;
        }
      }
    }
    const checkpoint = store.checkpoint();
    if (checkpoint?.busy) maintenanceWarning = 'Mailbox WAL checkpoint is busy';
    if (moreCandidates && !closed) {
      maintenanceContinuation = setImmediate(() => {
        maintenanceContinuation = undefined;
        runMaintenance(true);
      });
    }
  }

  function runMaintenance(continuing = false) {
    if (closed || storageError || maintenanceContinuation) return;
    try {
      if (!continuing) maintenanceWarning = undefined;
      maintainJournal();
    } catch (error) {
      if (isTransientLock(error)) {
        maintenanceWarning = `Mailbox maintenance deferred: ${error.message}`;
      } else if (isStorageFailure(error)) {
        noteStorageFailure(error);
      } else {
        maintenanceWarning = error instanceof Error ? error.message : String(error);
      }
    }
  }

  runMaintenance();
  const maintenanceTimer = setInterval(() => {
    if (closed || storageError) return;
    runMaintenance();
  }, RETENTION_INTERVAL_MS);
  maintenanceTimer.unref?.();

  function publishAndNotify(event) {
    let value;
    try { value = store.publish(event); }
    catch (error) {
      if (isStorageFailure(error)) noteStorageFailure(error, event.jobId);
      throw error;
    }
    if (!value.duplicate) for (const listener of subscribers) listener(value);
    return value;
  }

  function reconcileArtifact(jobId) {
    if (closed) return;
    let job;
    try { job = store.getJob(jobId); }
    catch (error) { noteStorageFailure(error, jobId); return; }
    if (!job || ['succeeded', 'failed', 'cancelled'].includes(job.state)) return;
    const filename = join(artifactDir, `${jobId}.json`);
    if (!existsSync(filename)) return;
    try {
      const serialized = readFileSync(filename, 'utf8');
      const outcome = JSON.parse(serialized);
      if (!['succeeded', 'failed', 'cancelled'].includes(outcome.state)) return;
      publishAndNotify({
        eventId: `result:${jobId}`,
        jobId,
        eventType: 'terminal',
        executionState: outcome.state,
        payload: {
          state: outcome.state,
          summary: String(outcome.summary ?? outcome.error ?? '').slice(0, 8192),
          resultRef: filename,
          resultSha256: createHash('sha256').update(serialized).digest('hex'),
        },
      });
    } catch (error) {
      // Keep the job unknown: a broken artifact must not become a success.
      process.stderr.write(`Mailbox result recovery failed for ${jobId}: ${error}\n`);
    }
  }

  function recoverOpenJobs() {
    for (const job of store.openJobs()) {
      reconcileArtifact(job.job_id);
      const current = store.getJob(job.job_id);
      if (current && current.state !== 'unknown' && !['succeeded', 'failed', 'cancelled'].includes(current.state)) {
        publishAndNotify({
          eventId: `supervision-lost:${job.job_id}`,
          jobId: job.job_id,
          eventType: 'supervision_lost',
          executionState: 'unknown',
          payload: { previousState: current.state },
        });
      }
    }
  }

  const artifactWatcher = watch(artifactDir, (_kind, filename) => {
    if (closed) return;
    if (filename && /^[a-zA-Z0-9_-]{1,128}\.json$/.test(filename)) {
      reconcileArtifact(filename.slice(0, -5));
    }
  });

  function startJob(frame) {
    if (storageError) throw new Error(`Mailbox storage unavailable: ${storageError}`);
    const job = frame.job;
    if (!job || !/^[a-zA-Z0-9_-]{1,128}$/.test(job.jobId ?? '')) throw new Error('Invalid worker job ID');
    if (!isAbsolute(frame.adapterPath)) throw new Error('Worker adapter path must be absolute');
    const created = store.registerJob(job);
    if (created.state !== 'registered') return { job: created, duplicate: true };
    const resultPath = join(artifactDir, `${job.jobId}.json`);
    if (existsSync(resultPath)) throw new Error('Job result artifact already exists');
    // This durable claim precedes fork. After a crash, a repeated start must
    // remain indeterminate instead of launching the same external work twice.
    if (!store.claimStart(job.jobId)) return { job: store.getJob(job.jobId), duplicate: true };
    const workerPath = fileURLToPath(new URL('./worker.mjs', import.meta.url));
    let worker;
    try {
      worker = fork(workerPath, [], {
        detached: detachMailboxProcess(),
        stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
        windowsHide: true,
        env: { ...process.env, ...(job.depth !== undefined ? { PI_SUBAGENT_DEPTH: String(job.depth) } : {}) },
      });
      if (!worker.pid) throw new Error('Worker failed to start');
    } catch (error) {
      publishAndNotify({ eventId: `spawn-error:${job.jobId}`, jobId: job.jobId,
        eventType: 'worker_error', executionState: 'failed',
        payload: { error: error instanceof Error ? error.message : String(error) } });
      return { job: store.getJob(job.jobId), duplicate: false };
    }
    try { store.recordWorker(job.jobId, worker.pid); }
    catch (error) {
      if (isStorageFailure(error)) noteStorageFailure(error, job.jobId);
      try { worker.kill(); } catch { /* Worker may have exited already. */ }
      throw error;
    }
    workers.set(job.jobId, worker);
    clearIdle();
    armStall(job.jobId);
    try {
      publishAndNotify({ eventId: `started:${job.jobId}`, jobId: job.jobId, eventType: 'started', executionState: 'running', payload: { workerPid: worker.pid } });
    } catch (error) {
      clearStall(job.jobId);
      workers.delete(job.jobId);
      try { worker.kill(); } catch { /* Worker may have exited already. */ }
      throw error;
    }
    worker.on('message', message => {
      if (closed) return;
      if (message?.op === 'progress') {
        armStall(job.jobId);
        // Progress is presentation data. It is not a model wake or a terminal result.
        for (const listener of subscribers) listener({ event_type: 'progress', job_id: job.jobId, payload: message.update });
      } else if (message?.op === 'done') {
        reconcileArtifact(job.jobId);
      } else if (message?.op === 'failed_to_persist') {
        try {
          publishAndNotify({ eventId: `persist-error:${job.jobId}`, jobId: job.jobId, eventType: 'persistence_failed', executionState: 'unknown', payload: { error: message.error } });
        } catch { /* The storage health event already reported the failure. */ }
      }
    });
    worker.on('exit', (code, signal) => {
      clearStall(job.jobId);
      workers.delete(job.jobId);
      if (closed) return;
      reconcileArtifact(job.jobId);
      try {
        const current = store.getJob(job.jobId);
        if (current && !['succeeded', 'failed', 'cancelled', 'unknown'].includes(current.state)) {
          publishAndNotify({ eventId: `worker-exit:${job.jobId}`, jobId: job.jobId, eventType: 'worker_exit', executionState: 'failed', payload: { code, signal } });
        }
      } catch (error) {
        if (isStorageFailure(error)) noteStorageFailure(error, job.jobId);
      }
      scheduleIdle();
    });
    worker.on('error', error => {
      clearStall(job.jobId);
      if (closed) return;
      try {
        const current = store.getJob(job.jobId);
        if (current && !['succeeded', 'failed', 'cancelled', 'unknown'].includes(current.state)) {
          publishAndNotify({ eventId: `worker-error:${job.jobId}`, jobId: job.jobId, eventType: 'worker_error', executionState: 'failed', payload: { error: error.message } });
        }
      } catch (failure) {
        if (isStorageFailure(failure)) noteStorageFailure(failure, job.jobId);
      }
    });
    worker.send({ op: 'start', job, adapterPath: frame.adapterPath, resultPath });
    return { job: store.getJob(job.jobId), duplicate: false };
  }

  recoverOpenJobs();
  const server = createServer(socket => {
    clearIdle();
    sockets.add(socket);
    socket.setNoDelay(true);
    let authenticated = false;
    let subscribed = false;
    const send = payload => {
      if (socket.destroyed) return;
      if (payload?.op === 'event' && EPHEMERAL_EVENTS.has(payload.value?.event_type) &&
        socket.writableLength >= DROP_EPHEMERAL_AT_BYTES) return;
      try {
        const encoded = encodeFrame(payload);
        if (socket.writableLength + Buffer.byteLength(encoded, 'utf8') > MAX_SOCKET_BACKLOG_BYTES) {
          // A reconnect replays committed events; retaining an unbounded socket queue is unnecessary.
          socket.destroy();
          return;
        }
        socket.write(encoded);
      } catch { socket.destroy(); }
    };
    const listener = event => send({ op: 'event', value: event });
    const handle = frame => {
      const id = frame?.id;
      try {
        if (!authenticated) {
          if (frame?.op !== 'hello' || frame?.version !== PROTOCOL_VERSION || frame?.sessionId !== sessionId || !sameToken(frame?.token, token)) {
            throw new Error('Mailbox handshake rejected');
          }
          authenticated = true;
          send({ id, ok: true, value: { version: PROTOCOL_VERSION, sessionId } });
          return;
        }
        let value;
        if (['register', 'publish', 'start', 'cancel', 'watch', 'cancel_wait', 'ack'].includes(frame.op) && ownerSocket !== socket) {
          throw new Error('Mailbox session is owned by another Pi interface; this connection is an observer');
        }
        switch (frame.op) {
          case 'claim':
            if (!ownerSocket || ownerSocket.destroyed) {
              ownerSocket = socket;
              ownerGeneration++;
            }
            value = { owner: ownerSocket === socket, generation: ownerGeneration };
            break;
          case 'register':
            value = store.registerJob(frame.job);
            break;
          case 'publish': {
            value = publishAndNotify(frame.event);
            send({ id, ok: true, value });
            return;
          }
          case 'start':
            value = startJob(frame);
            break;
          case 'cancel': {
            const child = workers.get(frame.jobId);
            if (!child) throw new Error('Job is not attached to this supervisor');
            child.send({ op: 'cancel' });
            value = { requested: true };
            break;
          }
          case 'subscribe': {
            const after = frame.after;
            if (!Number.isSafeInteger(after) || after < 0) throw new Error('Invalid event cursor');
            // Install before replay: no publisher can interleave synchronously on this loop.
            if (!subscribed) {
              subscribed = true;
              subscribers.add(listener);
            }
            value = store.eventsAfter(after, frame.limit);
            break;
          }
          case 'events':
            value = store.eventsAfter(frame.after, frame.limit);
            break;
          case 'job':
            value = store.getJob(frame.jobId) ?? null;
            break;
          case 'watch':
            value = store.armWait(frame.wait);
            break;
          case 'wait_status':
            value = store.getWait(frame.waitId);
            break;
          case 'wait_results':
            value = store.waitResults(frame.waitId);
            break;
          case 'cancel_wait':
            value = store.cancelWait(frame.waitId);
            break;
          case 'artifact': {
            const job = store.getJob(frame.jobId);
            if (!job || !['succeeded', 'failed', 'cancelled'].includes(job.state)) throw new Error('Result is unavailable');
            const filename = join(artifactDir, `${frame.jobId}.json`);
            value = JSON.parse(readFileSync(filename, 'utf8'));
            break;
          }
          case 'ack':
            value = store.ack(frame.eventId, frame.entryId);
            break;
          case 'ping':
            value = { at: Date.now(), storageError: storageError ?? null, maintenanceWarning: maintenanceWarning ?? null };
            break;
          default:
            throw new Error('Unsupported mailbox operation');
        }
        send({ id, ok: true, value });
      } catch (error) {
        if (authenticated && isStorageFailure(error)) {
          noteStorageFailure(error, frame?.job?.jobId ?? frame?.event?.jobId ?? frame?.jobId);
        }
        send({ id, ok: false, error: error instanceof Error ? error.message : String(error) });
        if (!authenticated) socket.destroy();
      }
    };
    const parse = createFrameParser(handle);
    socket.on('data', bytes => {
      try { parse(bytes); }
      catch { socket.destroy(); }
    });
    socket.on('close', () => {
      sockets.delete(socket);
      if (subscribed) subscribers.delete(listener);
      if (ownerSocket === socket) ownerSocket = undefined;
      scheduleIdle();
    });
    socket.on('error', () => {});
  });
  return {
    endpoint: endpointFor(sessionId, baseDir),
    store,
    publish: publishAndNotify,
    ownerPresent: () => Boolean(ownerSocket && !ownerSocket.destroyed),
    maxSocketBacklogBytes: () => Math.max(0, ...[...sockets].map(socket => socket.writableLength)),
    async listen() {
      await new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(endpointFor(sessionId, baseDir), () => {
          server.off('error', reject);
          scheduleIdle();
          resolve();
        });
      });
    },
    async close() {
      if (closed) return;
      closed = true;
      clearIdle();
      clearInterval(heartbeat);
      clearInterval(maintenanceTimer);
      if (maintenanceContinuation) clearImmediate(maintenanceContinuation);
      for (const jobId of stallTimers.keys()) clearStall(jobId);
      artifactWatcher.close();
      for (const socket of sockets) socket.destroy();
      await new Promise(resolve => server.close(resolve));
      store.close();
    },
  };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const [sessionId, baseDir] = process.argv.slice(2);
  try {
    let supervisor;
    const stop = () => supervisor.close().finally(() => process.exit(0));
    supervisor = createSupervisor({ sessionId, baseDir, onIdle: stop });
    await supervisor.listen();
    process.once('SIGINT', stop);
    process.once('SIGTERM', stop);
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    process.exitCode = 1;
  }
}
