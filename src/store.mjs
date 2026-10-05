import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';

const TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);
const REPORTABLE = new Set([...TERMINAL, 'unknown']);

export class MailboxStore {
  constructor(filename) {
    mkdirSync(dirname(filename), { recursive: true, mode: 0o700 });
    this.db = new DatabaseSync(filename);
    this.db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS schema_meta (version INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS jobs (
        job_id TEXT PRIMARY KEY, coordinator_id TEXT NOT NULL,
        workflow_id TEXT NOT NULL, goal_id TEXT, state TEXT NOT NULL,
        worker_pid INTEGER, worker_started_at INTEGER,
        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS jobs_state_updated ON jobs(state, updated_at);
      CREATE TABLE IF NOT EXISTS events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        job_id TEXT NOT NULL, event_type TEXT NOT NULL,
        payload TEXT NOT NULL, occurred_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS events_job_seq ON events(job_id, seq);
      CREATE TABLE IF NOT EXISTS deliveries (
        event_id TEXT PRIMARY KEY REFERENCES events(event_id),
        state TEXT NOT NULL, entry_id TEXT, recorded_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS waits (
        wait_id TEXT PRIMARY KEY, coordinator_id TEXT NOT NULL,
        workflow_id TEXT NOT NULL, mode TEXT NOT NULL,
        job_ids TEXT NOT NULL, after_seq INTEGER NOT NULL,
        state TEXT NOT NULL, created_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS consumer_cursors (
        consumer_id TEXT PRIMARY KEY, last_seq INTEGER NOT NULL
      );
    `);
    const versions = this.db.prepare('SELECT version FROM schema_meta').all();
    if (versions.length === 0) this.db.prepare('INSERT INTO schema_meta VALUES (2)').run();
    else if (versions.length === 1 && versions[0].version === 1) {
      this.db.exec('BEGIN IMMEDIATE');
      try {
        this.db.exec('ALTER TABLE jobs ADD COLUMN goal_id TEXT');
        this.db.prepare('UPDATE schema_meta SET version=2').run();
        this.db.exec('COMMIT');
      } catch (error) {
        this.db.exec('ROLLBACK');
        this.db.close();
        throw error;
      }
    } else if (versions.length !== 1 || versions[0].version !== 2) {
      this.db.close();
      throw new Error('Unsupported mailbox schema');
    }
    this.insertJob = this.db.prepare(`INSERT OR IGNORE INTO jobs(job_id,coordinator_id,workflow_id,goal_id,state,worker_pid,worker_started_at,created_at,updated_at) VALUES (?, ?, ?, ?, 'registered', NULL, NULL, ?, ?)`);
    this.getJobStmt = this.db.prepare('SELECT * FROM jobs WHERE job_id = ?');
    this.updateJob = this.db.prepare('UPDATE jobs SET state = ?, updated_at = ? WHERE job_id = ?');
    this.claimStartStmt = this.db.prepare("UPDATE jobs SET state='starting', updated_at=? WHERE job_id=? AND state='registered'");
    this.recordWorkerStmt = this.db.prepare("UPDATE jobs SET worker_pid=?, worker_started_at=?, updated_at=? WHERE job_id=? AND state='starting' AND worker_pid IS NULL");
    this.openJobsStmt = this.db.prepare("SELECT * FROM jobs WHERE state IN ('registered','starting','running','unknown')");
    this.insertEvent = this.db.prepare('INSERT OR IGNORE INTO events(event_id,job_id,event_type,payload,occurred_at) VALUES(?,?,?,?,?)');
    this.getEventById = this.db.prepare('SELECT * FROM events WHERE event_id = ?');
    this.getEventsAfter = this.db.prepare('SELECT * FROM events WHERE seq > ? ORDER BY seq LIMIT ?');
    this.latestResultForJob = this.db.prepare("SELECT * FROM events WHERE job_id=? AND event_type IN ('terminal','supervision_lost','worker_exit','worker_error','persistence_failed') ORDER BY seq DESC LIMIT 1");
    this.insertDelivery = this.db.prepare("INSERT OR IGNORE INTO deliveries VALUES (?, 'pending', NULL, NULL)");
    this.ackDelivery = this.db.prepare("UPDATE deliveries SET state='recorded', entry_id=?, recorded_at=? WHERE event_id=?");
    this.insertWait = this.db.prepare('INSERT OR IGNORE INTO waits VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
    this.getWaitStmt = this.db.prepare('SELECT * FROM waits WHERE wait_id = ?');
    this.updateWait = this.db.prepare("UPDATE waits SET state=? WHERE wait_id=?");
    this.waitsForJob = this.db.prepare("SELECT * FROM waits WHERE state='armed' AND job_ids LIKE ?");
    this.retentionCandidates = this.db.prepare(`
      SELECT j.job_id FROM jobs j
      WHERE j.state IN ('succeeded', 'failed', 'cancelled') AND j.updated_at < ?
        AND EXISTS (
          SELECT 1 FROM events e JOIN deliveries d ON d.event_id=e.event_id
          WHERE e.job_id=j.job_id
        )
        AND NOT EXISTS (
          SELECT 1 FROM events e LEFT JOIN deliveries d ON d.event_id=e.event_id
          WHERE e.job_id=j.job_id AND (
            (d.event_id IS NOT NULL AND (d.state!='recorded' OR d.recorded_at IS NULL OR d.recorded_at >= ?))
            OR (e.event_type IN ('terminal','supervision_lost','worker_exit','worker_error','persistence_failed') AND d.event_id IS NULL)
          )
        )
        AND NOT EXISTS (
          SELECT 1 FROM waits w, json_each(w.job_ids) target
          WHERE w.state IN ('armed','ready') AND target.value=j.job_id
        )
      ORDER BY j.updated_at LIMIT ?
    `);
    this.deleteDeliveriesForJob = this.db.prepare('DELETE FROM deliveries WHERE event_id IN (SELECT event_id FROM events WHERE job_id=?)');
    this.deleteEventsForJob = this.db.prepare('DELETE FROM events WHERE job_id=?');
    this.deleteExpiredCancelledWaits = this.db.prepare("DELETE FROM waits WHERE state='cancelled' AND created_at < ?");
  }

  registerJob({ jobId, coordinatorId, workflowId, goalId = null }) {
    if (![jobId, coordinatorId, workflowId].every(x => typeof x === 'string' && x.length > 0)) {
      throw new Error('Invalid job identity');
    }
    if (goalId !== null && (typeof goalId !== 'string' || !goalId)) throw new Error('Invalid job goal ID');
    const now = Date.now();
    this.insertJob.run(jobId, coordinatorId, workflowId, goalId, now, now);
    const row = this.getJobStmt.get(jobId);
    if (row.coordinator_id !== coordinatorId || row.workflow_id !== workflowId || row.goal_id !== goalId) {
      throw new Error('Job ID belongs to another workflow');
    }
    return row;
  }

  publish({ eventId = randomUUID(), jobId, eventType, executionState, payload = {} }) {
    if (typeof eventId !== 'string' || !eventId || typeof eventType !== 'string' || !eventType) {
      throw new Error('Invalid event identity');
    }
    const job = this.getJobStmt.get(jobId);
    if (!job) throw new Error('Unknown job');
    if (executionState && !['registered','starting','running',...REPORTABLE].includes(executionState)) {
      throw new Error('Invalid execution state');
    }
    if (TERMINAL.has(job.state) && executionState && job.state !== executionState) {
      throw new Error('A terminal job cannot change state');
    }
    if (TERMINAL.has(job.state) && !this.getEventById.get(eventId)) {
      throw new Error('A terminal job cannot publish another event');
    }
    const serialized = JSON.stringify(payload);
    if (Buffer.byteLength(serialized, 'utf8') > 512 * 1024) throw new Error('Mailbox event payload is too large');
    const now = Date.now();
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const inserted = this.insertEvent.run(eventId, jobId, eventType, serialized, now).changes;
      if (inserted) {
        if (executionState) this.updateJob.run(executionState, now, jobId);
        if (executionState && REPORTABLE.has(executionState)) this.insertDelivery.run(eventId);
        if (executionState && REPORTABLE.has(executionState)) this.refreshWaitsForJob(jobId);
      }
      const event = this.getEventById.get(eventId);
      if (event.job_id !== jobId || event.event_type !== eventType || event.payload !== serialized) {
        throw new Error('Event ID reused with another payload');
      }
      this.db.exec('COMMIT');
      return { ...event, payload: JSON.parse(event.payload), duplicate: !inserted };
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  eventsAfter(sequence = 0, limit = 1000) {
    const rows = this.getEventsAfter.all(sequence, Math.max(1, Math.min(1000, limit)));
    const result = [];
    let bytes = 0;
    for (const row of rows) {
      const size = Buffer.byteLength(JSON.stringify(row), 'utf8');
      if (result.length && bytes + size > 1024 * 1024) break;
      bytes += size;
      result.push({ ...row, payload: JSON.parse(row.payload) });
    }
    return result;
  }

  getJob(jobId) { return this.getJobStmt.get(jobId); }

  openJobs() { return this.openJobsStmt.all(); }

  claimStart(jobId) {
    if (this.claimStartStmt.run(Date.now(), jobId).changes !== 1) return false;
    return true;
  }

  recordWorker(jobId, pid) {
    if (!Number.isSafeInteger(pid) || pid < 1) throw new Error('Invalid worker process ID');
    const now = Date.now();
    if (this.recordWorkerStmt.run(pid, now, now, jobId).changes !== 1) {
      throw new Error('Job was already started');
    }
    return this.getJob(jobId);
  }

  waitStatus(jobIds, mode) {
    const statuses = {};
    for (const id of jobIds) {
      const job = this.getJob(id);
      if (!job) throw new Error(`Unknown wait job ${id}`);
      statuses[id] = job.state;
    }
    const values = Object.values(statuses);
    const failed = values.some(state => ['failed', 'cancelled', 'unknown'].includes(state));
    const completed = mode === 'any'
      ? values.some(state => REPORTABLE.has(state))
      : values.every(state => state === 'succeeded');
    return { ready: failed || completed, statuses };
  }

  armWait({ waitId, coordinatorId, workflowId, jobIds, mode, afterSeq = 0 }) {
    if (![waitId, coordinatorId, workflowId].every(value => typeof value === 'string' && value.length > 0)) throw new Error('Invalid wait identity');
    if (!Array.isArray(jobIds) || jobIds.length === 0 || jobIds.length > 64 || new Set(jobIds).size !== jobIds.length) throw new Error('Invalid wait targets');
    if (!['any', 'all'].includes(mode)) throw new Error('Invalid wait mode');
    if (!Number.isSafeInteger(afterSeq) || afterSeq < 0) throw new Error('Invalid wait cursor');
    for (const id of jobIds) {
      const job = this.getJob(id);
      if (!job || job.coordinator_id !== coordinatorId || job.workflow_id !== workflowId) throw new Error(`Wait target ${id} does not belong to this coordinator`);
    }
    const existing = this.getWaitStmt.get(waitId);
    if (existing && (existing.coordinator_id !== coordinatorId || existing.workflow_id !== workflowId || existing.mode !== mode || existing.job_ids !== JSON.stringify(jobIds))) throw new Error('Wait ID reused with different targets');
    const status = this.waitStatus(jobIds, mode);
    this.insertWait.run(waitId, coordinatorId, workflowId, mode, JSON.stringify(jobIds), afterSeq, status.ready ? 'ready' : 'armed', Date.now());
    if (status.ready) this.updateWait.run('ready', waitId);
    return { waitId, mode, ...status, state: this.getWaitStmt.get(waitId).state };
  }

  getWait(waitId) {
    const wait = this.getWaitStmt.get(waitId);
    if (!wait) return null;
    return { waitId, mode: wait.mode, state: wait.state, ...this.waitStatus(JSON.parse(wait.job_ids), wait.mode) };
  }

  waitResults(waitId) {
    const wait = this.getWaitStmt.get(waitId);
    if (!wait) throw new Error('Unknown wait');
    return JSON.parse(wait.job_ids).flatMap(jobId => {
      const event = this.latestResultForJob.get(jobId);
      return event ? [{ ...event, payload: JSON.parse(event.payload) }] : [];
    }).sort((a, b) => a.seq - b.seq);
  }

  cancelWait(waitId) {
    const current = this.getWaitStmt.get(waitId);
    if (current && ['armed', 'ready'].includes(current.state)) this.updateWait.run('cancelled', waitId);
    return this.getWait(waitId);
  }

  refreshWaitsForJob(jobId) {
    // The quoted JSON string avoids matching a job ID that is merely a substring.
    for (const wait of this.waitsForJob.all(`%"${jobId}"%`)) {
      const targets = JSON.parse(wait.job_ids);
      if (targets.includes(jobId) && this.waitStatus(targets, wait.mode).ready) {
        this.updateWait.run('ready', wait.wait_id);
      }
    }
  }

  ack(eventId, entryId) {
    if (typeof entryId !== 'string' || !entryId) throw new Error('History entry ID required');
    const changed = this.ackDelivery.run(entryId, Date.now(), eventId).changes;
    if (!changed) throw new Error('Unknown terminal event');
    return this.db.prepare('SELECT * FROM deliveries WHERE event_id = ?').get(eventId);
  }

  pruneRecorded(before, limit = 1000) {
    if (!Number.isSafeInteger(before) || before < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error('Invalid mailbox retention bounds');
    }
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const jobIds = this.retentionCandidates.all(before, before, limit).map(row => row.job_id);
      for (const jobId of jobIds) {
        this.deleteDeliveriesForJob.run(jobId);
        this.deleteEventsForJob.run(jobId);
      }
      const waits = this.deleteExpiredCancelledWaits.run(before).changes;
      this.db.exec('COMMIT');
      return { jobIds, waits };
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  checkpoint() {
    return this.db.prepare('PRAGMA wal_checkpoint(PASSIVE)').get();
  }

  close() { this.db.close(); }
}
