import { connect } from 'node:net';
import { mkdirSync, writeFileSync, readFileSync, linkSync, unlinkSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createFrameParser, encodeFrame, endpointFor, PROTOCOL_VERSION, statePath } from './protocol.mjs';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

export function ensureToken(sessionId, baseDir) {
  const dir = statePath(baseDir, sessionId);
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const filename = join(dir, 'token');
  const temporary = join(dir, `token.${process.pid}.${randomBytes(8).toString('hex')}.tmp`);
  try {
    writeFileSync(temporary, randomBytes(32).toString('hex'), { flag: 'wx', mode: 0o600 });
    try { linkSync(temporary, filename); }
    catch (error) { if (error.code !== 'EEXIST') throw error; }
  } finally {
    try { unlinkSync(temporary); } catch { /* Temporary file may not have been created. */ }
  }
  const token = readFileSync(filename, 'utf8').trim();
  if (!/^[0-9a-f]{64}$/.test(token)) throw new Error('Mailbox token is incomplete or invalid');
  return token;
}

export class MailboxClient {
  constructor(socket, sessionId, token) {
    this.socket = socket;
    this.sessionId = sessionId;
    this.token = token;
    this.nextId = 1;
    this.pending = new Map();
    this.listeners = new Set();
    this.closeListeners = new Set();
    this.closed = false;
    this.isOwner = false;
    this.generation = 0;
    const parse = createFrameParser(frame => {
      if (frame?.op === 'event') {
        for (const listener of this.listeners) listener(frame.value);
        return;
      }
      const pending = this.pending.get(frame?.id);
      if (!pending) return;
      this.pending.delete(frame.id);
      clearTimeout(pending.timer);
      if (frame.ok) pending.resolve(frame.value);
      else pending.reject(new Error(frame.error ?? 'Mailbox operation failed'));
    });
    socket.on('data', bytes => { try { parse(bytes); } catch { socket.destroy(); } });
    socket.on('close', () => this.close());
    socket.on('error', () => this.close());
  }

  static async connect({ sessionId, baseDir, token, timeoutMs = 2000 }) {
    const endpoint = endpointFor(sessionId, baseDir);
    const socket = await new Promise((resolve, reject) => {
      const value = connect(endpoint);
      const timer = setTimeout(() => { value.destroy(); reject(new Error('Mailbox connection timed out')); }, timeoutMs);
      value.once('connect', () => { clearTimeout(timer); value.off('error', reject); resolve(value); });
      value.once('error', error => { clearTimeout(timer); reject(error); });
    });
    const client = new MailboxClient(socket, sessionId, token);
    try {
      await client.request('hello', { sessionId, token, version: PROTOCOL_VERSION });
      await client.claim();
      return client;
    } catch (error) {
      client.close();
      throw error;
    }
  }

  request(op, data = {}, timeoutMs = 10000) {
    if (this.closed) return Promise.reject(new Error('Mailbox connection closed'));
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Mailbox ${op} timed out`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      try { this.socket.write(encodeFrame({ id, op, ...data })); }
      catch (error) { clearTimeout(timer); this.pending.delete(id); reject(error); }
    });
  }

  async claim() {
    const value = await this.request('claim');
    this.isOwner = value.owner === true;
    this.generation = value.generation;
    return value;
  }

  async subscribe(after, listener) {
    let replaying = true;
    const buffered = [];
    let lastSequence = after;
    const deliver = event => {
      if (Number.isSafeInteger(event?.seq)) {
        if (event.seq <= lastSequence) return;
        lastSequence = event.seq;
      }
      listener(event);
    };
    const receive = event => {
      if (replaying) buffered.push(event);
      else deliver(event);
    };
    this.listeners.add(receive);
    try {
      let cursor = after;
      let replay = await this.request('subscribe', { after: cursor, limit: 1000 });
      while (true) {
        for (const event of replay) {
          deliver(event);
          cursor = event.seq;
        }
        if (replay.length === 0) break;
        replay = await this.request('events', { after: cursor, limit: 1000 });
      }
      replaying = false;
      for (const event of buffered) deliver(event);
    } catch (error) {
      this.listeners.delete(receive);
      throw error;
    }
    return () => this.listeners.delete(receive);
  }

  onClose(listener) {
    if (this.closed) listener();
    else this.closeListeners.add(listener);
    return () => this.closeListeners.delete(listener);
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    this.isOwner = false;
    this.socket.destroy();
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(new Error('Mailbox connection closed'));
    }
    this.pending.clear();
    this.listeners.clear();
    for (const listener of this.closeListeners) listener();
    this.closeListeners.clear();
  }
}

export async function connectOrStartSupervisor({ sessionId, baseDir, startupTimeoutMs = 5000 }) {
  const token = ensureToken(sessionId, baseDir);
  try { return await MailboxClient.connect({ sessionId, baseDir, token }); }
  catch (error) {
    if (!['ENOENT', 'ECONNREFUSED'].includes(error.code)) throw error;
  }
  const supervisorPath = fileURLToPath(new URL('./supervisor.mjs', import.meta.url));
  const child = spawn(process.execPath, [supervisorPath, sessionId, baseDir], {
    detached: true,
    stdio: 'ignore',
    windowsHide: true,
  });
  child.unref();
  const deadline = Date.now() + startupTimeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try { return await MailboxClient.connect({ sessionId, baseDir, token, timeoutMs: 500 }); }
    catch (error) {
      lastError = error;
      if (!['ENOENT', 'ECONNREFUSED', 'ETIMEDOUT'].includes(error.code)) throw error;
      await sleep(50);
    }
  }
  throw new Error(`Mailbox supervisor did not start: ${lastError?.message ?? 'unknown error'}`);
}
