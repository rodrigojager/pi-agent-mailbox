import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { StringDecoder } from 'node:string_decoder';

export const PROTOCOL_VERSION = 1;
export const MAX_FRAME_BYTES = 2 * 1024 * 1024;

export function sessionKey(sessionId) {
  if (typeof sessionId !== 'string' || !/^[a-zA-Z0-9._-]{1,128}$/.test(sessionId)) {
    throw new Error('Invalid Pi session ID');
  }
  return createHash('sha256').update(sessionId).digest('hex').slice(0, 32);
}

export function statePath(baseDir, sessionId) {
  return join(baseDir, sessionKey(sessionId));
}

export function endpointFor(sessionId, baseDir) {
  const key = sessionKey(sessionId);
  if (process.platform === 'win32') return `\\\\.\\pipe\\pi-agent-mailbox-${key}`;
  return join(statePath(baseDir, sessionId), 'supervisor.sock');
}

export function encodeFrame(value) {
  const line = JSON.stringify(value);
  if (Buffer.byteLength(line, 'utf8') > MAX_FRAME_BYTES) throw new Error('IPC frame is too large');
  return `${line}\n`;
}

export function createFrameParser(onFrame) {
  let buffered = '';
  const decoder = new StringDecoder('utf8');
  return (chunk) => {
    buffered += decoder.write(chunk);
    if (Buffer.byteLength(buffered, 'utf8') > MAX_FRAME_BYTES * 2) {
      throw new Error('IPC buffer is too large');
    }
    let separator;
    while ((separator = buffered.indexOf('\n')) >= 0) {
      const raw = buffered.slice(0, separator);
      buffered = buffered.slice(separator + 1);
      if (!raw) continue;
      if (Buffer.byteLength(raw, 'utf8') > MAX_FRAME_BYTES) throw new Error('IPC frame is too large');
      onFrame(JSON.parse(raw));
    }
  };
}
