import { createJiti } from 'jiti';
import { mkdirSync, openSync, writeFileSync, fsyncSync, closeSync, renameSync } from 'node:fs';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';

const jiti = createJiti(import.meta.url);
let started = false;
let controller;

function persistResult(filename, result) {
  mkdirSync(dirname(filename), { recursive: true, mode: 0o700 });
  const temporary = `${filename}.${randomUUID()}.tmp`;
  const descriptor = openSync(temporary, 'wx', 0o600);
  try {
    const serialized = JSON.stringify(result);
    writeFileSync(descriptor, serialized, 'utf8');
    fsyncSync(descriptor);
  } finally { closeSync(descriptor); }
  renameSync(temporary, filename);
}

function report(frame) {
  try { if (process.connected) process.send?.(frame, () => {}); }
  catch { /* The supervisor can replay the result artifact later. */ }
}

process.on('message', async frame => {
  if (frame?.op === 'cancel') {
    controller?.abort(new Error('Cancelled by coordinator'));
    return;
  }
  if (started || frame?.op !== 'start') return;
  started = true;
  controller = new AbortController();
  let outcome;
  try {
    const adapter = await jiti.import(frame.adapterPath);
    if (typeof adapter.runMailboxJob !== 'function') throw new Error('Invalid subagent worker adapter');
    if (controller.signal.aborted) throw new Error('Cancelled before worker adapter started');
    outcome = await adapter.runMailboxJob(frame.job, update => report({ op: 'progress', update }), controller.signal);
    if (!outcome || !['succeeded', 'failed', 'cancelled'].includes(outcome.state)) {
      throw new Error('Worker returned an invalid outcome');
    }
  } catch (error) {
    outcome = { state: controller.signal.aborted ? 'cancelled' : 'failed', error: error instanceof Error ? error.message : String(error) };
  }
  try {
    persistResult(frame.resultPath, outcome);
    report({ op: 'done', state: outcome.state, resultPath: frame.resultPath, summary: String(outcome.summary ?? outcome.error ?? '').slice(0, 8192) });
  } catch (error) {
    report({ op: 'failed_to_persist', error: error instanceof Error ? error.message : String(error) });
  }
  // Do not leave a disconnected worker holding a handle after its child ended.
  setTimeout(() => process.exit(0), 25).unref();
});
