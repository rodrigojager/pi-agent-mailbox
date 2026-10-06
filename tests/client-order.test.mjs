import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { MailboxClient } from '../src/client.mjs';
import { encodeFrame } from '../src/protocol.mjs';

test('subscription merges replay with duplicate and reordered live frames once by sequence', async () => {
  const socket = new EventEmitter();
  socket.destroy = () => socket.emit('close');
  socket.write = bytes => {
    const request = JSON.parse(bytes);
    const send = frame => socket.emit('data', Buffer.from(encodeFrame(frame)));
    if (request.op === 'subscribe') {
      send({ op: 'event', value: { seq: 3, event_id: 'third' } });
      send({ op: 'event', value: { event_type: 'progress' } });
      send({ op: 'event', value: { seq: 2, event_id: 'second' } });
      send({ op: 'event', value: { seq: 2, event_id: 'second' } });
      send({ id: request.id, ok: true, value: [{ seq: 1, event_id: 'first' }] });
    } else if (request.op === 'events') {
      send({ id: request.id, ok: true, value: [] });
    } else {
      throw new Error(`Unexpected test operation: ${request.op}`);
    }
  };
  const client = new MailboxClient(socket, 'fixture', 'fixture-token');
  try {
    const seen = [];
    const unsubscribe = await client.subscribe(0, event => seen.push(event.event_id ?? event.event_type));
    assert.deepEqual(seen, ['progress', 'first', 'second', 'third']);
    unsubscribe();
  } finally {
    client.close();
  }
});
