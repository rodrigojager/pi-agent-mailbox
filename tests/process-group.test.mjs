import test from 'node:test';
import assert from 'node:assert/strict';
import { detachMailboxProcess } from '../src/process-group.mjs';

test('root supervisors stay independent while nested POSIX supervisors and workers join their Pi group', () => {
  for (const platform of ['linux', 'darwin']) {
    assert.equal(detachMailboxProcess(platform, '0'), true);
    assert.equal(detachMailboxProcess(platform, '1'), false);
    assert.equal(detachMailboxProcess(platform, '3'), false);
    assert.equal(detachMailboxProcess(platform, 'invalid'), true);
  }
  for (const depth of [undefined, '0', '1', '3']) {
    assert.equal(detachMailboxProcess('win32', depth), true);
  }
});
