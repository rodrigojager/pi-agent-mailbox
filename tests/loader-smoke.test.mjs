import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, basename } from 'node:path';

const workspace = resolve(import.meta.dirname, '../..');
const installedRoot = process.env.PI_MAILBOX_INSTALL_ROOT;
const useInstalledSettings = process.env.PI_MAILBOX_USE_USER_SETTINGS === '1';
if (useInstalledSettings && !installedRoot) throw new Error('PI_MAILBOX_INSTALL_ROOT is required for the user-settings smoke');
const packages = installedRoot
  ? [
      'pi-agent-mailbox-v0.1.5/src/index.ts',
      'pi-subagent-v0.13.0-rodrigo.3/src/index.ts',
      'pi-goal-v0.54.8-rodrigo.1/dist/index.ts',
      'pi-agent-switcher-v0.4.0-rodrigo.8/index.ts',
      'pi-goal-highlight-v1.1.2/index.ts',
    ]
  : [
      'pi-agent-mailbox/src/index.ts',
      'pi-subagent/src/index.ts',
      'pi-goal-rodrigo/dist/index.ts',
      'pi-agent-switcher/index.ts',
      'pi-goal-highlight/index.ts',
    ];
const entries = packages.map(path => join(installedRoot ?? workspace, path));
const cli = process.env.PI_TEST_PACKAGE
  ? join(process.env.PI_TEST_PACKAGE, 'dist/bundle/cli.js')
  : resolve(import.meta.dirname, '../node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js');

test('the installed Pi CLI loads all extension entrypoints in an isolated RPC session', {
  skip: (!entries.every(existsSync) || !existsSync(cli)) && 'Run beside all package worktrees and a Pi SDK installation',
  timeout: 90000,
}, async () => {
  const directory = mkdtempSync(join(tmpdir(), 'pi-mailbox-loader-test-'));
  const target = resolve(directory);
  const child = spawn(process.execPath, [
    cli, '--mode', 'rpc', '--no-session', '--no-context-files',
    '--no-skills', '--no-themes', '--no-prompt-templates',
    ...(useInstalledSettings ? [] : ['--no-extensions', ...entries.flatMap(entry => ['-e', entry])]),
  ], {
    cwd: useInstalledSettings ? tmpdir() : directory,
    env: { ...process.env, PI_CODING_AGENT_DIR: useInstalledSettings ? resolve(installedRoot, '..') : join(directory, 'agent'), PI_OFFLINE: '1' },
    stdio: ['pipe', 'pipe', 'pipe'],
    windowsHide: true,
  });
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', chunk => { stderr += chunk; });
  try {
    const commands = await new Promise((resolveCommands, reject) => {
      const timer = setTimeout(() => reject(new Error(`Pi RPC loader timed out: ${stderr.slice(-2000)}`)), 60000);
      let pending = '';
      child.stdout.setEncoding('utf8').on('data', chunk => {
        pending += chunk;
        while (pending.includes('\n')) {
          const index = pending.indexOf('\n');
          const line = pending.slice(0, index).trim();
          pending = pending.slice(index + 1);
          if (!line) continue;
          let record;
          try { record = JSON.parse(line); } catch { continue; }
          if (record.type === 'response' && record.command === 'get_commands') {
            clearTimeout(timer);
            if (record.success) resolveCommands(record.data?.commands ?? []);
            else reject(new Error(JSON.stringify(record.error)));
          }
        }
      });
      child.once('error', error => { clearTimeout(timer); reject(error); });
      child.once('exit', code => { clearTimeout(timer); reject(new Error(`Pi RPC exited ${code}: ${stderr.slice(-2000)}`)); });
      child.stdin.write(`${JSON.stringify({ type: 'get_commands', id: 'loader-smoke' })}\n`);
    });
    const names = new Set(commands.map(command => command.name));
    for (const name of ['mailbox', 'goal', 'run', 'agent', 'goal-highlight-preview']) {
      assert.ok(names.has(name), `Pi did not register /${name}; found ${[...names].join(', ')}`);
    }
  } finally {
    child.stdin.end();
    if (child.exitCode === null) {
      child.kill();
      let timeout;
      await Promise.race([
        new Promise(resolveExit => child.once('exit', resolveExit)),
        new Promise(resolveTimeout => { timeout = setTimeout(resolveTimeout, 5000); }),
      ]);
      clearTimeout(timeout);
    }
    if (!target.startsWith(resolve(tmpdir()) + '\\') && !target.startsWith(resolve(tmpdir()) + '/')) throw new Error('Refusing to delete outside temp');
    if (!basename(target).startsWith('pi-mailbox-loader-test-')) throw new Error('Unexpected fixture name');
    rmSync(target, { recursive: true, force: true, maxRetries: 50, retryDelay: 100 });
  }
});
