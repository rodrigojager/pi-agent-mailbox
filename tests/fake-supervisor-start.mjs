import { createSupervisor } from '../src/supervisor.mjs';

const [sessionId, baseDir] = process.argv.slice(2);
const supervisor = createSupervisor({ sessionId, baseDir });
try {
  await supervisor.listen();
  process.send?.({ state: 'listening' });
  process.on('message', message => {
    if (message === 'stop') void supervisor.close().then(() => process.exit(0));
  });
} catch (error) {
  process.send?.({ state: 'failed', code: error?.code, message: error?.message });
  process.exitCode = 1;
}
