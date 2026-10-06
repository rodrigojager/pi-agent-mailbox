import { createSupervisor } from '../src/supervisor.mjs';

const [sessionId, baseDir] = process.argv.slice(2);
if (!sessionId || !baseDir) throw new Error('Expected session ID and fixture directory');
const supervisor = createSupervisor({ sessionId, baseDir });
await supervisor.listen();
supervisor.store.registerJob({ jobId: 'commit-before-notify', coordinatorId: 'root', workflowId: 'flow' });
supervisor.store.publish({
  eventId: 'committed-without-notify', jobId: 'commit-before-notify', eventType: 'terminal',
  executionState: 'succeeded', payload: { summary: 'durable before IPC publication' },
});
process.exit(9);
