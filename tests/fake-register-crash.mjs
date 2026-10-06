import { createSupervisor } from '../src/supervisor.mjs';

const [sessionId, baseDir] = process.argv.slice(2);
if (!sessionId || !baseDir) throw new Error('Expected session ID and fixture directory');
const supervisor = createSupervisor({ sessionId, baseDir });
await supervisor.listen();
supervisor.store.registerJob({ jobId: 'registered-before-crash', coordinatorId: 'root', workflowId: 'flow' });
process.exit(9);
