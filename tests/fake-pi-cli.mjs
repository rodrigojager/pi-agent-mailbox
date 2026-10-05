import { writeFileSync } from 'node:fs';
import { join } from 'node:path';

writeFileSync(join(process.cwd(), 'mailbox-child-argv.json'), JSON.stringify(process.argv.slice(2)), 'utf8');
const message = {
  role: 'assistant', content: [{ type: 'text', text: 'offline child completed' }],
  api: 'fixture', provider: 'fixture', model: 'fixture-model',
  usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { total: 0 } },
  stopReason: 'stop', timestamp: Date.now(),
};
process.stdout.write(`${JSON.stringify({ type: 'message_end', message })}\n`);
process.stdout.write(`${JSON.stringify({ type: 'agent_end', messages: [message] })}\n`);
