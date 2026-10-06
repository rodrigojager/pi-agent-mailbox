import { appendFileSync } from 'node:fs';

export async function runMailboxJob(job) {
  appendFileSync(job.markerPath, 'executed\n');
  const result = { state: 'succeeded', summary: 'computed but never persisted' };
  if (result.state !== 'succeeded') throw new Error('Unexpected fixture result');
  process.exit(9);
}
