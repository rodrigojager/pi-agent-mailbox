export async function runMailboxJob(job, onProgress, signal) {
  onProgress({ activity: 'running fixture' });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, job.delayMs ?? 20);
    signal.addEventListener('abort', () => { clearTimeout(timer); reject(new Error('cancelled')); }, { once: true });
  });
  if (job.fail) throw new Error('fixture failure');
  return { state: 'succeeded', summary: job.summaryLength ? 'X'.repeat(job.summaryLength) : `finished ${job.jobId}`, details: { result: job.jobId } };
}
