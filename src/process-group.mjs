/** Nested mailbox processes must stay in their Pi child's process group on POSIX. */
export function detachMailboxProcess(platform = process.platform, depth = process.env.PI_SUBAGENT_DEPTH) {
  if (platform === 'win32') return true;
  const parsed = Number(depth ?? '0');
  return !(Number.isFinite(parsed) && parsed >= 1);
}
