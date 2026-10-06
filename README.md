# Pi Agent Mailbox

An installable Pi extension and local supervisor for event-driven subagent coordination. It works with the matching `pi-subagent` adapter and the optional `pi-goal` mailbox bridge. No Pi core files are changed.

## How it works

The coordinator registers a job before the supervisor starts a child. A Node worker uses the `pi-subagent` runtime to execute it. Root-session workers are detached; on POSIX, nested supervisors and workers inherit their Pi child's process group so cancelling the parent can terminate the whole nested tree. The supervisor writes terminal state and a pending delivery to a per-session SQLite journal before notifying the Pi extension over authenticated local IPC. The extension routes a `subagent-result` to the owning session and branch, then acknowledges it only after finding its event ID in Pi history. Reconnection replays the journal. `goal_wait` can arm an `any` or `all` wait before sleeping; ordinary `subagent_wait` also subscribes before waiting. Neither wait asks the model to check status repeatedly.

Retention removes only acknowledged results with no active wait. Startup and daily cleanup process old jobs in bounded batches, yielding to IPC between batches; a checkpoint runs after each batch.

The journal lives under `~/.pi/agent/state/pi-agent-mailbox/v1/<session-hash>/`. Large results are separate, hashed artifacts. The worker and supervisor use the installed Node executable; Node 26 or newer is required for `node:sqlite`. The supervisor stops after its clients disconnect and its workers finish, while the journal remains for recovery. Startup holds a separate SQLite lock until the endpoint is bound and journal recovery finishes, so competing supervisors cannot reconcile a live worker. On Unix, a crash may leave the socket pathname behind; a new supervisor probes it and removes it only if it is still a socket and refuses connections. An active endpoint or another file is preserved.

## Install and activate

Install this package from GitHub alongside compatible `pi-subagent`, `pi-goal`, `pi-agent-switcher`, and `pi-goal-highlight` versions:

```sh
pi install https://github.com/rodrigojager/pi-agent-mailbox
```

Load only one copy of each extension. New Pi sessions load the new code; an already running session needs `/reload` while idle. Existing child processes cannot be transferred into the mailbox and should finish under their original mechanism.

`/mailbox` reports connection and ownership. `/mailbox claim` takes delivery ownership after the previous owner disconnects. A second Pi interface can observe but cannot start, cancel, or acknowledge jobs until it owns the session. `/mailbox legacy` selects the original in-process `pi-subagent` path for **new** jobs in the current branch; `/mailbox durable` restores mailbox execution. Both keep the mailbox consumer loaded for outstanding results.

The orchestrator can call `subagent_wait` with exact `job_ids`, `mode: any|all`, and an optional safety deadline. It can inspect a job with `subagent_status` when there is a specific reason. With an active goal, `goal_wait` accepts `subagents: { job_ids, mode }`. A wait timeout does not cancel the children. `/cancel-subagent` remains the explicit cancellation path. Closing the Pi window does not cancel children.

## Guarantees and limits

- A terminal result and its pending delivery are committed together. Replay and history-ID deduplication provide at-least-once delivery while the local journal and destination session survive. A transient ACK failure is retried without sending the recorded message again. An ACK means the result was recorded in Pi history, not that the model acted on it.
- Navigating to a fork or an ancestor creates a separate workflow when it would otherwise inherit the same branch marker. Returning to a branch that already owns jobs preserves its workflow and replays its pending results.
- A crash around child startup may leave the execution **unknown**. The same job is never relaunched automatically after an uncertain start. A late result artifact can reconcile that state.
- Heartbeats and lack-of-progress warnings update the UI without waking the model. Silence alone does not mark a child failed.
- A slow client may miss ephemeral progress frames. Its socket backlog is capped at 4 MiB; reconnecting replays committed events, including every terminal result.
- If a journal write fails, the supervisor reports storage unavailable and refuses new jobs until it is restarted after the storage problem is fixed. An already running worker can still leave an artifact for recovery; the coordinator never treats an uncommitted result as delivered.
- The supervisor persists final results, but an OS or disk failure before an artifact or journal commit can lose the result. There is no atomic transaction spanning Pi history, SQLite, and a child’s external side effects.
- Nested subagents continue to use `pi-subagent`’s existing synchronous child path. Their parent’s durable completion remains journaled. The mailbox does not reconstruct a lost child conversation.

No credentials or environment dumps are written to the journal. Keep the state directory private and backed up if these results matter. Do not delete it while jobs or deliveries are pending.

Recorded results become eligible for pruning after 30 days by default. Startup and daily maintenance remove old event payloads and artifacts only after every delivery for that job has been recorded in Pi history and no active or ready wait refers to it. Pending deliveries, indeterminate jobs, and small job-ID tombstones remain. `PI_AGENT_MAILBOX_RETENTION_DAYS` accepts 0 to disable pruning or 1–3650 days to change the period. Maintenance uses a passive WAL checkpoint; `/mailbox` reports a checkpoint or artifact cleanup warning.

## Development

Run `npm test` and `npm run typecheck`. Tests cover replay, ownership, worker execution, disconnects, waits, ACKs, cancellation, and the real adapter/goal bridge when their sibling package worktrees are present.
