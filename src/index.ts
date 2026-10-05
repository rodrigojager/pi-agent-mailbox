import { join, resolve } from 'node:path';
import { homedir } from 'node:os';
import { createReadStream, existsSync } from 'node:fs';
import { readFile, stat } from 'node:fs/promises';
import { randomUUID, createHash } from 'node:crypto';
import { Type } from 'typebox';
import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent';
import { connectOrStartSupervisor, type MailboxClient } from './client.mjs';
import { statePath } from './protocol.mjs';
import { waitForSubagents } from './wait.mjs';

const DEFAULT_BASE_DIR = join(homedir(), '.pi', 'agent', 'state', 'pi-agent-mailbox', 'v1');
export const MAILBOX_REQUEST_CHANNEL = 'rodrigojager:pi-agent-mailbox:request:v1';
export const MAILBOX_EVENT_CHANNEL = 'rodrigojager:pi-agent-mailbox:event:v1';
export const MAILBOX_CANCEL_CHANNEL = 'rodrigojager:pi-agent-mailbox:cancel:v1';
export const MAILBOX_GOAL_WAIT_CHANNEL = 'pi:agent-job-wait:v1';
export const MAILBOX_HEALTH_CHANNEL = 'rodrigojager:pi-agent-mailbox:health:v1';
export const MAILBOX_WAIT_ACTIVITY_CHANNEL = 'rodrigojager:pi-agent-mailbox:wait-activity:v1';

type MailboxEvent = {
  event_id: string;
  event_type: string;
  job_id: string;
  payload?: { state?: string; summary?: string; resultRef?: string; resultSha256?: string };
};
type GoalBinding = {
  waitId: string; goalId: string; sessionId: string; workflowId: string;
  jobIds: string[]; mode: 'any' | 'all';
  committed: boolean; settled: boolean; flushing: boolean;
};

export default function registerAgentMailbox(pi: ExtensionAPI, options: { baseDir?: string } = {}) {
  const baseDir = options.baseDir ?? DEFAULT_BASE_DIR;
  let client: MailboxClient | undefined;
  let connectedSession: string | undefined;
  let currentContext: ExtensionContext | undefined;
  let unsubscribe: (() => void) | undefined;
  let heartbeatDeadline: ReturnType<typeof setTimeout> | undefined;
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  let reconnectAttempts = 0;
  let connecting: Promise<MailboxClient> | undefined;
  let connectingSession: string | undefined;
  let connectionEpoch = 0;
  const inFlight = new Set<string>();
  const deliveryAttempts = new Map<string, number>();
  const deliveryChecks = new Map<string, ReturnType<typeof setTimeout>>();
  let goalBinding: GoalBinding | undefined;

  const setHealth = (ctx: ExtensionContext, state: 'responsive' | 'disconnected' | 'suspected_stall' | 'observer') => {
    pi.events.emit(MAILBOX_HEALTH_CHANNEL, { sessionId: ctx.sessionManager.getSessionId(), state });
    if (!ctx.hasUI) return;
    const label = state === 'responsive' ? undefined
      : state === 'suspected_stall' ? '⚠ mailbox sem heartbeat'
        : state === 'observer' ? 'mailbox: sessão observadora' : '⚠ mailbox desconectado';
    ctx.ui.setStatus('pi-agent-mailbox-health', label);
  };

  const workflowFor = (ctx: ExtensionContext): string | undefined => {
    const marker = [...ctx.sessionManager.getBranch()].reverse()
      .find(entry => entry.type === 'custom' && entry.customType === 'mailbox-workflow');
    if (marker?.type !== 'custom') return undefined;
    const id = (marker.data as { id?: unknown } | undefined)?.id;
    return typeof id === 'string' ? id : undefined;
  };

  const ensureWorkflow = (ctx: ExtensionContext): string => {
    const existing = workflowFor(ctx);
    if (existing) return existing;
    const id = randomUUID();
    pi.appendEntry('mailbox-workflow', { id, sessionId: ctx.sessionManager.getSessionId() });
    return id;
  };

  const executionModeFor = (ctx: ExtensionContext): 'durable' | 'legacy' => {
    const marker = [...ctx.sessionManager.getBranch()].reverse()
      .find(entry => entry.type === 'custom' && entry.customType === 'mailbox-execution-mode');
    return marker?.type === 'custom' && (marker.data as { mode?: unknown } | undefined)?.mode === 'legacy'
      ? 'legacy' : 'durable';
  };

  const disconnect = () => {
    connectionEpoch++;
    if (heartbeatDeadline) clearTimeout(heartbeatDeadline);
    if (reconnectTimer) clearTimeout(reconnectTimer);
    heartbeatDeadline = undefined;
    reconnectTimer = undefined;
    unsubscribe?.();
    unsubscribe = undefined;
    const previous = client;
    client = undefined;
    connectedSession = undefined;
    previous?.close();
    inFlight.clear();
    deliveryAttempts.clear();
    for (const timer of deliveryChecks.values()) clearTimeout(timer);
    deliveryChecks.clear();
    goalBinding = undefined;
  };

  const scheduleReconnect = (ctx: ExtensionContext) => {
    if (reconnectTimer || currentContext !== ctx) return;
    const delay = Math.min(30000, 500 * 2 ** Math.min(reconnectAttempts++, 6));
    reconnectTimer = setTimeout(() => {
      reconnectTimer = undefined;
      if (currentContext !== ctx) return;
      void connectFor(ctx).catch(() => scheduleReconnect(ctx));
    }, delay);
    reconnectTimer.unref?.();
  };

  const armHeartbeat = (opened: MailboxClient, ctx: ExtensionContext) => {
    if (heartbeatDeadline) clearTimeout(heartbeatDeadline);
    heartbeatDeadline = setTimeout(() => {
      if (client !== opened || currentContext !== ctx) return;
      setHealth(ctx, 'suspected_stall');
      opened.close();
    }, 45000);
    heartbeatDeadline.unref?.();
  };

  const historyEntryFor = (ctx: ExtensionContext, eventId: string) =>
    ctx.sessionManager.getBranch().find(entry =>
      entry.type === 'custom_message' && entry.customType === 'subagent-result' &&
      ((entry.details as { mailboxEventId?: string; mailboxEventIds?: string[] } | undefined)?.mailboxEventId === eventId ||
        (entry.details as { mailboxEventIds?: string[] } | undefined)?.mailboxEventIds?.includes(eventId)),
    );

  const ackRecorded = (ctx: ExtensionContext, eventId: string) => {
    if (!client || connectedSession !== ctx.sessionManager.getSessionId()) return;
    const entry = historyEntryFor(ctx, eventId);
    if (!entry) return;
    inFlight.delete(eventId);
    deliveryAttempts.delete(eventId);
    const timer = deliveryChecks.get(eventId);
    if (timer) clearTimeout(timer);
    deliveryChecks.delete(eventId);
    void client.request('ack', { eventId, entryId: entry.id }).catch(() => {});
  };

  const scheduleDeliveryCheck = (ctx: ExtensionContext, eventIds: string[]) => {
    for (const eventId of eventIds) {
      if (deliveryChecks.has(eventId)) continue;
      const attempts = deliveryAttempts.get(eventId) ?? 1;
      const delay = Math.min(60000, 5000 * 2 ** Math.min(attempts - 1, 4));
      const timer = setTimeout(() => {
        deliveryChecks.delete(eventId);
        if (currentContext !== ctx || connectedSession !== ctx.sessionManager.getSessionId()) return;
        ackRecorded(ctx, eventId);
        if (!inFlight.has(eventId)) return;
        inFlight.delete(eventId);
        if (goalBinding) void flushGoalWait().catch(() => {});
        else void replayForCurrentBranch(ctx).catch(() => {});
      }, delay);
      timer.unref?.();
      deliveryChecks.set(eventId, timer);
    }
  };

  const readResult = async (sessionId: string, jobId: string, resultRef: string | undefined, expectedHash?: string) => {
    if (!resultRef) return undefined;
    const expected = resolve(statePath(baseDir, sessionId), 'artifacts', `${jobId}.json`);
    if (resolve(resultRef) !== expected) throw new Error('Result path does not belong to the session');
    const bytes = (await stat(expected)).size;
    if (bytes > 1024 * 1024) {
      if (expectedHash) {
        const hash = createHash('sha256');
        for await (const chunk of createReadStream(expected)) hash.update(chunk);
        if (hash.digest('hex') !== expectedHash) throw new Error('Subagent result artifact hash does not match the journal');
      }
      return { omitted: true, summary: undefined, details: undefined } as const;
    }
    const serialized = await readFile(expected, 'utf8');
    if (expectedHash && createHash('sha256').update(serialized).digest('hex') !== expectedHash) {
      throw new Error('Subagent result artifact hash does not match the journal');
    }
    return JSON.parse(serialized) as { summary?: string; details?: unknown; omitted?: false };
  };

  const summarize = (value: unknown, resultRef?: string) => {
    const text = String(value ?? '');
    return text.length <= 32768 ? text : `${text.slice(0, 32768)}\n[Result truncated; full artifact: ${resultRef ?? 'mailbox journal'}]`;
  };

  const waitingGoalId = (ctx: ExtensionContext): string | undefined => {
    const latest = [...ctx.sessionManager.getBranch()].reverse()
      .find(entry => entry.type === 'custom' && entry.customType === 'goal-state');
    if (latest?.type !== 'custom') return undefined;
    const goal = (latest.data as { goal?: { id?: string; status?: string; waiting?: unknown } } | undefined)?.goal;
    return goal?.status === 'active' && goal.waiting ? goal.id : undefined;
  };

  const latestGoal = (ctx: ExtensionContext): { id?: string; status?: string } | undefined => {
    const entry = [...ctx.sessionManager.getBranch()].reverse()
      .find(value => value.type === 'custom' && value.customType === 'goal-state');
    return entry?.type === 'custom'
      ? (entry.data as { goal?: { id?: string; status?: string } } | undefined)?.goal
      : undefined;
  };

  async function flushGoalWait() {
    const binding = goalBinding;
    const ctx = currentContext;
    if (!binding || !ctx || !client?.isOwner || !binding.committed || !binding.settled || binding.flushing) return;
    if (ctx.sessionManager.getSessionId() !== binding.sessionId || workflowFor(ctx) !== binding.workflowId) return;
    binding.flushing = true;
    try {
      if (waitingGoalId(ctx) !== binding.goalId) {
        goalBinding = undefined;
        const events = await client.request('wait_results', { waitId: binding.waitId }) as MailboxEvent[];
        for (const event of events) await deliver(event);
        return;
      }
      const status = await client.request('wait_status', { waitId: binding.waitId });
      pi.events.emit(MAILBOX_WAIT_ACTIVITY_CHANNEL, {
        sessionId: binding.sessionId, goalId: binding.goalId, waitId: binding.waitId,
        statuses: status?.statuses, ready: status?.ready,
      });
      if (!status?.ready) return;
      const events = await client.request('wait_results', { waitId: binding.waitId }) as MailboxEvent[];
      const pending = events.filter(event => !historyEntryFor(ctx, event.event_id));
      if (!pending.length) { goalBinding = undefined; return; }
      if (pending.some(event => inFlight.has(event.event_id))) return;
      const messages: string[] = [];
      const results: unknown[] = [];
      for (const event of pending) {
        const result = await readResult(binding.sessionId, event.job_id, event.payload?.resultRef, event.payload?.resultSha256);
        const summary = summarize(result?.summary ?? event.payload?.summary ?? event.event_type, event.payload?.resultRef);
        messages.push(`${event.job_id}: ${summary}${result?.omitted ? `\n[Full result artifact: ${event.payload?.resultRef}]` : ''}`);
        if (result?.details && typeof result.details === 'object') {
          const records = (result.details as { results?: unknown[] }).results;
          if (Array.isArray(records)) results.push(...records);
        }
        inFlight.add(event.event_id);
        deliveryAttempts.set(event.event_id, (deliveryAttempts.get(event.event_id) ?? 0) + 1);
      }
      try {
        pi.sendMessage({
          customType: 'subagent-result',
          content: messages.join('\n\n'),
          display: true,
          details: {
            mode: 'single', agentScope: 'both', projectAgentsDir: null, results,
            mailboxEventIds: pending.map(event => event.event_id), mailboxWaitId: binding.waitId,
          },
        }, { deliverAs: 'followUp', triggerTurn: true });
      } finally {
        scheduleDeliveryCheck(ctx, pending.map(event => event.event_id));
      }
      setImmediate(() => {
        if (currentContext === ctx) for (const event of pending) ackRecorded(ctx, event.event_id);
      });
    } finally {
      binding.flushing = false;
    }
  }

  async function deliver(event: MailboxEvent) {
    if (event.event_type === 'heartbeat') {
      if (client && currentContext) armHeartbeat(client, currentContext);
      return;
    }
    if (!client?.isOwner) return;
    pi.events.emit(MAILBOX_EVENT_CHANNEL, event);
    if (!['terminal', 'supervision_lost', 'worker_exit', 'worker_error', 'persistence_failed'].includes(event.event_type)) return;
    const ctx = currentContext;
    const sessionId = connectedSession;
    if (!ctx || !sessionId || ctx.sessionManager.getSessionId() !== sessionId) return;
    const workflowId = workflowFor(ctx);
    if (!workflowId) return;
    const owner = await client?.request('job', { jobId: event.job_id });
    if (!owner || owner.coordinator_id !== sessionId || owner.workflow_id !== workflowId) return;
    if (historyEntryFor(ctx, event.event_id)) {
      ackRecorded(ctx, event.event_id);
      return;
    }
    if (goalBinding?.sessionId === sessionId && goalBinding.jobIds.includes(event.job_id)) {
      await flushGoalWait();
      return;
    }
    if (inFlight.has(event.event_id)) return;
    const result = await readResult(sessionId, event.job_id, event.payload?.resultRef, event.payload?.resultSha256);
    const details = result?.details && typeof result.details === 'object'
      ? result.details as Record<string, unknown>
      : { mode: 'single', agentScope: 'both', projectAgentsDir: null, results: [] };
    const goal = latestGoal(ctx);
    const activeGoal = owner.goal_id
      ? goal?.id === owner.goal_id && goal?.status === 'active'
      : !goal || goal.status !== 'active';
    inFlight.add(event.event_id);
    deliveryAttempts.set(event.event_id, (deliveryAttempts.get(event.event_id) ?? 0) + 1);
    try {
      pi.sendMessage({
        customType: 'subagent-result',
        content: `${summarize(result?.summary ?? event.payload?.summary ?? `Subagent ${event.job_id}: ${event.event_type}`, event.payload?.resultRef)}${result?.omitted ? `\n[Full result artifact: ${event.payload?.resultRef}]` : ''}`,
        display: true,
        details: { ...details, mailboxEventId: event.event_id, mailboxJobId: event.job_id },
      }, { deliverAs: 'followUp', triggerTurn: activeGoal });
    } finally {
      scheduleDeliveryCheck(ctx, [event.event_id]);
    }
    // message_end precedes host persistence in the streaming path.
    setImmediate(() => { if (currentContext === ctx) ackRecorded(ctx, event.event_id); });
  }

  async function connectFor(ctx: ExtensionContext): Promise<MailboxClient> {
    const sessionId = ctx.sessionManager.getSessionId();
    currentContext = ctx;
    if (client && connectedSession === sessionId && !client.closed) return client;
    if (connecting && connectingSession === sessionId) return connecting;
    const retainedBinding = goalBinding?.sessionId === sessionId ? goalBinding : undefined;
    disconnect();
    goalBinding = retainedBinding;
    const epoch = connectionEpoch;
    const attempt = (async () => {
    const opened = await connectOrStartSupervisor({ sessionId, baseDir });
      if (epoch !== connectionEpoch || currentContext !== ctx || ctx.sessionManager.getSessionId() !== sessionId) {
        opened.close();
        throw new Error('Mailbox session changed during connection');
      }
      client = opened;
      connectedSession = sessionId;
      opened.onClose(() => {
        if (client !== opened || currentContext !== ctx) return;
        if (heartbeatDeadline) clearTimeout(heartbeatDeadline);
        heartbeatDeadline = undefined;
        setHealth(ctx, 'disconnected');
        scheduleReconnect(ctx);
      });
      armHeartbeat(opened, ctx);
      try {
        unsubscribe = await opened.subscribe(0, event => {
          void deliver(event as MailboxEvent).catch(error => ctx.ui?.notify(
            `Mailbox delivery error: ${error instanceof Error ? error.message : String(error)}`, 'error',
          ));
        });
      } catch (error) {
        opened.close();
        throw error;
      }
      reconnectAttempts = 0;
      setHealth(ctx, opened.isOwner ? 'responsive' : 'observer');
      if (goalBinding?.sessionId === sessionId) void flushGoalWait();
      return opened;
    })();
    connecting = attempt;
    connectingSession = sessionId;
    try { return await attempt; }
    finally { if (connecting === attempt) { connecting = undefined; connectingSession = undefined; } }
  }

  async function replayForCurrentBranch(ctx: ExtensionContext) {
    const opened = await connectFor(ctx);
    let after = 0;
    while (true) {
      const events = await opened.request('events', { after, limit: 1000 }) as MailboxEvent[];
      for (const event of events) await deliver(event);
      if (events.length === 0) return;
      after = (events.at(-1) as MailboxEvent & { seq: number }).seq;
    }
  }

  pi.events.on(MAILBOX_REQUEST_CHANNEL, data => {
    const request = data as {
      context?: ExtensionContext;
      accept?: (start: (job: Record<string, unknown>, adapterPath: string) => Promise<unknown>) => void;
    } | undefined;
    if (!request?.context || typeof request.accept !== 'function' || executionModeFor(request.context) === 'legacy') return;
    request.accept(async (job, adapterPath) => {
      const opened = await connectFor(request.context!);
      const sessionId = request.context!.sessionManager.getSessionId();
      const workflowId = ensureWorkflow(request.context!);
      const goal = latestGoal(request.context!);
      return opened.request('start', {
        job: { ...job, coordinatorId: sessionId, workflowId, goalId: goal?.status === 'active' ? goal.id : null }, adapterPath,
      });
    });
  });
  pi.events.on(MAILBOX_CANCEL_CHANNEL, data => {
    const request = data as {
      context?: ExtensionContext;
      sessionId?: string;
      accept?: (cancel: (jobId: string) => Promise<unknown>) => void;
    } | undefined;
    if (!request?.context || typeof request.accept !== 'function') return;
    request.accept(async jobId => {
      const targetSession = request.sessionId ?? request.context!.sessionManager.getSessionId();
      if (targetSession === connectedSession && client && !client.closed) return client.request('cancel', { jobId });
      const opened = await connectOrStartSupervisor({ sessionId: targetSession, baseDir });
      try { return await opened.request('cancel', { jobId }); }
      finally { opened.close(); }
    });
  });
  pi.events.on(MAILBOX_GOAL_WAIT_CHANNEL, data => {
    const request = data as {
      context?: ExtensionContext;
      accept?: (bridge: {
        arm: (input: { goalId: string; jobIds: string[]; mode: 'any' | 'all' }) => Promise<unknown>;
        commit: (waitId: string) => void;
      }) => void;
    } | undefined;
    if (!request?.context || typeof request.accept !== 'function') return;
    request.accept({
      arm: async ({ goalId, jobIds, mode }) => {
        const opened = await connectFor(request.context!);
        const sessionId = request.context!.sessionManager.getSessionId();
        const waitId = randomUUID();
        const workflowId = ensureWorkflow(request.context!);
        const status = await opened.request('watch', {
          wait: { waitId, coordinatorId: sessionId, workflowId, jobIds, mode },
        });
        pi.events.emit(MAILBOX_WAIT_ACTIVITY_CHANNEL, {
          sessionId, goalId, waitId, statuses: status.statuses, ready: status.ready,
        });
        if (!status.ready) {
          goalBinding = { waitId, goalId, sessionId, workflowId, jobIds, mode, committed: false, settled: false, flushing: false };
        }
        return status;
      },
      commit: waitId => {
        const binding = goalBinding;
        if (!binding || binding.waitId !== waitId || !request.context || request.context.sessionManager.getSessionId() !== binding.sessionId) return;
        binding.committed = true;
        pi.appendEntry('mailbox-goal-wait', {
          waitId, goalId: binding.goalId, sessionId: binding.sessionId, workflowId: binding.workflowId,
          jobIds: binding.jobIds, mode: binding.mode,
        });
      },
    });
  });

  pi.on('session_start', (_event, ctx) => {
    currentContext = ctx;
    const marker = [...ctx.sessionManager.getBranch()].reverse()
      .find(entry => entry.type === 'custom' && entry.customType === 'mailbox-goal-wait');
    const saved = marker?.type === 'custom' ? marker.data as Partial<GoalBinding> | undefined : undefined;
    if (saved?.goalId && saved?.waitId && saved.sessionId === ctx.sessionManager.getSessionId() && saved.workflowId === workflowFor(ctx) && Array.isArray(saved.jobIds) && (saved.mode === 'any' || saved.mode === 'all') && waitingGoalId(ctx) === saved.goalId) {
      goalBinding = {
        waitId: saved.waitId, goalId: saved.goalId, sessionId: saved.sessionId, workflowId: saved.workflowId!,
        jobIds: saved.jobIds, mode: saved.mode, committed: true, settled: true, flushing: false,
      };
    }
    const filename = join(statePath(baseDir, ctx.sessionManager.getSessionId()), 'journal.sqlite');
    if (existsSync(filename)) void connectFor(ctx).catch(() => {});
  });
  pi.on('session_before_switch', () => { disconnect(); currentContext = undefined; });
  pi.on('session_shutdown', () => { disconnect(); currentContext = undefined; });
  pi.on('session_tree', (event, ctx) => {
    currentContext = ctx;
    if (event.oldLeafId && event.newLeafId) {
      const oldBranch = ctx.sessionManager.getBranch(event.oldLeafId);
      const newBranch = ctx.sessionManager.getBranch(event.newLeafId);
      let common = 0;
      while (common < oldBranch.length && common < newBranch.length && oldBranch[common].id === newBranch[common].id) common++;
      if (common < oldBranch.length && common < newBranch.length) {
        const lastMarker = newBranch.findLastIndex(entry => entry.type === 'custom' && entry.customType === 'mailbox-workflow');
        if (lastMarker >= 0 && lastMarker < common) {
          pi.appendEntry('mailbox-workflow', { id: randomUUID(), sessionId: ctx.sessionManager.getSessionId() });
        }
      }
    }
    if (client) void replayForCurrentBranch(ctx).catch(() => {});
  });
  const onBoundary = (ctx: ExtensionContext, settled: boolean) => {
    currentContext = ctx;
    if (!client) return;
    for (const eventId of inFlight) ackRecorded(ctx, eventId);
    if (settled && goalBinding) {
      goalBinding.settled = true;
      void flushGoalWait().catch(error => ctx.ui?.notify(`Mailbox wait error: ${error}`, 'error'));
    }
  };
  pi.on('agent_settled', (_event, ctx) => onBoundary(ctx, true));
  pi.on('turn_end', (_event, ctx) => onBoundary(ctx, false));
  pi.on('message_end', (_event, ctx) => onBoundary(ctx, false));
  pi.registerCommand('mailbox', {
    description: 'Inspect the mailbox; /mailbox claim takes ownership, /mailbox legacy or durable selects new-job execution',
    handler: async (args, ctx) => {
      try {
        if (args.trim() === 'legacy' || args.trim() === 'durable') {
          const mode = args.trim();
          pi.appendEntry('mailbox-execution-mode', { mode });
          ctx.ui?.notify(`New subagent jobs will use ${mode} execution`, 'info');
          return;
        }
        const opened = await connectFor(ctx as ExtensionContext);
        if (args.trim() === 'claim') {
          const claim = await opened.claim();
          setHealth(ctx as ExtensionContext, claim.owner ? 'responsive' : 'observer');
          if (claim.owner) void replayForCurrentBranch(ctx as ExtensionContext);
          ctx.ui?.notify(claim.owner ? 'Mailbox delivery ownership acquired' : 'Mailbox remains owned by another interface', claim.owner ? 'info' : 'warning');
          return;
        }
        const result = await opened.request('ping');
        ctx.ui?.notify(`Mailbox ${opened.isOwner ? 'owner' : 'observer'} · ${new Date(result.at).toLocaleTimeString()}`, 'info');
      } catch (error) {
        ctx.ui?.notify(`Mailbox unavailable: ${error instanceof Error ? error.message : String(error)}`, 'error');
      }
    },
  });

  pi.registerTool({
    name: 'subagent_wait',
    label: 'Wait for subagents',
    description: 'Wait for completion or failure of selected subagents. Uses events; a timeout is a safety deadline, not a polling interval.',
    parameters: Type.Object({
      job_ids: Type.Array(Type.String({ minLength: 1 }), { minItems: 1, maxItems: 64 }),
      mode: Type.Union([Type.Literal('any'), Type.Literal('all')]),
      timeout_ms: Type.Optional(Type.Integer({ minimum: 1000, maximum: 43200000 })),
    }),
    async execute(_callId, params, signal, _onUpdate, ctx) {
      const opened = await connectFor(ctx);
      const sessionId = ctx.sessionManager.getSessionId();
      const workflowId = ensureWorkflow(ctx);
      const result = await waitForSubagents(opened, {
        coordinatorId: sessionId,
        workflowId,
        jobIds: params.job_ids,
        mode: params.mode,
        timeoutMs: params.timeout_ms ?? 300000,
        signal,
      });
      return { content: [{ type: 'text', text: JSON.stringify(result) }], details: {} };
    },
  });

  pi.registerTool({
    name: 'subagent_status',
    label: 'Subagent status',
    description: 'Inspect a subagent only when a status check is explicitly needed.',
    parameters: Type.Object({ job_id: Type.String({ minLength: 1 }) }),
    async execute(_callId, params, _signal, _onUpdate, ctx) {
      const opened = await connectFor(ctx);
      const job = await opened.request('job', { jobId: params.job_id });
      return { content: [{ type: 'text', text: JSON.stringify(job) }], details: {} };
    },
  });
}
