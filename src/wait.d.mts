export function waitForSubagents(client: import('./client.mjs').MailboxClient, options: {
  coordinatorId: string; workflowId: string; jobIds: string[];
  mode: 'any' | 'all'; timeoutMs: number; signal?: AbortSignal;
}): Promise<any>;
