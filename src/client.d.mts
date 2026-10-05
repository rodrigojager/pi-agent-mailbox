export class MailboxClient {
  closed: boolean;
  isOwner: boolean;
  generation: number;
  request(op: string, data?: Record<string, unknown>, timeoutMs?: number): Promise<any>;
  claim(): Promise<{ owner: boolean; generation: number }>;
  subscribe(after: number, listener: (event: any) => void): Promise<() => void>;
  onClose(listener: () => void): () => void;
  close(): void;
}
export function connectOrStartSupervisor(options: { sessionId: string; baseDir: string; startupTimeoutMs?: number }): Promise<MailboxClient>;
