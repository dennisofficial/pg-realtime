import { LogicalReplicationService, PgoutputPlugin, Pgoutput } from 'pg-logical-replication';
import { ChangeEvent, Logger, NOOP_LOGGER } from '../types';
import { toChangeEvent } from './normalizer';
import { routingKey } from './resolved-model';

type ChangeMessage = Pgoutput.MessageInsert | Pgoutput.MessageUpdate | Pgoutput.MessageDelete;

export interface ReplicationSourceDeps {
  connectionString: string;
  slotName: string;
  publicationName: string;
  /** `${schema}.${table}` -> primary-key columns, for the tables we watch. */
  pkByTable: Map<string, string[]>;
  /** Called once per change, in commit order, BEFORE the commit is acknowledged. */
  onChange: (ev: ChangeEvent) => Promise<void> | void;
  logger?: Logger;
}

const BACKOFF_BASE_MS = 500;
const BACKOFF_MAX_MS = 8_000;

/**
 * Consumes the logical replication slot (LEADER ONLY). Buffers each transaction
 * and, on commit, normalizes its changes — stamping the transaction's
 * `commitEndLsn` — hands them to `onChange`, then acknowledges. Acknowledging only
 * after `onChange` makes delivery at-least-once: a crash mid-transaction redelivers
 * the whole transaction, which the per-session Set + PK dedup renders idempotent.
 */
export class ReplicationSource {
  private service?: LogicalReplicationService;
  private buffer: ChangeMessage[] = [];
  private stopped = false;
  private attempts = 0;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private readonly logger: Logger;

  constructor(private readonly deps: ReplicationSourceDeps) {
    this.logger = deps.logger ?? NOOP_LOGGER;
  }

  async start(): Promise<void> {
    this.stopped = false;
    await this.connect();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    try {
      await this.service?.stop();
    } catch {
      /* best effort */
    }
    this.service = undefined;
    this.buffer = [];
  }

  private async connect(): Promise<void> {
    if (this.stopped) return;
    const plugin = new PgoutputPlugin({
      protoVersion: 1,
      publicationNames: [this.deps.publicationName],
    });
    const service = new LogicalReplicationService(
      { connectionString: this.deps.connectionString },
      // Manual ack (we ack at commit, after publishing) + backpressure so our async
      // handler fully completes before the next WAL message is delivered (ordering).
      { acknowledge: { auto: false, timeoutSeconds: 0 }, flowControl: { enabled: true } },
    );
    this.service = service;

    service.on('start', () => {
      this.attempts = 0; // healthy stream — reset backoff
    });
    service.on('data', (lsn: string, log: Pgoutput.Message) => this.onData(lsn, log));
    service.on('heartbeat', (lsn: string, _ts: number, shouldRespond: boolean) => {
      if (shouldRespond) void service.acknowledge(lsn);
    });
    service.on('error', (err: Error) => this.onFailure(err));

    // subscribe() resolves when the stream ends and rejects on connection failure
    // (including "replication slot N is active" during a leadership handoff).
    service.subscribe(plugin, this.deps.slotName).then(
      () => {
        if (!this.stopped) this.onFailure(new Error('replication stream ended'));
      },
      (err: unknown) => this.onFailure(err),
    );
  }

  private async onData(lsn: string, log: Pgoutput.Message): Promise<void> {
    switch (log.tag) {
      case 'begin':
        this.buffer = [];
        break;
      case 'insert':
      case 'update':
      case 'delete':
        this.buffer.push(log);
        break;
      case 'commit':
        await this.flush(log.commitEndLsn ?? lsn);
        break;
      default:
        // relation/type/origin/truncate/message — the relation is embedded in each
        // change message, so nothing to do here.
        break;
    }
  }

  private async flush(commitEndLsn: string): Promise<void> {
    const batch = this.buffer;
    this.buffer = [];
    for (const msg of batch) {
      const key = routingKey(msg.relation.schema, msg.relation.name);
      const pk = this.deps.pkByTable.get(key);
      if (!pk) continue; // not a watched table
      const ev = toChangeEvent(msg, commitEndLsn, pk);
      await this.deps.onChange(ev);
    }
    await this.service?.acknowledge(commitEndLsn);
  }

  private onFailure(err: unknown): void {
    if (this.stopped) return;
    const message = err instanceof Error ? err.message : String(err);
    this.attempts += 1;
    const delay = Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * 2 ** (this.attempts - 1));
    const jitter = Math.floor(delay * 0.2 * deterministicJitter(this.attempts));
    const wait = delay + jitter;
    // Slot-active during a leadership handoff is expected and transient — stay quiet
    // for the first couple of attempts, then warn.
    if (this.attempts >= 3) {
      this.logger.warn(`replication source reconnecting (attempt ${this.attempts}): ${message}`);
    }
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => {
      void this.connect().catch((e) => this.onFailure(e));
    }, wait);
  }
}

function deterministicJitter(n: number): number {
  const x = Math.sin(n * 12.9898) * 43758.5453;
  return x - Math.floor(x);
}
