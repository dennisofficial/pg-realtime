import { Client, Pool } from 'pg';
import { ChangeEvent, Logger, NOOP_LOGGER, PubSubBus } from '../types';

/**
 * Postgres-native fan-out via LISTEN/NOTIFY — no Redis. The leader publishes each
 * decoded change with `pg_notify`; every replica LISTENs and matches its own
 * sessions. The listener connection auto-reconnects and re-LISTENs, because a
 * silently-dropped notification would leave clients stale.
 *
 * Caveat: Postgres caps a NOTIFY payload at 8000 bytes. That is ample for typical
 * realtime rows (ids, statuses, versions), but a row image larger than that cannot
 * ride this bus — publish logs an error and drops that single change. For tables
 * with large columns, keep the matched/rendered columns small (a refetch-on-receive
 * bus that lifts the cap is a possible future addition).
 */
export interface PgNotifyBusOptions {
  /** Direct connection string. The bus manages its own listener + publisher connections. */
  connectionString: string;
  logger?: Logger;
}

const PAYLOAD_LIMIT = 7990; // a little under Postgres's 8000-byte NOTIFY cap
const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 8_000;

export class PgNotifyBus implements PubSubBus {
  private listener?: Client;
  private publisher?: Pool;
  private closed = false;
  private reconnectAttempts = 0;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private readonly handlers = new Map<string, Set<(e: ChangeEvent) => void>>();
  private readonly connectionString: string;
  private readonly logger: Logger;

  constructor(opts: PgNotifyBusOptions) {
    this.connectionString = opts.connectionString;
    this.logger = opts.logger ?? NOOP_LOGGER;
  }

  async publish(channel: string, event: ChangeEvent): Promise<void> {
    const payload = serialize(event);
    if (Buffer.byteLength(payload, 'utf8') > PAYLOAD_LIMIT) {
      this.logger.error(
        `PgNotifyBus: change for ${event.schema}.${event.table} pk=${event.pk} exceeds the ` +
          `NOTIFY payload limit and was dropped — keep matched/rendered columns small`,
      );
      return;
    }
    const pool = this.ensurePublisher();
    await pool.query('SELECT pg_notify($1, $2)', [channel, payload]);
  }

  async subscribe(channel: string, handler: (e: ChangeEvent) => void): Promise<() => void> {
    let set = this.handlers.get(channel);
    if (!set) {
      set = new Set();
      this.handlers.set(channel, set);
    }
    set.add(handler);

    const listener = await this.ensureListener();
    await listener.query(`LISTEN ${quoteIdent(channel)}`);

    return () => {
      set?.delete(handler);
    };
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    try {
      await this.listener?.end();
    } catch {
      /* best effort */
    }
    try {
      await this.publisher?.end();
    } catch {
      /* best effort */
    }
    this.listener = undefined;
    this.publisher = undefined;
  }

  // ─── internals ───────────────────────────────────────────────────────────

  private ensurePublisher(): Pool {
    if (!this.publisher) {
      this.publisher = new Pool({ connectionString: this.connectionString, max: 2 });
      this.publisher.on('error', (err) => this.logger.error(`PgNotifyBus publisher error: ${errMsg(err)}`));
    }
    return this.publisher;
  }

  private async ensureListener(): Promise<Client> {
    if (this.listener) return this.listener;
    const client = new Client({ connectionString: this.connectionString });
    client.on('notification', (msg) => this.dispatch(msg.channel, msg.payload));
    client.on('error', (err) => this.onListenerLost(err));
    await client.connect();
    this.listener = client;
    this.reconnectAttempts = 0;
    // (Re-)LISTEN every known channel — covers both first connect and reconnect.
    for (const channel of this.handlers.keys()) {
      await client.query(`LISTEN ${quoteIdent(channel)}`);
    }
    return client;
  }

  private dispatch(channel: string, payload: string | undefined): void {
    if (!payload) return;
    const set = this.handlers.get(channel);
    if (!set) return;
    let event: ChangeEvent;
    try {
      event = JSON.parse(payload) as ChangeEvent;
    } catch {
      return;
    }
    for (const h of [...set]) h(event);
  }

  private onListenerLost(err: unknown): void {
    if (this.closed) return;
    this.logger.warn(`PgNotifyBus listener dropped, reconnecting: ${errMsg(err)}`);
    this.listener = undefined;
    this.reconnectAttempts += 1;
    const delay = Math.min(
      RECONNECT_MAX_MS,
      RECONNECT_BASE_MS * 2 ** (this.reconnectAttempts - 1),
    );
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => {
      void this.ensureListener().catch((e) => this.onListenerLost(e));
    }, delay);
  }
}

function serialize(event: ChangeEvent): string {
  return JSON.stringify(event, (_key, value) =>
    typeof value === 'bigint' ? value.toString() : value,
  );
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
