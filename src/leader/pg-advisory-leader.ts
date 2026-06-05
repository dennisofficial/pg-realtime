import { createHash } from 'crypto';
import { Client } from 'pg';
import { Logger, NOOP_LOGGER, LeaderElector } from '../types';

/**
 * Postgres-native leader election via a session-level advisory lock — no Redis.
 *
 * One instance holds `pg_advisory_lock(key)` on a dedicated connection; others get
 * `false` from `pg_try_advisory_lock` and keep trying. A session advisory lock is
 * released automatically when the holder's connection drops, so a dead leader fails
 * over on the next tick with no TTL to tune. Costs one extra connection per instance.
 *
 * Use this when the WAL consumer runs in more than one replica. If the consumer runs
 * in a single-instance process, the default `NoopLeaderElector` already suffices.
 */
export interface PgAdvisoryLockOptions {
  /** Direct connection string (the elector holds its own dedicated connection). */
  connectionString: string;
  /** A stable name hashed to the 64-bit lock key. Default 'pg_realtime'. */
  lockName?: string;
  /** Or pass the lock key directly (overrides lockName). */
  key?: bigint;
  logger?: Logger;
}

function keyFromName(name: string): bigint {
  // First 8 bytes of sha256, as a signed 64-bit int (the advisory-lock key space).
  const digest = createHash('sha256').update(name).digest();
  return digest.readBigInt64BE(0);
}

export class PgAdvisoryLockLeaderElector implements LeaderElector {
  private client?: Client;
  private held = false;
  private readonly key: bigint;
  private readonly connectionString: string;
  private readonly logger: Logger;

  constructor(opts: PgAdvisoryLockOptions) {
    this.connectionString = opts.connectionString;
    this.key = opts.key ?? keyFromName(opts.lockName ?? 'pg_realtime');
    this.logger = opts.logger ?? NOOP_LOGGER;
  }

  async acquire(): Promise<boolean> {
    if (this.held) return this.renew();
    try {
      const client = await this.ensureClient();
      const res = await client.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_lock($1::bigint) AS locked',
        [this.key.toString()],
      );
      this.held = res.rows[0]?.locked === true;
      return this.held;
    } catch (err) {
      this.logger.error(`advisory-lock acquire failed: ${errMsg(err)}`);
      await this.reset();
      return false;
    }
  }

  async renew(): Promise<boolean> {
    // A session advisory lock persists with the connection; renewing just confirms
    // the connection (and therefore the lock) is still alive.
    if (!this.held || !this.client) return false;
    try {
      await this.client.query('SELECT 1');
      return true;
    } catch (err) {
      this.logger.warn(`advisory-lock renew lost the connection: ${errMsg(err)}`);
      await this.reset();
      return false;
    }
  }

  async release(): Promise<void> {
    if (this.client && this.held) {
      try {
        await this.client.query('SELECT pg_advisory_unlock($1::bigint)', [this.key.toString()]);
      } catch {
        /* connection may already be gone; the lock releases on disconnect anyway */
      }
    }
    await this.reset();
  }

  private async ensureClient(): Promise<Client> {
    if (this.client) return this.client;
    const client = new Client({ connectionString: this.connectionString });
    client.on('error', () => {
      // Connection died -> we no longer hold the lock; next acquire() reconnects.
      this.held = false;
      this.client = undefined;
    });
    await client.connect();
    this.client = client;
    return client;
  }

  private async reset(): Promise<void> {
    this.held = false;
    const client = this.client;
    this.client = undefined;
    if (client) {
      try {
        await client.end();
      } catch {
        /* best effort */
      }
    }
  }
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
