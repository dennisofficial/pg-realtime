import { randomUUID } from 'crypto';
import type { RedisClientType } from 'redis';
import { LeaderElector } from '../types';

/**
 * Mirrors the lease pattern the cubix control plane already uses
 * (node-staleness / rollout-controller): `SET key token NX PX ttl`, renewed each
 * tick, released with a token-checked compare-and-del so a process never deletes
 * a lease it no longer owns. Exactly one engine instance becomes leader and
 * consumes the replication slot.
 *
 * Pass a connected node-redis v4 client.
 */
export interface RedisLeaderOptions {
  client: RedisClientType;
  /** Lease key. Default 'pg_realtime:leader'. */
  key?: string;
  /** Lease TTL in ms. Renew on an interval shorter than this (the engine ticks at 5s). */
  ttlMs?: number;
}

const RENEW_LUA =
  "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end";
const RELEASE_LUA =
  "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

export class RedisLeaderElector implements LeaderElector {
  private readonly client: RedisClientType;
  private readonly key: string;
  private readonly ttlMs: number;
  private readonly token = randomUUID();

  constructor(opts: RedisLeaderOptions) {
    this.client = opts.client;
    this.key = opts.key ?? 'pg_realtime:leader';
    this.ttlMs = opts.ttlMs ?? 15_000;
  }

  async acquire(): Promise<boolean> {
    const res = await this.client.set(this.key, this.token, { NX: true, PX: this.ttlMs });
    if (res === 'OK') return true;
    // We may already hold the lease (e.g. acquire called again before renew).
    return this.renew();
  }

  async renew(): Promise<boolean> {
    const res = await this.client.eval(RENEW_LUA, {
      keys: [this.key],
      arguments: [this.token, String(this.ttlMs)],
    });
    return res === 1;
  }

  async release(): Promise<void> {
    await this.client.eval(RELEASE_LUA, { keys: [this.key], arguments: [this.token] });
  }
}
