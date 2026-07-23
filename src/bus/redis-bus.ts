import superjson from 'superjson';
import type { Redis as RedisClient } from 'ioredis';
import { ChangeEvent, Logger, NOOP_LOGGER, PubSubBus } from '../types';

/**
 * Redis pub/sub fan-out — the scale path. Unlike `PgNotifyBus`, Redis pub/sub has
 * no ~8KB payload cap (Postgres's NOTIFY limit), so wide row images are safe here.
 *
 * Redis pub/sub REQUIRES separate connections for publishing and subscribing (a
 * connection in subscribe mode can't issue other commands), so this bus either
 * takes two already-connected `ioredis` instances or a `url` it uses to create
 * its own pair.
 *
 * `ioredis` is an OPTIONAL peer dependency (like `socket.io`): only the types are
 * imported at the top level (erased at compile time); the actual client is
 * required lazily on first use so consumers who never construct a `RedisBus`
 * don't need `ioredis` installed.
 */
export interface RedisBusOptions {
  /** Connection string. Mutually exclusive with `publisher`/`subscriber`. */
  url?: string;
  /** Pre-connected ioredis instance used for PUBLISH. Provide together with `subscriber`. */
  publisher?: RedisClient;
  /** Pre-connected ioredis instance used for SUBSCRIBE. Provide together with `publisher`. */
  subscriber?: RedisClient;
  /**
   * Namespace prefix for the underlying Redis channel(s) — Redis channels are global
   * to the whole Redis instance (unlike Postgres LISTEN, scoped per connection), so a
   * prefix avoids collisions when other apps share the same Redis. Default:
   * 'pg_realtime_events'.
   */
  channel?: string;
  logger?: Logger;
}

const DEFAULT_CHANNEL_PREFIX = 'pg_realtime_events';

export class RedisBus implements PubSubBus {
  private publisher?: RedisClient;
  private subscriber?: RedisClient;
  private readonly ownsConnections: boolean;
  private readonly url?: string;
  private readonly channelPrefix: string;
  private readonly logger: Logger;
  private readonly handlers = new Map<string, Set<(e: ChangeEvent) => void>>();
  private messageListenerAttached = false;
  private closed = false;
  private connecting?: Promise<void>;

  constructor(opts: RedisBusOptions = {}) {
    if (!opts.url && !(opts.publisher && opts.subscriber)) {
      throw new Error(
        'RedisBus requires either `url` or both `publisher` and `subscriber` ioredis instances',
      );
    }
    this.url = opts.url;
    this.publisher = opts.publisher;
    this.subscriber = opts.subscriber;
    this.ownsConnections = !opts.publisher && !opts.subscriber;
    this.channelPrefix = opts.channel ?? DEFAULT_CHANNEL_PREFIX;
    this.logger = opts.logger ?? NOOP_LOGGER;
  }

  async publish(channel: string, event: ChangeEvent): Promise<void> {
    await this.ensureConnections();
    const payload = superjson.stringify(event);
    await this.publisher!.publish(this.wireChannel(channel), payload);
  }

  async subscribe(channel: string, handler: (event: ChangeEvent) => void): Promise<() => void> {
    let set = this.handlers.get(channel);
    if (!set) {
      set = new Set();
      this.handlers.set(channel, set);
    }
    set.add(handler);

    await this.ensureConnections();
    this.attachMessageListener();
    await this.subscriber!.subscribe(this.wireChannel(channel));

    return () => {
      set?.delete(handler);
    };
  }

  async close(): Promise<void> {
    this.closed = true;
    if (!this.ownsConnections) return;
    try {
      await this.publisher?.quit();
    } catch {
      /* best effort */
    }
    try {
      await this.subscriber?.quit();
    } catch {
      /* best effort */
    }
    this.publisher = undefined;
    this.subscriber = undefined;
  }

  // ─── internals ───────────────────────────────────────────────────────────

  private wireChannel(channel: string): string {
    return `${this.channelPrefix}:${channel}`;
  }

  private async ensureConnections(): Promise<void> {
    if (this.publisher && this.subscriber) return;
    if (this.connecting) return this.connecting;
    this.connecting = this.connect();
    try {
      await this.connecting;
    } finally {
      this.connecting = undefined;
    }
  }

  private async connect(): Promise<void> {
    if (this.publisher && this.subscriber) return;
    const { Redis: IORedis } = await import('ioredis');
    if (!this.publisher) {
      this.publisher = new IORedis(this.url!);
      this.publisher.on('error', (err: unknown) =>
        this.logger.error(`RedisBus publisher error: ${errMsg(err)}`),
      );
    }
    if (!this.subscriber) {
      this.subscriber = new IORedis(this.url!);
      this.subscriber.on('error', (err: unknown) =>
        this.logger.error(`RedisBus subscriber error: ${errMsg(err)}`),
      );
    }
  }

  private attachMessageListener(): void {
    if (this.messageListenerAttached || !this.subscriber) return;
    this.messageListenerAttached = true;
    this.subscriber.on('message', (wireChannel: string, payload: string) => {
      const channel = this.unwireChannel(wireChannel);
      const set = this.handlers.get(channel);
      if (!set) return;
      let event: ChangeEvent;
      try {
        event = superjson.parse<ChangeEvent>(payload);
      } catch (err) {
        this.logger.error(`RedisBus: failed to deserialize message on ${wireChannel}: ${errMsg(err)}`);
        return;
      }
      for (const h of [...set]) h(event);
    });
  }

  private unwireChannel(wireChannel: string): string {
    const prefix = `${this.channelPrefix}:`;
    return wireChannel.startsWith(prefix) ? wireChannel.slice(prefix.length) : wireChannel;
  }
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
