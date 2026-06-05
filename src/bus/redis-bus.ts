import type { RedisClientType } from 'redis';
import { ChangeEvent, PubSubBus } from '../types';

/**
 * Redis pub/sub fan-out: the single leader publishes decoded changes; every
 * replica subscribes and matches its own local sessions ("Redis Oplog" pattern).
 *
 * node-redis requires a DEDICATED connection for subscribing, so pass a publisher
 * client plus a factory that yields a fresh connected subscriber per channel.
 */
export interface RedisBusOptions {
  publisher: RedisClientType;
  createSubscriber: () => Promise<RedisClientType>;
}

export class RedisPubSubBus implements PubSubBus {
  constructor(private readonly opts: RedisBusOptions) {}

  async publish(channel: string, event: ChangeEvent): Promise<void> {
    await this.opts.publisher.publish(channel, serialize(event));
  }

  async subscribe(
    channel: string,
    handler: (event: ChangeEvent) => void,
  ): Promise<() => void> {
    const sub = await this.opts.createSubscriber();
    await sub.subscribe(channel, (message: string) => {
      try {
        handler(JSON.parse(message) as ChangeEvent);
      } catch {
        /* drop malformed message */
      }
    });
    return async () => {
      try {
        await sub.unsubscribe(channel);
        await sub.quit();
      } catch {
        /* best effort */
      }
    };
  }
}

/**
 * JSON-serialize a ChangeEvent. `bigint` (e.g. an `int8` column not parsed to a
 * JS number) would throw under JSON.stringify, so coerce it to string. Predicate
 * columns are expected to be small scalars, so this is lossless for matching.
 */
function serialize(event: ChangeEvent): string {
  return JSON.stringify(event, (_key, value) =>
    typeof value === 'bigint' ? value.toString() : value,
  );
}
