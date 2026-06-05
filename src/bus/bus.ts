import { ChangeEvent, PubSubBus } from '../types';

export type { PubSubBus };

/**
 * Default bus: in-process EventEmitter-style fan-out. Correct when the leader and
 * the matchers run in the SAME process (single-instance, tests). Multi-replica
 * deployments supply `RedisPubSubBus` (from `pg-realtime/bus/redis`) so the single
 * leader's decoded changes reach every replica's matchers.
 */
export class InProcessBus implements PubSubBus {
  private readonly handlers = new Map<string, Set<(e: ChangeEvent) => void>>();

  publish(channel: string, event: ChangeEvent): void {
    const set = this.handlers.get(channel);
    if (!set) return;
    // Copy so a handler unsubscribing mid-iteration can't disturb the loop.
    for (const h of [...set]) h(event);
  }

  subscribe(channel: string, handler: (event: ChangeEvent) => void): () => void {
    let set = this.handlers.get(channel);
    if (!set) {
      set = new Set();
      this.handlers.set(channel, set);
    }
    set.add(handler);
    return () => {
      set?.delete(handler);
    };
  }
}
