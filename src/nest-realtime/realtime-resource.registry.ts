import { Injectable } from '@nestjs/common';

/**
 * A named, request-shaped realtime resource: the composite/derived subscriptions a client
 * hits with one logical name, rather than subscribing to raw models directly.
 */
export interface RealtimeResourceDefinition<P = unknown> {
  /** The underlying model subscriptions (with optional filter/pk) this resource fans out to. */
  triggers: (
    params: Record<string, unknown>,
  ) => Array<{ model: string; filter?: Record<string, unknown>; pk?: string }>;
  /** Loads the resource's initial snapshot for `params`, scoped to `principal`. */
  load: (
    params: Record<string, unknown>,
    principal: P,
  ) => Promise<Array<{ pk: string; row: Record<string, unknown> }>>;
}

@Injectable()
export class RealtimeResourceRegistry {
  private readonly resources = new Map<string, RealtimeResourceDefinition<never>>();

  register<P>(name: string, def: RealtimeResourceDefinition<P>): void {
    if (this.resources.has(name)) {
      throw new Error(`Realtime resource "${name}" is already registered`);
    }
    this.resources.set(name, def);
  }

  get<P = unknown>(name: string): RealtimeResourceDefinition<P> | null {
    return (this.resources.get(name) as RealtimeResourceDefinition<P> | undefined) ?? null;
  }
}
