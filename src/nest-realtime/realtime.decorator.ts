import 'reflect-metadata';

const REALTIME_PUBLISH = Symbol.for('pg-realtime:publish');

export interface RealtimePublishOptions {
  /** Model name clients subscribe by. Defaults to the entity's table name. */
  name?: string;
}

/**
 * Marks an entity as live-publishable through the realtime engine. Discovered at boot by
 * `buildRealtimeModels` — an entity WITHOUT this decorator is never subscribable, regardless
 * of what columns it has. Which properties actually reach the wire is decided by the host
 * app's `resolveExposed` plug passed into `buildRealtimeModels` (e.g. an `@Expose()`-style
 * decorator from `@workspace/nestjs-rls`).
 *
 *   @Entity({ name: 'jobs' })
 *   @Realtime()
 *   export class Job { ... }
 */
export function Realtime(options: RealtimePublishOptions = {}): ClassDecorator {
  return (target) => {
    Reflect.defineMetadata(REALTIME_PUBLISH, options, target);
  };
}

/** Reads the `@Realtime` options off an entity class, or `undefined` if undecorated. */
export function getRealtimePublish(target: Function): RealtimePublishOptions | undefined {
  return Reflect.getMetadata(REALTIME_PUBLISH, target) as RealtimePublishOptions | undefined;
}
