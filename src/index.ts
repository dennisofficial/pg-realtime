export { InProcessBus } from './bus/bus';
export { RealtimeEngine } from './engine/engine';
export { applyMatch, type MatchContext } from './engine/matcher';
export { buildPk, pkColumns, toChangeEvent } from './engine/normalizer';
export type { ResolvedModel } from './engine/resolved-model';
export { applyGuard, mingoAnd, type GuardResult } from './engine/rule-guard';
export { SubscriptionImpl } from './engine/snapshot';
export { isToastIncomplete } from './engine/toast';
export { NoopLeaderElector } from './leader/leader';
export { RealtimeRls, type RealtimeRlsOptions } from './rls';
export * from './types';
// Postgres-native adapters (no Redis) — need only `pg`, already a core dependency.
export { PgNotifyBus, type PgNotifyBusOptions } from './bus/pg-notify-bus';
export {
  PgAdvisoryLockLeaderElector,
  type PgAdvisoryLockOptions,
} from './leader/pg-advisory-leader';
// Redis-backed adapter (scale path, no ~8KB payload cap) — needs `ioredis` (optional peer).
export { RedisBus, type RedisBusOptions } from './bus/redis-bus';
export { ZERO_LSN, lsnGt, lsnGte, parseLsn } from './lsn';
