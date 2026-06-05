/**
 * pg-realtime — reusable, read-only realtime-over-Postgres engine. Pure Postgres:
 * leadership and fan-out use advisory locks + LISTEN/NOTIFY, no external stack.
 *
 * Core (transport-agnostic). Optional transports live behind subpaths:
 *   pg-realtime/client        — browser/Node client (onQuery / onDocument)
 *   pg-realtime/socketio      — bind a socket.io Server to an engine
 *   pg-realtime/nest          — NestJS module wrapper
 */
export * from './types';
export { RealtimeEngine } from './engine/engine';
export { RealtimeRls, type RealtimeRlsOptions } from './rls';
export { SubscriptionImpl } from './engine/snapshot';
export { applyMatch, type MatchContext } from './engine/matcher';
export { applyGuard, mingoAnd, type GuardResult } from './engine/rule-guard';
export { toChangeEvent, buildPk, pkColumns } from './engine/normalizer';
export { isToastIncomplete } from './engine/toast';
export type { ResolvedModel } from './engine/resolved-model';
export { NoopLeaderElector } from './leader/leader';
export { InProcessBus } from './bus/bus';
// Postgres-native adapters (no Redis) — need only `pg`, already a core dependency.
export {
  PgAdvisoryLockLeaderElector,
  type PgAdvisoryLockOptions,
} from './leader/pg-advisory-leader';
export { PgNotifyBus, type PgNotifyBusOptions } from './bus/pg-notify-bus';
export { parseLsn, lsnGt, lsnGte, ZERO_LSN } from './lsn';
