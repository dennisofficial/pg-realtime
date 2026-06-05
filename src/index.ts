/**
 * pg-realtime — reusable, read-only realtime-over-Postgres engine.
 *
 * Core (transport- and Redis-agnostic). Optional adapters live behind subpaths:
 *   pg-realtime/client        — browser/Node client (onQuery / onDocument)
 *   pg-realtime/socketio      — bind a socket.io Server to an engine
 *   pg-realtime/nest          — NestJS module wrapper
 *   pg-realtime/leader/redis  — Redis SET NX EX leader election
 *   pg-realtime/bus/redis     — Redis pub/sub fan-out bus
 */
export * from './types';
export { RealtimeEngine } from './engine/engine';
export { SubscriptionImpl } from './engine/snapshot';
export { applyMatch, type MatchContext } from './engine/matcher';
export { applyGuard, mingoAnd, type GuardResult } from './engine/rule-guard';
export { toChangeEvent, buildPk, pkColumns } from './engine/normalizer';
export { isToastIncomplete } from './engine/toast';
export type { ResolvedModel } from './engine/resolved-model';
export { NoopLeaderElector } from './leader/leader';
export { InProcessBus } from './bus/bus';
export { parseLsn, lsnGt, lsnGte, ZERO_LSN } from './lsn';
