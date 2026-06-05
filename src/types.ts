/**
 * Core, transport-agnostic types for the pg-realtime engine.
 *
 * The data flow is:
 *   Postgres WAL ─► ReplicationSource (leader only) ─► ChangeEvent
 *               ─► PubSubBus ─► every replica ─► per-session matcher ─► RowDelta ─► client
 */

/** A raw row keyed by Postgres column name. Values are already parsed by pgoutput. */
export type Row = Record<string, unknown>;

export type Op = 'insert' | 'update' | 'delete';

/**
 * A normalized change, independent of table/transport. Republished on the bus so
 * every replica can match it against its own local sessions.
 */
export interface ChangeEvent {
  op: Op;
  schema: string;
  table: string;
  /** Stable stringified primary key (composite keys are JSON-encoded arrays). */
  pk: string;
  /** New row image. `null` on DELETE. May contain TOAST placeholders — see `toastIncomplete`. */
  row: Row | null;
  /** Old row image (`key`/`old` from WAL). `null` when Postgres did not provide one. */
  oldRow: Row | null;
  /**
   * True when the UPDATE image is missing the current value of an unchanged TOASTed
   * (large `text`/`jsonb`/…) column — pgoutput sends those as `undefined`, regardless
   * of REPLICA IDENTITY. Predicate columns are expected to be small (always present);
   * this flags that the *payload* may need a refetch (see `ModelConfig.refetchOnUpdate`).
   */
  toastIncomplete: boolean;
  /** The committing transaction's `commitEndLsn` — the visibility-ordered LSN. */
  lsn: string;
}

/** A delta pushed toward a client. Mirrors the Mongo reference's `data/add/update/remove`. */
export type RowDelta =
  | { kind: 'data'; rows: Array<{ pk: string; row: Row }> }
  | { kind: 'add'; pk: string; row: Row }
  | { kind: 'update'; pk: string; row: Row }
  | { kind: 'remove'; pk: string };

/** A Mongo-style filter object, evaluated by mingo. */
export type MingoFilter = Record<string, unknown>;

/**
 * Row-level authorization, mirroring the Mongo reference's `RealtimeRuleGuard`.
 * `canRead` returns:
 *   - a `MingoFilter` → scope: rows must also match this filter (ANDed with the client filter)
 *   - `true`          → allow all rows of this model
 *   - `false`         → deny the subscription entirely
 *
 * The returned scope is a mingo filter (not SQL): membership is decided by ONE engine
 * (mingo) on both the snapshot and the live path, so snapshot and live can never diverge.
 */
export type GuardDecision = MingoFilter | boolean | Promise<MingoFilter | boolean>;
export type GuardAction = 'read' | 'create' | 'update' | 'delete';

export abstract class RealtimeRuleGuard<U = unknown, R extends Row = Row> {
  /** Which rows the user may read. The realtime engine uses ONLY this one. Required. */
  abstract canRead(user: U | null): GuardDecision;
  /** Which candidate rows the user may insert. Optional — falls back to `canRead`. */
  canCreate?(user: U | null): GuardDecision;
  /** Which existing rows the user may update. Optional — falls back to `canRead`. */
  canUpdate?(user: U | null): GuardDecision;
  /** Which existing rows the user may delete. Optional — falls back to `canRead`. */
  canDelete?(user: U | null): GuardDecision;
}

/**
 * An optional, COARSE SQL pre-filter for the snapshot SELECT. It exists purely to bound
 * how many rows the snapshot pulls — it is NOT a security boundary. It MUST be a superset
 * of the guard+filter (never exclude a row mingo would match), or the snapshot under-delivers.
 * Security is always enforced by the mingo guard, never by this.
 */
export type CoarseScope = { text: string; values: unknown[] } | undefined;

export interface ModelConfig<R extends Row = Row> {
  /** Table name as it appears in the publication / WAL relation. */
  table: string;
  /** Schema, default 'public'. */
  schema?: string;
  /** Logical model name clients subscribe by. Defaults to `table`. */
  name?: string;
  /** Primary key column(s). Defaults to the relation's replica-identity key columns. */
  primaryKey?: string | string[];
  /**
   * Set true for tables with large columns clients render (e.g. MDX content, JSON blobs):
   * on an UPDATE whose WAL image dropped a TOAST column, the engine refetches the full row
   * by PK before emitting, so clients never cache a placeholder. Tables with only small
   * columns leave this false and push the WAL row directly (zero extra DB hit).
   */
  refetchOnUpdate?: boolean;
  /** Maps a raw (snake_case PG) row to the shape clients expect. Default: identity. */
  mapRow?: (raw: Row) => R;
  /** Row-level auth scope. */
  guard?: RealtimeRuleGuard<any, R>;
  /** Optional coarse SQL scope for the snapshot fetch (perf only; must be a superset). */
  coarseScope?: (user: unknown) => CoarseScope;
}

/** Exactly one process may consume the replication slot. */
export interface LeaderElector {
  /** Resolves true if THIS process now holds (or retains) leadership. */
  acquire(): Promise<boolean>;
  /** Extend the lease; resolves false if leadership was lost. */
  renew(): Promise<boolean>;
  release(): Promise<void>;
}

/** Fan-out bus. The leader publishes; every replica subscribes and matches locally. */
export interface PubSubBus {
  publish(channel: string, event: ChangeEvent): Promise<void> | void;
  /** Returns an unsubscribe function. */
  subscribe(
    channel: string,
    handler: (event: ChangeEvent) => void,
  ): Promise<() => void> | (() => void);
  close?(): Promise<void> | void;
}

export interface EngineConfig {
  /** Standard libpq connection string. Used for snapshots/refetch and (in replication mode) WAL. */
  connectionString: string;
  /** Logical replication slot name, e.g. 'pg_realtime_slot'. */
  slotName: string;
  /** Publication name, e.g. 'pg_realtime_pub'. */
  publicationName: string;
  models: ModelConfig[];
  /** Default: NoopLeaderElector (single instance). */
  leader?: LeaderElector;
  /** Default: InProcessBus. */
  bus?: PubSubBus;
  /** Bus channel for fan-out. Default: 'pg_realtime:changes'. */
  channel?: string;
  /** Defensive cap on snapshot rows per subscription. Default: 10000. */
  snapshotMaxRows?: number;
  /** Optional logger. Defaults to a no-op (the engine stays quiet by design). */
  logger?: Logger;
}

export interface Logger {
  debug(msg: string): void;
  info(msg: string): void;
  warn(msg: string): void;
  error(msg: string): void;
}

export const NOOP_LOGGER: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

export interface OpenSubscriptionArgs {
  /** Model name (or table name) to subscribe to. */
  model: string;
  /** The authenticated principal, passed to the guard. */
  user?: unknown;
  /** Client-supplied mingo filter. */
  filter?: MingoFilter;
  /** Document mode: subscribe to a single row by its stringified PK. */
  pk?: string;
}

export interface Subscription {
  readonly id: string;
  /** Register a delta handler. The first delta is always a `data` snapshot. */
  on(handler: (delta: RowDelta) => void): void;
  close(): void;
}
