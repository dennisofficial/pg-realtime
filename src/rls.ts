import { Query } from 'mingo';
import { Pool } from 'pg';
import { ident, resolvePrimaryKey, selectByPkValues, selectRows } from './engine/db';
import { applyGuard, mingoAnd } from './engine/rule-guard';
import {
  CoarseScope,
  GuardAction,
  MingoFilter,
  ModelConfig,
  RealtimeRuleGuard,
  Row,
} from './types';

/**
 * Server-side authorizer built from the SAME `RealtimeRuleGuard`s the realtime engine
 * uses — so a subscription, a server-side list, and a server-side row check all derive
 * from one source of truth.
 *
 * It is standalone: construct it from your models config anywhere (e.g. the panel
 * backend handling a power-on), with no running engine. The pure checks (`scope`,
 * `authorize`, `filterRows`) need no database; `query`/`get` need a pool.
 *
 *   const rls = new RealtimeRls({ models, connectionString });
 *   const server = await rls.get({ model: 'servers', user, pk: [serverId] });
 *   if (!server) throw new ForbiddenException();   // not found OR not permitted
 *   // ...power it on
 */
export interface RealtimeRlsOptions {
  models: ModelConfig[];
  /** Pool (or connection string) for `query`/`get`. The pure checks need neither. */
  pool?: Pool;
  connectionString?: string;
  /** Defensive cap on `query` rows. Default 10000. */
  maxRows?: number;
}

interface RlsModel {
  name: string;
  schema: string;
  table: string;
  mapRow: (raw: Row) => Row;
  guard?: RealtimeRuleGuard<any, Row>;
  coarseScope?: (user: unknown) => CoarseScope;
  primaryKey?: string[];
}

const identity = (r: Row): Row => r;
const DEFAULT_MAX_ROWS = 10_000;

export class RealtimeRls {
  private readonly models = new Map<string, RlsModel>();
  private readonly maxRows: number;
  private pool?: Pool;
  private readonly ownsPool: boolean;
  private readonly pkCache = new Map<string, string[]>();

  constructor(opts: RealtimeRlsOptions) {
    this.maxRows = opts.maxRows ?? DEFAULT_MAX_ROWS;
    if (opts.pool) {
      this.pool = opts.pool;
      this.ownsPool = false;
    } else if (opts.connectionString) {
      this.pool = new Pool({ connectionString: opts.connectionString });
      this.ownsPool = true;
    } else {
      this.ownsPool = false;
    }

    for (const m of opts.models) {
      const name = m.name ?? m.table;
      this.models.set(name, {
        name,
        schema: m.schema ?? 'public',
        table: m.table,
        mapRow: (m.mapRow as (raw: Row) => Row) ?? identity,
        guard: m.guard,
        coarseScope: m.coarseScope,
        primaryKey: m.primaryKey
          ? Array.isArray(m.primaryKey)
            ? m.primaryKey
            : [m.primaryKey]
          : undefined,
      });
    }
  }

  /** The effective mingo filter for (user, action), ANDed with an optional caller filter. */
  async scope(args: {
    model: string;
    user?: unknown;
    action?: GuardAction;
    filter?: MingoFilter;
  }): Promise<{ allowed: boolean; filter: MingoFilter }> {
    const m = this.requireModel(args.model);
    const { allowed, scope } = await applyGuard(m.guard, args.user, args.action ?? 'read');
    if (!allowed) return { allowed: false, filter: {} };
    return { allowed: true, filter: mingoAnd(scope, args.filter) };
  }

  /** Is `user` allowed `action` on an already-fetched `row`? Pure — no DB. */
  async authorize(args: {
    model: string;
    user?: unknown;
    row: Row;
    action?: GuardAction;
  }): Promise<boolean> {
    const { allowed, filter } = await this.scope({
      model: args.model,
      user: args.user,
      action: args.action,
    });
    if (!allowed) return false;
    const m = this.requireModel(args.model);
    return new Query(filter).test(m.mapRow(args.row));
  }

  /** Filter an in-memory array down to what the user may access. Pure — no DB. */
  async filterRows(args: {
    model: string;
    user?: unknown;
    rows: Row[];
    action?: GuardAction;
    filter?: MingoFilter;
  }): Promise<Row[]> {
    const { allowed, filter } = await this.scope({
      model: args.model,
      user: args.user,
      action: args.action,
      filter: args.filter,
    });
    if (!allowed) return [];
    const m = this.requireModel(args.model);
    const q = new Query(filter);
    return args.rows.map((r) => m.mapRow(r)).filter((r) => q.test(r));
  }

  /**
   * Fetch the rows a user may read — the same coarse SQL + mingo a subscription's
   * snapshot would return, so a server-side list matches the realtime view. Needs a pool.
   */
  async query(args: {
    model: string;
    user?: unknown;
    filter?: MingoFilter;
    pk?: string | unknown[];
  }): Promise<Row[]> {
    const pool = this.requirePool();
    const m = this.requireModel(args.model);
    const { allowed, filter } = await this.scope({
      model: args.model,
      user: args.user,
      action: 'read',
      filter: args.filter,
    });
    if (!allowed) return [];

    let coarse: CoarseScope = m.coarseScope?.(args.user);
    if (args.pk !== undefined) {
      const pkCols = await this.pkColumns(m);
      const values = pkValues(args.pk);
      coarse = {
        text: pkCols.map((c, i) => `${ident(c)} = $${i + 1}`).join(' AND '),
        values,
      };
    }

    const rows = await selectRows(pool, m.schema, m.table, coarse, this.maxRows);
    const q = new Query(filter);
    return rows.map((r) => m.mapRow(r)).filter((r) => q.test(r));
  }

  /**
   * Fetch one row by primary key and return it only if `user` is permitted `action`
   * (else null — indistinguishable from "not found", which is usually what you want).
   * Needs a pool. `pk` is the stringified-array form (as `onDocument` uses) or an array.
   */
  async get(args: {
    model: string;
    user?: unknown;
    pk: string | unknown[];
    action?: GuardAction;
  }): Promise<Row | null> {
    const pool = this.requirePool();
    const m = this.requireModel(args.model);
    const pkCols = await this.pkColumns(m);
    const raw = await selectByPkValues(pool, m.schema, m.table, pkCols, pkValues(args.pk));
    if (!raw) return null;
    const ok = await this.authorize({
      model: args.model,
      user: args.user,
      row: raw,
      action: args.action,
    });
    return ok ? m.mapRow(raw) : null;
  }

  async close(): Promise<void> {
    if (this.ownsPool && this.pool) await this.pool.end();
  }

  // ─── internals ───────────────────────────────────────────────────────────

  private requireModel(name: string): RlsModel {
    const m = this.models.get(name);
    if (!m) throw new Error(`pg-realtime RLS: unknown model '${name}'`);
    return m;
  }

  private requirePool(): Pool {
    if (!this.pool) {
      throw new Error(
        'pg-realtime RLS: a pool or connectionString is required for query()/get()',
      );
    }
    return this.pool;
  }

  private async pkColumns(m: RlsModel): Promise<string[]> {
    if (m.primaryKey) return m.primaryKey;
    const cached = this.pkCache.get(m.name);
    if (cached) return cached;
    const cols = await resolvePrimaryKey(this.requirePool(), m.schema, m.table);
    if (cols.length === 0) {
      throw new Error(`pg-realtime RLS: no primary key resolvable for ${m.schema}.${m.table}`);
    }
    this.pkCache.set(m.name, cols);
    return cols;
  }
}

function pkValues(pk: string | unknown[]): unknown[] {
  return Array.isArray(pk) ? pk : (JSON.parse(pk) as unknown[]);
}
