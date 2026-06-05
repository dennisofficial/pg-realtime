import { randomUUID } from 'crypto';
import { Query } from 'mingo';
import { Pool } from 'pg';
import { InProcessBus } from '../bus/bus';
import { NoopLeaderElector } from '../leader/leader';
import {
  ChangeEvent,
  CoarseScope,
  EngineConfig,
  LeaderElector,
  Logger,
  NOOP_LOGGER,
  OpenSubscriptionArgs,
  PubSubBus,
  Row,
} from '../types';
import { ensurePublication, ensureSlot, ident, resolvePrimaryKey } from './db';
import { ReplicationSource } from './replication-source';
import { ResolvedModel, routingKey } from './resolved-model';
import { mingoAnd, applyGuard } from './rule-guard';
import { SubscriptionImpl } from './snapshot';
import { RealtimeRls } from '../rls';

const DEFAULT_CHANNEL = 'pg_realtime:changes';
const DEFAULT_MAX_ROWS = 10_000;
const LEADERSHIP_TICK_MS = 5_000;

const identity = (r: Row): Row => r;

export class RealtimeEngine {
  private readonly config: EngineConfig;
  private readonly logger: Logger;
  private readonly leader: LeaderElector;
  private readonly bus: PubSubBus;
  private readonly channel: string;
  private readonly maxRows: number;
  private readonly consume: boolean;

  private pool?: Pool;
  private rlsInstance?: RealtimeRls;
  private source?: ReplicationSource;
  private busUnsub?: () => void;
  private leadershipTimer?: ReturnType<typeof setInterval>;
  private holdingLeadership = false;
  private started = false;
  private stopping = false;

  /** name -> resolved model */
  private readonly modelsByName = new Map<string, ResolvedModel>();
  /** routingKey -> resolved models on that table */
  private readonly modelsByTable = new Map<string, ResolvedModel[]>();
  /** routingKey -> pk columns (for the source) */
  private readonly pkByTable = new Map<string, string[]>();
  /** routingKey -> live subscriptions on that table */
  private readonly subsByTable = new Map<string, Set<SubscriptionImpl>>();

  constructor(config: EngineConfig) {
    this.config = config;
    this.logger = config.logger ?? NOOP_LOGGER;
    this.leader = config.leader ?? new NoopLeaderElector();
    this.bus = config.bus ?? new InProcessBus();
    this.channel = config.channel ?? DEFAULT_CHANNEL;
    this.maxRows = config.snapshotMaxRows ?? DEFAULT_MAX_ROWS;
    this.consume = config.consume ?? true;
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.pool = new Pool({ connectionString: this.config.connectionString });

    await this.resolveModels();

    // Server-side authorizer sharing this engine's models + pool, so the SAME guards
    // that scope subscriptions can authorize server-side reads/mutations in-process.
    this.rlsInstance = new RealtimeRls({
      models: this.config.models,
      pool: this.pool,
      maxRows: this.maxRows,
    });

    // Every instance (consumer or follower) subscribes to the bus and matches its own
    // local sessions.
    this.busUnsub = await this.bus.subscribe(this.channel, (ev) => this.route(ev));

    if (this.consume) {
      // Consumer: only the leader reads the slot. Tick now, then on an interval (the
      // interval renews a real elector's lease; Noop is a no-op).
      await this.leadershipTick();
      this.leadershipTimer = setInterval(() => void this.leadershipTick(), LEADERSHIP_TICK_MS);
    }
    // Follower (consume:false): no leadership, no WAL source — it only serves
    // subscriptions, matching changes the consumer publishes onto the bus. Use this in
    // the HTTP tier (which may scale to zero) while a single always-on process consumes.

    this.started = true;
    this.logger.info(
      `pg-realtime engine started (${this.consume ? 'consumer' : 'follower'}; models: ${[...this.modelsByName.keys()].join(', ')})`,
    );
  }

  async stop(): Promise<void> {
    this.stopping = true;
    if (this.leadershipTimer) clearInterval(this.leadershipTimer);
    await this.stopSource();
    try {
      await this.leader.release();
    } catch {
      /* best effort */
    }
    this.busUnsub?.();
    if (this.bus.close) await this.bus.close();
    for (const set of this.subsByTable.values()) {
      for (const sub of [...set]) sub.close();
    }
    this.subsByTable.clear();
    await this.pool?.end();
    this.started = false;
  }

  /**
   * Server-side authorizer backed by this engine's guards + pool. Use it to check
   * RLS on the server (e.g. before a mutation) so it stays in sync with subscriptions.
   * In a process that does NOT run the engine, construct a standalone `RealtimeRls`
   * from the shared models config instead.
   */
  get rls(): RealtimeRls {
    if (!this.rlsInstance) {
      throw new Error('engine not started — construct a standalone RealtimeRls instead');
    }
    return this.rlsInstance;
  }

  async openSubscription(args: OpenSubscriptionArgs): Promise<SubscriptionImpl> {
    if (!this.started || !this.pool) throw new Error('engine not started');
    const model = this.modelsByName.get(args.model);
    if (!model) throw new Error(`unknown model: ${args.model}`);

    const { allowed, scope } = await applyGuard(model.guard, args.user);
    if (!allowed) throw new Error(`forbidden: ${args.model}`);

    // Document mode: parse the pk into per-column equality (mingo) and tighten the
    // snapshot to the single row (coarse SQL).
    let docFilter: Row = {};
    let coarse: CoarseScope = model.coarseScope?.(args.user);
    if (args.pk !== undefined) {
      const values = JSON.parse(args.pk) as unknown[];
      docFilter = Object.fromEntries(model.pkCols.map((c, i) => [c, values[i]]));
      coarse = {
        text: model.pkCols.map((c, i) => `${ident(c)} = $${i + 1}`).join(' AND '),
        values,
      };
    }

    const effFilter = mingoAnd(scope, args.filter, docFilter);
    const effQuery = new Query(effFilter);

    const sub = new SubscriptionImpl({
      id: randomUUID(),
      model,
      effQuery,
      coarse,
      user: args.user,
      pool: this.pool,
      snapshotMaxRows: this.maxRows,
      logger: this.logger,
      onClose: (s) => this.unregister(s),
    });

    let set = this.subsByTable.get(model.routingKey);
    if (!set) {
      set = new Set();
      this.subsByTable.set(model.routingKey, set);
    }
    set.add(sub);

    // Registered now (so routing/buffering captures the snapshot↔stream window);
    // the snapshot begins when the caller attaches its first handler via `on`.
    return sub;
  }

  // ─── internals ───────────────────────────────────────────────────────────

  private async resolveModels(): Promise<void> {
    const pool = this.pool as Pool;
    for (const m of this.config.models) {
      const schema = m.schema ?? 'public';
      const table = m.table;
      const name = m.name ?? table;
      const pkCols = m.primaryKey
        ? Array.isArray(m.primaryKey)
          ? m.primaryKey
          : [m.primaryKey]
        : await resolvePrimaryKey(pool, schema, table);
      if (pkCols.length === 0) {
        throw new Error(
          `pg-realtime: table ${schema}.${table} has no resolvable primary key — ` +
            `declare ModelConfig.primaryKey or add a PK / REPLICA IDENTITY`,
        );
      }
      const key = routingKey(schema, table);
      const resolved: ResolvedModel = {
        name,
        schema,
        table,
        routingKey: key,
        pkCols,
        refetchOnUpdate: m.refetchOnUpdate ?? false,
        mapRow: (m.mapRow as (raw: Row) => Row) ?? identity,
        guard: m.guard,
        coarseScope: m.coarseScope,
      };
      this.modelsByName.set(name, resolved);
      const list = this.modelsByTable.get(key) ?? [];
      list.push(resolved);
      this.modelsByTable.set(key, list);
      this.pkByTable.set(key, pkCols);
    }
  }

  /** Route a bus change to every local subscription on its table. */
  private route(ev: ChangeEvent): void {
    const key = routingKey(ev.schema, ev.table);
    const set = this.subsByTable.get(key);
    if (!set) return;
    for (const sub of set) sub.ingest(ev);
  }

  private unregister(sub: SubscriptionImpl): void {
    const set = this.subsByTable.get(sub.model.routingKey);
    set?.delete(sub);
  }

  private async leadershipTick(): Promise<void> {
    if (this.stopping) return;
    let isLeader = false;
    try {
      isLeader = this.holdingLeadership ? await this.leader.renew() : await this.leader.acquire();
    } catch (err) {
      this.logger.error(`leadership check failed: ${errMsg(err)}`);
      isLeader = false;
    }

    if (isLeader && !this.holdingLeadership) {
      this.holdingLeadership = true;
      this.logger.info('pg-realtime: acquired leadership — starting WAL consumer');
      await this.startSource();
    } else if (!isLeader && this.holdingLeadership) {
      this.holdingLeadership = false;
      this.logger.warn('pg-realtime: lost leadership — stopping WAL consumer');
      await this.stopSource();
    }
  }

  private async startSource(): Promise<void> {
    if (this.source || !this.pool) return;
    // Only the leader touches the publication/slot; both are idempotent.
    const tables = [...this.modelsByTable.values()].map((list) => ({
      schema: list[0].schema,
      table: list[0].table,
    }));
    await ensurePublication(this.pool, this.config.publicationName, tables);
    await ensureSlot(this.pool, this.config.slotName);

    this.source = new ReplicationSource({
      connectionString: this.config.connectionString,
      slotName: this.config.slotName,
      publicationName: this.config.publicationName,
      pkByTable: this.pkByTable,
      // A bus failure (e.g. a NOTIFY payload over the size cap) must not stall the WAL
      // consumer — log and continue rather than reject the change handler.
      onChange: async (ev) => {
        try {
          await this.bus.publish(this.channel, ev);
        } catch (err) {
          this.logger.error(
            `bus publish failed for ${ev.schema}.${ev.table} pk=${ev.pk}: ${errMsg(err)}`,
          );
        }
      },
      logger: this.logger,
    });
    await this.source.start();
  }

  private async stopSource(): Promise<void> {
    if (!this.source) return;
    const src = this.source;
    this.source = undefined;
    await src.stop();
  }
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
