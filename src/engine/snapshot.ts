import { Query } from 'mingo';
import { Pool } from 'pg';
import { lsnGt, ZERO_LSN } from '../lsn';
import {
  ChangeEvent,
  CoarseScope,
  Logger,
  NOOP_LOGGER,
  Row,
  RowDelta,
  Subscription as ISubscription,
} from '../types';
import { refetchByPk, snapshotTable } from './db';
import { applyMatch, MatchContext } from './matcher';
import { buildPk } from './normalizer';
import { ResolvedModel } from './resolved-model';

type State = 'init' | 'snapshotting' | 'replaying' | 'live' | 'closed';

export interface SubscriptionDeps {
  id: string;
  model: ResolvedModel;
  /** The single membership query (guard scope ∧ client filter ∧ document pk). */
  effQuery: Query;
  /** Coarse SQL scope for the snapshot fetch (perf only; superset of effQuery). */
  coarse: CoarseScope;
  user: unknown;
  pool: Pool;
  snapshotMaxRows: number;
  logger?: Logger;
  onClose: (sub: SubscriptionImpl) => void;
}

/**
 * One client subscription, implementing the BUFFERING → SNAPSHOTTING → REPLAYING
 * → LIVE state machine that closes the snapshot↔stream phantom hole.
 *
 * Ordering is enforced by a single serial worker (`kick`/`work`): only one event
 * is applied at a time, in arrival order, even across the `refetch` await.
 */
export class SubscriptionImpl implements ISubscription {
  readonly id: string;
  readonly model: ResolvedModel;
  readonly documentIds = new Set<string>();

  private readonly effQuery: Query;
  private readonly coarse: CoarseScope;
  private readonly user: unknown;
  private readonly pool: Pool;
  private readonly maxRows: number;
  private readonly logger: Logger;
  private readonly onCloseCb: (s: SubscriptionImpl) => void;

  private handlers: Array<(d: RowDelta) => void> = [];
  private mailbox: ChangeEvent[] = [];
  private working = false;
  private startedFlag = false;
  private snapshotDone = false;
  private snapshotLsn: string = ZERO_LSN;
  private state: State = 'init';

  constructor(deps: SubscriptionDeps) {
    this.id = deps.id;
    this.model = deps.model;
    this.effQuery = deps.effQuery;
    this.coarse = deps.coarse;
    this.user = deps.user;
    this.pool = deps.pool;
    this.maxRows = deps.snapshotMaxRows;
    this.logger = deps.logger ?? NOOP_LOGGER;
    this.onCloseCb = deps.onClose;
  }

  on(handler: (delta: RowDelta) => void): void {
    this.handlers.push(handler);
    // Lazily begin the snapshot on the first handler, so the `data` emit is never
    // lost to a not-yet-attached listener.
    if (!this.startedFlag) this.start();
  }

  /**
   * Engine routes every change for this model's table here. Events are buffered
   * from the moment the subscription is registered (even before `start`), so the
   * snapshot↔stream window is captured; they are only processed once started.
   */
  ingest(ev: ChangeEvent): void {
    if (this.state === 'closed') return;
    this.mailbox.push(ev);
    if (this.startedFlag) this.kick();
  }

  /** Begin the snapshot → replay → live machine. Idempotent; also triggered by `on`. */
  start(): void {
    if (this.startedFlag || this.state === 'closed') return;
    this.startedFlag = true;
    this.kick();
  }

  close(): void {
    if (this.state === 'closed') return;
    this.state = 'closed';
    this.mailbox = [];
    this.handlers = [];
    this.onCloseCb(this);
  }

  // ─── internals ───────────────────────────────────────────────────────────

  private emit(delta: RowDelta): void {
    for (const h of this.handlers) {
      try {
        h(delta);
      } catch (err) {
        this.logger.error(`subscription ${this.id} handler threw: ${errMsg(err)}`);
      }
    }
  }

  private kick(): void {
    if (this.working) return;
    this.working = true;
    void this.work()
      .catch((err) => this.logger.error(`subscription ${this.id} worker failed: ${errMsg(err)}`))
      .finally(() => {
        this.working = false;
        if (this.mailbox.length && this.state !== 'closed') this.kick();
      });
  }

  private async work(): Promise<void> {
    if (!this.snapshotDone) {
      await this.doSnapshot();
      if (this.state === 'closed') return;
    }

    while (this.mailbox.length) {
      const ev = this.mailbox.shift() as ChangeEvent;
      if (this.state === 'closed') return;

      // Replay gate: while not LIVE, drop anything already reflected in the snapshot.
      if (this.state !== 'live' && !lsnGt(ev.lsn, this.snapshotLsn)) continue;

      await applyMatch(this.matchContext(), ev);
    }

    // Mailbox drained. Flip to LIVE atomically — no await between the check above
    // and this assignment, so no event can slip past the replay gate unseen.
    if (this.state === 'replaying') this.state = 'live';
  }

  private async doSnapshot(): Promise<void> {
    this.state = 'snapshotting';
    const { snapshotLsn, rows } = await snapshotTable(
      this.pool,
      this.model.schema,
      this.model.table,
      this.coarse,
      this.maxRows,
    );
    // `close()` can flip state during the await; TS narrows the field to the
    // literal set above, so widen the comparison.
    if ((this.state as State) === 'closed') return;
    this.snapshotLsn = snapshotLsn;

    const member: Array<{ pk: string; row: Row }> = [];
    for (const raw of rows) {
      const mapped = this.model.mapRow(raw);
      // mingo decides membership on the snapshot too — same engine as live.
      if (this.effQuery.test(mapped)) {
        const pk = buildPk(this.model.pkCols, raw);
        this.documentIds.add(pk);
        member.push({ pk, row: mapped });
      }
    }

    this.emit({ kind: 'data', rows: member });
    this.snapshotDone = true;
    this.state = 'replaying';
  }

  private matchContext(): MatchContext {
    return {
      effQuery: this.effQuery,
      documentIds: this.documentIds,
      refetchOnUpdate: this.model.refetchOnUpdate,
      mapRow: this.model.mapRow,
      refetch: (ev) =>
        refetchByPk(this.pool, this.model.schema, this.model.table, this.model.pkCols, ev),
      emit: (delta) => this.emit(delta),
    };
  }
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
