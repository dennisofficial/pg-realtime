import { ChangeEvent, Row, RowDelta } from '../types';
import { diffColumns } from './diff';

/**
 * Minimal surface the matcher needs from a subscription. Kept deliberately tiny
 * so the entire add/update/remove/TOAST transition table is unit-testable with
 * no database and no engine — see matcher.test.ts.
 */
export interface MatchContext {
  /** The single membership decider (mingo). Runs on BOTH the snapshot and here. */
  effQuery: { test(o: Row): boolean };
  /**
   * PKs this subscription believed were in its result set as of the snapshot.
   * NOT a steady-state cache: once LIVE, it is consulted/maintained only for the
   * two cases the stateless (FULL) classification can't handle on its own — see
   * `applyMatch`. For a REPLICA IDENTITY FULL table with no snapshot↔stream races
   * left to settle, this set is never read or written again after the subscription
   * goes live.
   */
  documentIds: Set<string>;
  /**
   * True while the subscription is still snapshotting/replaying (not yet LIVE).
   * During this window `snapshotLsn` is only a lower bound (see db.ts), so an
   * event may duplicate something already reflected in the snapshot; classification
   * always goes through the legacy Set-membership decider so that dedupe keeps
   * working exactly as before. Ignored once the caller is LIVE and the event
   * carries a usable old image.
   */
  replaying: boolean;
  /** Whether to refetch the full row by PK when an UPDATE dropped a TOAST column. */
  refetchOnUpdate: boolean;
  /** Maps a raw PG row to the client-facing shape. */
  mapRow: (raw: Row) => Row;
  /** Fetch the current full row by PK (used only on a TOAST-incomplete UPDATE). */
  refetch: (ev: ChangeEvent) => Promise<Row | null>;
  /** Push a delta toward the client. */
  emit: (delta: RowDelta) => void;
}

/**
 * The heart of the smart fan-out. Mirrors `handleQueryUpdate` from the Mongo
 * reference (stream.service.ts), generalized to Postgres insert/update/delete.
 *
 * Two classification strategies, chosen per-event:
 *
 * 1. STATELESS (steady state, REPLICA IDENTITY FULL): the filter is tested against
 *    BOTH the old and new row images (`ev.oldRow`/`ev.row`), which is all a FULL
 *    table's WAL ever needs — no per-subscription membership set is consulted:
 *
 *      insert                      : pass_new                -> add
 *      update, pass_old & pass_new : ...                     -> update (patch)
 *      update, !pass_old & pass_new: entered the filter       -> add
 *      update, pass_old & !pass_new: left the filter          -> remove
 *      delete, pass_old            : ...                     -> remove
 *
 *    Invariant this relies on: `pass_old` true ⟹ the client already has the row
 *    (it arrived via the snapshot or a prior add), so patching it on `pass_old &&
 *    pass_new` is always safe.
 *
 * 2. LEGACY Set-membership (pk ∈ documentIds = "client thinks row is in the set"):
 *    used whenever the WAL didn't supply an old image (table not REPLICA IDENTITY
 *    FULL — the server literally can't compute `pass_old`), and ALWAYS during
 *    snapshotting/replaying (`ctx.replaying`), because `snapshotLsn` is only a
 *    lower bound: a replayed event may duplicate a row the snapshot already
 *    includes, and the Set is what catches that duplicate.
 *
 *      delete           , pk∈set            -> remove, drop pk
 *      delete           , pk∉set            -> (nothing)
 *      insert  & pass    , (pk∉set)         -> add, track pk
 *      insert  & !pass                      -> (nothing)
 *      update  & pk∈set  & pass             -> update
 *      update  & pk∈set  & !pass            -> remove, drop pk   (left the result set)
 *      update  & pk∉set  & pass             -> add, track pk     (entered the result set)
 *      update  & pk∉set  & !pass            -> (nothing)
 */
export async function applyMatch(ctx: MatchContext, ev: ChangeEvent): Promise<void> {
  const pk = ev.pk;
  const stateless = !ctx.replaying;

  // DELETE has no new image — handle before any test (mirrors the reference).
  if (ev.op === 'delete') {
    if (stateless && ev.oldRow) {
      if (ctx.effQuery.test(ctx.mapRow(ev.oldRow))) ctx.emit({ kind: 'remove', pk });
      return;
    }
    if (ctx.documentIds.has(pk)) {
      ctx.documentIds.delete(pk);
      ctx.emit({ kind: 'remove', pk });
    }
    return;
  }

  // Resolve the row we will TEST and EMIT (insert & update only).
  let row = ctx.mapRow(ev.row ?? {});

  // TOAST hole: predicate columns are small (always present), but the payload may
  // be missing a large column's current value. Refetch only when it actually
  // dropped a TOAST column AND this table opted in.
  if (ev.op === 'update' && ctx.refetchOnUpdate && ev.toastIncomplete) {
    const fresh = await ctx.refetch(ev);
    if (fresh) row = ctx.mapRow(fresh);
  }

  const passNew = ctx.effQuery.test(row);

  if (ev.op === 'update') {
    if (stateless && ev.oldRow) {
      const oldRow = ctx.mapRow(ev.oldRow);
      const passOld = ctx.effQuery.test(oldRow);
      if (passOld && passNew) {
        ctx.emit({ kind: 'update', pk, row, changedColumns: diffColumns(oldRow, row) });
      } else if (!passOld && passNew) {
        ctx.emit({ kind: 'add', pk, row });
      } else if (passOld && !passNew) {
        ctx.emit({ kind: 'remove', pk });
      }
      return;
    }

    if (ctx.documentIds.has(pk)) {
      if (passNew) {
        const changedColumns = ev.oldRow ? diffColumns(ctx.mapRow(ev.oldRow), row) : undefined;
        ctx.emit({ kind: 'update', pk, row, changedColumns });
      } else {
        ctx.documentIds.delete(pk);
        ctx.emit({ kind: 'remove', pk });
      }
    } else if (passNew) {
      ctx.documentIds.add(pk);
      ctx.emit({ kind: 'add', pk, row });
    }
    return;
  }

  // insert
  if (stateless) {
    // No old image is possible for INSERT by definition — pass ⇒ add, no set needed.
    if (passNew) ctx.emit({ kind: 'add', pk, row });
    return;
  }

  // Replay-phase dedupe: snapshotLsn is a lower bound, so an insert already
  // reflected in the snapshot can be replayed; skip it rather than emit a
  // duplicate `add`.
  if (passNew && !ctx.documentIds.has(pk)) {
    ctx.documentIds.add(pk);
    ctx.emit({ kind: 'add', pk, row });
  }
}
