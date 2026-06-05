import { ChangeEvent, Row, RowDelta } from '../types';

/**
 * Minimal surface the matcher needs from a subscription. Kept deliberately tiny
 * so the entire add/update/remove/TOAST transition table is unit-testable with
 * no database and no engine — see matcher.test.ts.
 */
export interface MatchContext {
  /** The single membership decider (mingo). Runs on BOTH the snapshot and here. */
  effQuery: { test(o: Row): boolean };
  /** PKs this subscription currently believes are in its result set. */
  documentIds: Set<string>;
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
 * Transition table (pk ∈ documentIds = "client thinks row is in the set"):
 *   delete           , pk∈set            -> remove, drop pk
 *   delete           , pk∉set            -> (nothing)
 *   insert  & pass    , (pk∉set)         -> add, track pk
 *   insert  & !pass                      -> (nothing)
 *   update  & pk∈set  & pass             -> update
 *   update  & pk∈set  & !pass            -> remove, drop pk   (left the result set)
 *   update  & pk∉set  & pass             -> add, track pk     (entered the result set)
 *   update  & pk∉set  & !pass            -> (nothing)
 *
 * Membership is decided by `effQuery` (mingo) — never by SQL — so it can never
 * diverge from the snapshot's membership decision (which uses the same query).
 */
export async function applyMatch(ctx: MatchContext, ev: ChangeEvent): Promise<void> {
  const pk = ev.pk;

  // DELETE has no new image — handle before any test (mirrors the reference).
  if (ev.op === 'delete') {
    if (ctx.documentIds.has(pk)) {
      ctx.documentIds.delete(pk);
      ctx.emit({ kind: 'remove', pk });
    }
    return;
  }

  // Resolve the row we will TEST and EMIT.
  let row = ctx.mapRow(ev.row ?? {});

  // TOAST hole: predicate columns are small (always present), but the payload may
  // be missing a large column's current value. Refetch only when it actually
  // dropped a TOAST column AND this table opted in.
  if (ev.op === 'update' && ctx.refetchOnUpdate && ev.toastIncomplete) {
    const fresh = await ctx.refetch(ev);
    if (fresh) row = ctx.mapRow(fresh);
  }

  const pass = ctx.effQuery.test(row);

  if (ev.op === 'update') {
    if (ctx.documentIds.has(pk)) {
      if (pass) {
        ctx.emit({ kind: 'update', pk, row });
      } else {
        ctx.documentIds.delete(pk);
        ctx.emit({ kind: 'remove', pk });
      }
    } else if (pass) {
      ctx.documentIds.add(pk);
      ctx.emit({ kind: 'add', pk, row });
    }
    return;
  }

  // insert — dedupe on Set membership. snapshotLsn is a lower bound, so an insert
  // already reflected in the snapshot can be replayed; skip it rather than emit a
  // duplicate `add`.
  if (pass && !ctx.documentIds.has(pk)) {
    ctx.documentIds.add(pk);
    ctx.emit({ kind: 'add', pk, row });
  }
}
