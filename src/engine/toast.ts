import { Row } from '../types';

/**
 * Detects the TOAST hole: on an UPDATE, Postgres omits unchanged TOASTed
 * (large `text`/`jsonb`/…) columns from the WAL row image — REGARDLESS of
 * REPLICA IDENTITY. The pgoutput parser represents such a column as `undefined`
 * (a real SQL NULL parses to `null`, so the two are distinguishable).
 *
 * Therefore: any column whose value is `undefined` is an unchanged-TOAST
 * placeholder. Predicate columns are expected to be small (never TOASTed, so
 * always present); this only signals that the emitted *payload* may be missing
 * a large column's current value — handled per-table via `refetchOnUpdate`.
 */
export function isToastIncomplete(row: Row | null): boolean {
  if (!row) return false;
  for (const key in row) {
    if (row[key] === undefined) return true;
  }
  return false;
}
