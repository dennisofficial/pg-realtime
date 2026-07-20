import { Pgoutput } from 'pg-logical-replication';
import { ChangeEvent, Row } from '../types';
import { isToastIncomplete } from './toast';

type ChangeMessage = Pgoutput.MessageInsert | Pgoutput.MessageUpdate | Pgoutput.MessageDelete;

/**
 * Resolve the primary-key columns for a relation. Prefer the explicit config
 * override; otherwise use the relation's replica-identity key columns (the PK
 * under REPLICA IDENTITY DEFAULT). An empty result (REPLICA IDENTITY NOTHING)
 * means the table cannot be matched — the engine rejects such tables at startup.
 */
export function pkColumns(
  relation: Pgoutput.MessageRelation,
  override?: string | string[],
): string[] {
  if (override) return Array.isArray(override) ? override : [override];
  return relation.keyColumns ?? [];
}

export function buildPk(cols: string[], src: Row | null | undefined): string {
  if (!src) return '';
  return JSON.stringify(cols.map((c) => src[c] ?? null));
}

/**
 * Normalize a pgoutput change message into a transport-agnostic ChangeEvent,
 * stamped with the *committing transaction's* `commitEndLsn` (passed in by the
 * replication source) — the visibility-ordered LSN the snapshot machine compares
 * against.
 */
export function toChangeEvent(
  msg: ChangeMessage,
  commitEndLsn: string,
  pkOverride?: string | string[],
): ChangeEvent {
  const relation = msg.relation;
  const cols = pkColumns(relation, pkOverride);
  const common = { schema: relation.schema, table: relation.name, lsn: commitEndLsn };

  if (msg.tag === 'insert') {
    return {
      ...common,
      op: 'insert',
      row: msg.new,
      oldRow: null,
      pk: buildPk(cols, msg.new),
      toastIncomplete: false, // inserts carry full values; TOAST omission is UPDATE-only
    };
  }

  if (msg.tag === 'update') {
    // `new` always carries the PK columns (never TOASTed), so it is the PK source.
    return {
      ...common,
      op: 'update',
      row: msg.new,
      oldRow: msg.old,
      pk: buildPk(cols, msg.new ?? msg.key ?? msg.old),
      toastIncomplete: isToastIncomplete(msg.new),
    };
  }

  // delete: under REPLICA IDENTITY DEFAULT, `key` holds the PK; `old` may be null.
  return {
    ...common,
    op: 'delete',
    row: null,
    oldRow: msg.old,
    pk: buildPk(cols, msg.key ?? msg.old),
    toastIncomplete: false,
  };
}
