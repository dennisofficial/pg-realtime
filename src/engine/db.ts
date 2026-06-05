import { Pool } from 'pg';
import { ChangeEvent, CoarseScope, Row } from '../types';

/** Quote a Postgres identifier (schema/table/column/slot/publication). */
export function ident(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function qualified(schema: string, table: string): string {
  return `${ident(schema)}.${ident(table)}`;
}

/**
 * Discover a table's primary-key columns (in key order) from the catalog. Used
 * when a model does not declare `primaryKey`. The same columns are then used for
 * the per-session Set, the snapshot, refetch, and (as the pgoutput override) the
 * WAL pk — so every layer agrees on identity.
 */
export async function resolvePrimaryKey(
  pool: Pool,
  schema: string,
  table: string,
): Promise<string[]> {
  const { rows } = await pool.query<{ attname: string }>(
    `SELECT a.attname
       FROM pg_index i
       JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
      WHERE i.indrelid = $1::regclass
        AND i.indisprimary
      ORDER BY array_position(i.indkey, a.attnum)`,
    [qualified(schema, table)],
  );
  return rows.map((r) => r.attname);
}

/** Idempotently ensure the publication exists and covers exactly the given tables. */
export async function ensurePublication(
  pool: Pool,
  publicationName: string,
  tables: Array<{ schema: string; table: string }>,
): Promise<void> {
  const exists = await pool.query(`SELECT 1 FROM pg_publication WHERE pubname = $1`, [
    publicationName,
  ]);
  const tableList = tables.map((t) => qualified(t.schema, t.table)).join(', ');

  if (exists.rowCount === 0) {
    // Scoped to specific tables — never FOR ALL TABLES.
    await pool.query(`CREATE PUBLICATION ${ident(publicationName)} FOR TABLE ${tableList}`);
    return;
  }

  // Reconcile: add any configured table not already published.
  const current = await pool.query<{ schemaname: string; tablename: string }>(
    `SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1`,
    [publicationName],
  );
  const have = new Set(current.rows.map((r) => `${r.schemaname}.${r.tablename}`));
  for (const t of tables) {
    if (!have.has(`${t.schema}.${t.table}`)) {
      await pool.query(
        `ALTER PUBLICATION ${ident(publicationName)} ADD TABLE ${qualified(t.schema, t.table)}`,
      );
    }
  }
}

/**
 * Ensure the logical replication slot exists. Returns the slot's `consistent_point`
 * LSN when freshly created (the engine's global floor), or null if it already
 * existed (we cannot re-read the original consistent point).
 */
export async function ensureSlot(pool: Pool, slotName: string): Promise<string | null> {
  const exists = await pool.query(`SELECT 1 FROM pg_replication_slots WHERE slot_name = $1`, [
    slotName,
  ]);
  if (exists.rowCount && exists.rowCount > 0) return null;

  const { rows } = await pool.query<{ lsn: string }>(
    `SELECT lsn FROM pg_create_logical_replication_slot($1, 'pgoutput')`,
    [slotName],
  );
  return rows[0]?.lsn ?? null;
}

export interface SnapshotResult {
  /** WAL position captured in the same REPEATABLE READ txn as the SELECT. */
  snapshotLsn: string;
  rows: Row[];
}

/**
 * Take a consistent snapshot of a table: capture `pg_current_wal_lsn()` and the
 * rows in ONE REPEATABLE READ transaction, so the captured LSN is the exact
 * visibility floor of the returned rows. The optional coarse scope only bounds
 * the fetch (perf) — security is enforced later by mingo.
 */
export async function snapshotTable(
  pool: Pool,
  schema: string,
  table: string,
  coarse: CoarseScope,
  maxRows: number,
): Promise<SnapshotResult> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
    const lsnRes = await client.query<{ lsn: string }>('SELECT pg_current_wal_lsn() AS lsn');
    const snapshotLsn = lsnRes.rows[0].lsn;

    const where = coarse && coarse.text ? ` WHERE ${coarse.text}` : '';
    const values = coarse?.values ?? [];
    const sql = `SELECT * FROM ${qualified(schema, table)}${where} LIMIT ${Math.floor(maxRows)}`;
    const dataRes = await client.query<Row>(sql, values);

    await client.query('COMMIT');
    return { snapshotLsn, rows: dataRes.rows };
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* ignore rollback error */
    }
    throw err;
  } finally {
    client.release();
  }
}

/** Fetch the current full row by PK — used only on a TOAST-incomplete UPDATE. */
export async function refetchByPk(
  pool: Pool,
  schema: string,
  table: string,
  pkColumns: string[],
  ev: ChangeEvent,
): Promise<Row | null> {
  const src = ev.row ?? ev.oldRow;
  if (!src || pkColumns.length === 0) return null;
  const where = pkColumns.map((c, i) => `${ident(c)} = $${i + 1}`).join(' AND ');
  const values = pkColumns.map((c) => src[c] ?? null);
  const { rows } = await pool.query<Row>(
    `SELECT * FROM ${qualified(schema, table)} WHERE ${where} LIMIT 1`,
    values,
  );
  return rows[0] ?? null;
}
