import { Pool } from 'pg';
import { RealtimeEngine } from '../../src/engine/engine';
import { ModelConfig, RowDelta } from '../../src/types';

/**
 * End-to-end against a real logical-replication Postgres:
 *   docker compose -f docker-compose.test.yml up -d
 *   pnpm test:integration
 *
 * The precise snapshot↔stream LSN-gating is proven deterministically in the unit
 * suite (snapshot.test.ts); here we prove the real WAL → mingo → delta pipeline,
 * including the TOAST refetch policy against an actually-TOASTed column.
 */
jest.setTimeout(30000);

const DSN =
  process.env.PG_REALTIME_TEST_DSN ??
  'postgresql://postgres:postgres@localhost:5434/pgrealtime_test';

const ID = {
  r1: '11111111-1111-1111-1111-111111111111',
  r2: '22222222-2222-2222-2222-222222222222',
  r3: '33333333-3333-3333-3333-333333333333',
  r4: '44444444-4444-4444-4444-444444444444',
  r5: '55555555-5555-5555-5555-555555555555',
  toast: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
};

let admin: Pool;
let slotCounter = 0;

const pk = (id: string) => JSON.stringify([id]);

beforeAll(async () => {
  admin = new Pool({ connectionString: DSN });
  await admin.query(`
    CREATE TABLE IF NOT EXISTS realtime_scratch (
      id uuid PRIMARY KEY,
      tenant_id text NOT NULL,
      status text NOT NULL,
      label text,
      big_blob text
    )`);
  // EXTERNAL storage => the big column is stored out-of-line and is omitted from
  // the WAL image on an UPDATE that does not change it (the TOAST hole).
  await admin.query(`ALTER TABLE realtime_scratch ALTER COLUMN big_blob SET STORAGE EXTERNAL`);
});

afterAll(async () => {
  await admin?.query('DROP TABLE IF EXISTS realtime_scratch');
  await admin?.end();
});

beforeEach(async () => {
  await admin.query('TRUNCATE realtime_scratch');
});

async function withEngine(
  models: ModelConfig[],
  fn: (engine: RealtimeEngine) => Promise<void>,
): Promise<void> {
  slotCounter += 1;
  const slotName = `pgrt_test_slot_${slotCounter}`;
  const publicationName = `pgrt_test_pub_${slotCounter}`;
  const engine = new RealtimeEngine({ connectionString: DSN, slotName, publicationName, models });
  await engine.start();
  try {
    await fn(engine);
  } finally {
    await engine.stop();
    await dropSlot(slotName);
    await admin.query(`DROP PUBLICATION IF EXISTS "${publicationName}"`);
  }
}

async function dropSlot(slotName: string): Promise<void> {
  // The slot only becomes inactive once the replication connection is fully gone.
  for (let i = 0; i < 25; i++) {
    const { rows } = await admin.query<{ active: boolean }>(
      'SELECT active FROM pg_replication_slots WHERE slot_name = $1',
      [slotName],
    );
    if (rows.length === 0) return;
    if (!rows[0].active) {
      await admin.query('SELECT pg_drop_replication_slot($1)', [slotName]);
      return;
    }
    await sleep(150);
  }
}

function collector() {
  const map = new Map<string, Record<string, unknown>>();
  const deltas: RowDelta[] = [];
  let gotSnapshot = false;
  return {
    apply(d: RowDelta) {
      deltas.push(d);
      if (d.kind === 'data') {
        map.clear();
        for (const e of d.rows) map.set(e.pk, e.row);
        gotSnapshot = true;
      } else if (d.kind === 'remove') {
        map.delete(d.pk);
      } else {
        map.set(d.pk, d.row);
      }
    },
    get gotSnapshot() {
      return gotSnapshot;
    },
    labels: () =>
      [...map.values()].map((r) => r.label as string).sort(),
    row: (id: string) => map.get(pk(id)),
    deltas,
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor(pred: () => boolean, label: string, timeoutMs = 8000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error(`timeout waiting for: ${label}`);
    await sleep(25);
  }
}

const insert = (id: string, tenant: string, status: string, label: string, blob?: string) =>
  admin.query(
    `INSERT INTO realtime_scratch (id, tenant_id, status, label, big_blob) VALUES ($1,$2,$3,$4,$5)`,
    [id, tenant, status, label, blob ?? null],
  );

describe('pg-realtime end-to-end', () => {
  const scratch: ModelConfig = { table: 'realtime_scratch', name: 'scratch', primaryKey: 'id' };

  it('streams add/update/remove for a filtered query and respects the filter', async () => {
    // Seed: only r1 matches {tenant t1, status active}.
    await insert(ID.r1, 't1', 'active', 'r1');
    await insert(ID.r2, 't1', 'archived', 'r2');
    await insert(ID.r3, 't2', 'active', 'r3');

    await withEngine([scratch], async (engine) => {
      const sub = await engine.openSubscription({
        model: 'scratch',
        filter: { tenant_id: 't1', status: 'active' },
      });
      const c = collector();
      sub.on(c.apply);

      await waitFor(() => c.gotSnapshot, 'snapshot');
      expect(c.labels()).toEqual(['r1']);

      // enter the set
      await insert(ID.r4, 't1', 'active', 'r4');
      await waitFor(() => c.labels().includes('r4'), 'add r4');
      expect(c.labels()).toEqual(['r1', 'r4']);

      // leave the set (status -> archived)
      await admin.query(`UPDATE realtime_scratch SET status='archived' WHERE id=$1`, [ID.r1]);
      await waitFor(() => !c.labels().includes('r1'), 'remove r1');

      // small-column update, stays in the set
      await admin.query(`UPDATE realtime_scratch SET label='r4b' WHERE id=$1`, [ID.r4]);
      await waitFor(() => c.labels().includes('r4b'), 'update r4 -> r4b');

      // another row enters via update
      await admin.query(`UPDATE realtime_scratch SET status='active' WHERE id=$1`, [ID.r2]);
      await waitFor(() => c.labels().includes('r2'), 'add r2 via update');

      // delete leaves the set
      await admin.query(`DELETE FROM realtime_scratch WHERE id=$1`, [ID.r4]);
      await waitFor(() => !c.labels().includes('r4b'), 'remove r4');

      // non-matching tenant: no delta
      await insert(ID.r5, 't2', 'active', 'r5');
      await sleep(400);
      expect(c.labels()).toEqual(['r2']);
    });
  });

  it('TOAST: refetchOnUpdate refills the dropped large column; default keeps the placeholder', async () => {
    const blob = 'x'.repeat(16384); // out-of-line via EXTERNAL storage
    await insert(ID.toast, 't_toast', 'active', 'toast', blob);

    const models: ModelConfig[] = [
      { table: 'realtime_scratch', name: 'plain', primaryKey: 'id' }, // refetchOnUpdate defaults to false
      { table: 'realtime_scratch', name: 'full', primaryKey: 'id', refetchOnUpdate: true },
    ];

    await withEngine(models, async (engine) => {
      const filter = { tenant_id: 't_toast', status: 'active' };
      const plain = collector();
      const full = collector();
      (await engine.openSubscription({ model: 'plain', filter })).on(plain.apply);
      (await engine.openSubscription({ model: 'full', filter })).on(full.apply);

      await waitFor(() => plain.gotSnapshot && full.gotSnapshot, 'snapshots');
      // INSERT carries the full value on both.
      expect((plain.row(ID.toast)?.big_blob as string)?.length).toBe(16384);
      expect((full.row(ID.toast)?.big_blob as string)?.length).toBe(16384);

      // UPDATE a small column only — big_blob is unchanged-TOAST, so the WAL image drops it.
      await admin.query(`UPDATE realtime_scratch SET label='toast2' WHERE id=$1`, [ID.toast]);

      await waitFor(() => plain.row(ID.toast)?.label === 'toast2', 'plain update');
      await waitFor(() => full.row(ID.toast)?.label === 'toast2', 'full update');

      // plain: no refetch -> the placeholder (undefined) is what arrived on the wire.
      expect(plain.row(ID.toast)?.big_blob).toBeUndefined();
      // full: refetched by PK -> the current full value.
      expect((full.row(ID.toast)?.big_blob as string)?.length).toBe(16384);
    });
  });

  it('document mode: subscribes to a single row by pk', async () => {
    await insert(ID.r1, 't1', 'active', 'r1');

    await withEngine([scratch], async (engine) => {
      const sub = await engine.openSubscription({ model: 'scratch', pk: pk(ID.r1) });
      const c = collector();
      sub.on(c.apply);

      await waitFor(() => c.gotSnapshot, 'snapshot');
      expect(c.labels()).toEqual(['r1']);

      // a different row must not reach this subscription
      await insert(ID.r2, 't1', 'active', 'r2');
      await sleep(400);
      expect(c.labels()).toEqual(['r1']);

      await admin.query(`UPDATE realtime_scratch SET label='r1b' WHERE id=$1`, [ID.r1]);
      await waitFor(() => c.row(ID.r1)?.label === 'r1b', 'update r1');
    });
  });

  it('no missing rows when subscribing during concurrent inserts (snapshot race)', async () => {
    // Probes the snapshot↔stream boundary: subscribe while writes are in flight, so
    // some rows commit just before the snapshot LSN and some just after. With the
    // lower-bound LSN capture, none are dropped — the cache must equal ground truth.
    const N = 40;
    const uuidFor = (i: number) => `00000000-0000-0000-0000-${i.toString().padStart(12, '0')}`;

    await withEngine([scratch], async (engine) => {
      const writes = (async () => {
        for (let i = 0; i < N; i++) await insert(uuidFor(i), 't1', 'active', `s${i}`);
      })();

      const sub = await engine.openSubscription({
        model: 'scratch',
        filter: { tenant_id: 't1', status: 'active' },
      });
      const c = collector();
      sub.on(c.apply);

      await writes;
      await waitFor(() => c.labels().length >= N, `all ${N} rows present`, 15000);

      const ground = await admin.query<{ label: string }>(
        `SELECT label FROM realtime_scratch WHERE tenant_id='t1' AND status='active'`,
      );
      expect(c.labels()).toEqual(ground.rows.map((r) => r.label).sort());
    });
  });
});
