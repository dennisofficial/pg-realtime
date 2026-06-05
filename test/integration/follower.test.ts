import { Pool } from 'pg';
import { InProcessBus } from '../../src/bus/bus';
import { RealtimeEngine } from '../../src/engine/engine';
import { ModelConfig, RowDelta } from '../../src/types';

/**
 * The split deployment: one engine CONSUMES the WAL and publishes; a separate
 * `consume:false` FOLLOWER (sharing the bus) serves subscriptions and matches the
 * consumer's changes — never touching the replication slot. This is the
 * `workers` (consumer) + `staff` backend (follower, serves SSE) topology, in-process.
 */
jest.setTimeout(30000);

const DSN =
  process.env.PG_REALTIME_TEST_DSN ??
  'postgresql://postgres:postgres@localhost:5434/pgrealtime_test';

const ROW = 'dddddddd-dddd-dddd-dddd-dddddddddddd';
let admin: Pool;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function waitFor(pred: () => boolean, label: string, timeoutMs = 12000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error(`timeout: ${label}`);
    await sleep(25);
  }
}

function collector() {
  const map = new Map<string, Record<string, unknown>>();
  let gotSnapshot = false;
  return {
    apply(d: RowDelta) {
      if (d.kind === 'data') {
        map.clear();
        for (const e of d.rows) map.set(e.pk, e.row);
        gotSnapshot = true;
      } else if (d.kind === 'remove') map.delete(d.pk);
      else map.set(d.pk, d.row);
    },
    get gotSnapshot() {
      return gotSnapshot;
    },
    labels: () => [...map.values()].map((r) => r.label as string),
  };
}

beforeAll(async () => {
  admin = new Pool({ connectionString: DSN });
  await admin.query(`
    CREATE TABLE IF NOT EXISTS follower_scratch (
      id uuid PRIMARY KEY,
      status text NOT NULL,
      label text
    )`);
});

afterAll(async () => {
  await admin?.query('DROP TABLE IF EXISTS follower_scratch');
  await admin?.end();
});

beforeEach(async () => {
  await admin.query('TRUNCATE follower_scratch');
});

describe('consumer + follower split', () => {
  it('a follower serves a subscription fed by the consumer over a shared bus', async () => {
    const bus = new InProcessBus(); // shared by both engines (in-process stand-in for PgNotifyBus)
    const model: ModelConfig = { table: 'follower_scratch', name: 'scratch', primaryKey: 'id' };
    const slotName = 'pgrt_follower_slot';
    const publicationName = 'pgrt_follower_pub';

    const consumer = new RealtimeEngine({
      connectionString: DSN,
      slotName,
      publicationName,
      models: [model],
      bus,
      // consume: true (default) — reads the WAL, publishes to the bus
    });
    const follower = new RealtimeEngine({
      connectionString: DSN,
      slotName,
      publicationName,
      models: [model],
      bus,
      consume: false, // serves subscriptions only; never touches the slot
    });

    await consumer.start();
    await follower.start();

    try {
      await admin.query(
        `INSERT INTO follower_scratch (id, status, label) VALUES ($1,'active','seed')`,
        [ROW],
      );

      // The subscription lives on the FOLLOWER.
      const c = collector();
      (await follower.openSubscription({ model: 'scratch', filter: { status: 'active' } })).on(
        c.apply,
      );
      await waitFor(() => c.gotSnapshot, 'follower snapshot');
      expect(c.labels()).toEqual(['seed']);

      // A change the CONSUMER decodes from the WAL must reach the follower's subscription.
      await admin.query(`UPDATE follower_scratch SET label='changed' WHERE id=$1`, [ROW]);
      await waitFor(() => c.labels().includes('changed'), 'follower received consumer change');
    } finally {
      await follower.stop();
      await consumer.stop();
      for (let i = 0; i < 25; i++) {
        const { rows } = await admin.query<{ active: boolean }>(
          'SELECT active FROM pg_replication_slots WHERE slot_name=$1',
          [slotName],
        );
        if (rows.length === 0 || !rows[0].active) {
          if (rows.length) await admin.query('SELECT pg_drop_replication_slot($1)', [slotName]);
          break;
        }
        await sleep(150);
      }
      await admin.query(`DROP PUBLICATION IF EXISTS "${publicationName}"`);
    }
  });
});
