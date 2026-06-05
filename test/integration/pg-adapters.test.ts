import { Pool } from 'pg';
import { PgNotifyBus } from '../../src/bus/pg-notify-bus';
import { RealtimeEngine } from '../../src/engine/engine';
import { PgAdvisoryLockLeaderElector } from '../../src/leader/pg-advisory-leader';
import { ChangeEvent, ModelConfig, RowDelta } from '../../src/types';

/**
 * The Redis-free path: Postgres advisory-lock leader election + LISTEN/NOTIFY fan-out.
 *   docker compose -f docker-compose.test.yml up -d --wait
 *   pnpm test:integration
 */
jest.setTimeout(30000);

const DSN =
  process.env.PG_REALTIME_TEST_DSN ??
  'postgresql://postgres:postgres@localhost:5434/pgrealtime_test';

const ROW_ID = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';
let admin: Pool;
let counter = 0;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function waitFor(pred: () => boolean, label: string, timeoutMs = 10000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error(`timeout waiting for: ${label}`);
    await sleep(25);
  }
}

beforeAll(async () => {
  admin = new Pool({ connectionString: DSN });
  await admin.query(`
    CREATE TABLE IF NOT EXISTS adapters_scratch (
      id uuid PRIMARY KEY,
      tenant text NOT NULL,
      status text NOT NULL,
      label text
    )`);
});

afterAll(async () => {
  await admin?.query('DROP TABLE IF EXISTS adapters_scratch');
  await admin?.end();
});

beforeEach(async () => {
  await admin.query('TRUNCATE adapters_scratch');
});

async function dropSlot(slotName: string): Promise<void> {
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
  let gotSnapshot = false;
  return {
    apply(d: RowDelta) {
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
    labels: () => [...map.values()].map((r) => r.label as string),
  };
}

describe('Postgres advisory-lock leader election', () => {
  it('grants the lock to one holder; releasing lets the next acquire', async () => {
    const lockName = `pgrt_lock_${++counter}`;
    const a = new PgAdvisoryLockLeaderElector({ connectionString: DSN, lockName });
    const b = new PgAdvisoryLockLeaderElector({ connectionString: DSN, lockName });
    try {
      expect(await a.acquire()).toBe(true);
      expect(await b.acquire()).toBe(false); // a holds it
      expect(await a.renew()).toBe(true);

      await a.release();
      expect(await b.acquire()).toBe(true); // freed -> b gets it
    } finally {
      await a.release();
      await b.release();
    }
  });
});

describe('Postgres NOTIFY bus', () => {
  it('round-trips a ChangeEvent through LISTEN/NOTIFY', async () => {
    const bus = new PgNotifyBus({ connectionString: DSN });
    const channel = `pgrt_chan_${++counter}`;
    const received: ChangeEvent[] = [];
    const unsub = await bus.subscribe(channel, (e) => received.push(e));
    const event: ChangeEvent = {
      op: 'insert',
      schema: 'public',
      table: 'adapters_scratch',
      pk: '["x"]',
      row: { id: 'x', status: 'active' },
      oldRow: null,
      toastIncomplete: false,
      lsn: '0/ABCDEF',
    };
    try {
      await bus.publish(channel, event);
      await waitFor(() => received.length > 0, 'notification received');
      expect(received[0]).toEqual(event);
    } finally {
      await unsub();
      await bus.close();
    }
  });
});

describe('Redis-free multi-replica (advisory lock + NOTIFY)', () => {
  it('elects one consumer and fans a change out to every replica', async () => {
    const lockName = `pgrt_e2e_lock_${++counter}`;
    const slotName = `pgrt_e2e_slot_${counter}`;
    const publicationName = `pgrt_e2e_pub_${counter}`;
    const model: ModelConfig = { table: 'adapters_scratch', name: 'scratch', primaryKey: 'id' };

    const make = () =>
      new RealtimeEngine({
        connectionString: DSN,
        slotName,
        publicationName,
        models: [model],
        leader: new PgAdvisoryLockLeaderElector({ connectionString: DSN, lockName }),
        bus: new PgNotifyBus({ connectionString: DSN }),
      });

    const a = make();
    const b = make();
    await a.start(); // a wins the advisory lock -> sole WAL consumer
    await b.start(); // b loses the lock -> follower, only LISTENs

    try {
      const ca = collector();
      const cb = collector();
      (await a.openSubscription({ model: 'scratch', filter: { tenant: 't1', status: 'active' } })).on(
        ca.apply,
      );
      (await b.openSubscription({ model: 'scratch', filter: { tenant: 't1', status: 'active' } })).on(
        cb.apply,
      );
      await waitFor(() => ca.gotSnapshot && cb.gotSnapshot, 'both snapshots');

      // One INSERT. a decodes it from the WAL and NOTIFYs; BOTH replicas must see it.
      await admin.query(
        `INSERT INTO adapters_scratch (id, tenant, status, label) VALUES ($1, 't1', 'active', 'x')`,
        [ROW_ID],
      );
      await waitFor(
        () => ca.labels().includes('x') && cb.labels().includes('x'),
        'change reached both replicas',
        12000,
      );
    } finally {
      await a.stop();
      await b.stop();
      await dropSlot(slotName);
      await admin.query(`DROP PUBLICATION IF EXISTS "${publicationName}"`);
    }
  });
});
