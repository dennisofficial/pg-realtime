import { Pool } from 'pg';
import { RealtimeRls } from '../../src/rls';
import { ModelConfig, RealtimeRuleGuard } from '../../src/types';

/**
 * The server-side authorizer against a real database — proving `query`/`get` return
 * exactly what the SAME guard would scope a subscription to.
 */
jest.setTimeout(30000);

const DSN =
  process.env.PG_REALTIME_TEST_DSN ??
  'postgresql://postgres:postgres@localhost:5434/pgrealtime_test';

class OwnerGuard extends RealtimeRuleGuard<{ id: string }> {
  canRead(user: { id: string } | null) {
    return user ? { owner_id: user.id } : false;
  }
}

const U1 = '11111111-1111-1111-1111-111111111111';
const U2 = '22222222-2222-2222-2222-222222222222';
const S1 = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'; // owned by U1
const S2 = 'cccccccc-cccc-cccc-cccc-cccccccccccc'; // owned by U2
const MISSING = '99999999-9999-9999-9999-999999999999';

let admin: Pool;
let rls: RealtimeRls;

beforeAll(async () => {
  admin = new Pool({ connectionString: DSN });
  await admin.query(`
    CREATE TABLE IF NOT EXISTS rls_scratch (
      id uuid PRIMARY KEY,
      owner_id uuid NOT NULL,
      status text NOT NULL
    )`);
  const models: ModelConfig[] = [
    { table: 'rls_scratch', name: 'res', primaryKey: 'id', guard: new OwnerGuard() },
  ];
  rls = new RealtimeRls({ models, connectionString: DSN });
});

afterAll(async () => {
  await rls?.close();
  await admin?.query('DROP TABLE IF EXISTS rls_scratch');
  await admin?.end();
});

beforeEach(async () => {
  await admin.query('TRUNCATE rls_scratch');
  await admin.query(`INSERT INTO rls_scratch (id, owner_id, status) VALUES ($1, $2, 'RUNNING')`, [S1, U1]);
  await admin.query(`INSERT INTO rls_scratch (id, owner_id, status) VALUES ($1, $2, 'RUNNING')`, [S2, U2]);
});

describe('RealtimeRls DB-backed query/get', () => {
  it('query returns only the rows the user may read', async () => {
    const r1 = await rls.query({ model: 'res', user: { id: U1 } });
    expect(r1.map((r) => r.id)).toEqual([S1]);

    const r2 = await rls.query({ model: 'res', user: { id: U2 } });
    expect(r2.map((r) => r.id)).toEqual([S2]); // same as a subscription snapshot would show
  });

  it('get returns an owned row; null for someone else\'s OR a missing one', async () => {
    expect((await rls.get({ model: 'res', user: { id: U1 }, pk: [S1] }))?.id).toBe(S1);
    expect(await rls.get({ model: 'res', user: { id: U2 }, pk: [S1] })).toBeNull(); // not owned
    expect(await rls.get({ model: 'res', user: { id: U1 }, pk: [MISSING] })).toBeNull(); // not found
  });

  it('the power-on pattern: fetch-and-authorize in one call', async () => {
    // server-side, before a mutation:
    const server = await rls.get({ model: 'res', user: { id: U1 }, pk: [S1] });
    expect(server).not.toBeNull();
    // a different user gets null — indistinguishable from "not found", which is the safe default
    expect(await rls.get({ model: 'res', user: { id: U2 }, pk: [S1] })).toBeNull();
  });
});
