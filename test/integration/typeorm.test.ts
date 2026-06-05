import 'reflect-metadata';
import { Pool } from 'pg';
import { Column, DataSource, Entity, PrimaryColumn, Repository } from 'typeorm';
import { RealtimeRls } from '../../src/rls';
import { ModelConfig, RealtimeRuleGuard } from '../../src/types';
import { scopedFindWhere, toFindOptionsWhere } from '../../src/typeorm';

/**
 * Proves the RLS guard scope is enforced at the DATABASE through a real TypeORM
 * repository — including that `findAndCount` returns the *filtered* total, so
 * pagination is computed on the rows the user may see (not the whole table).
 */
jest.setTimeout(30000);

const DSN =
  process.env.PG_REALTIME_TEST_DSN ??
  'postgresql://postgres:postgres@localhost:5434/pgrealtime_test';

@Entity('orm_scratch')
class OrmScratch {
  @PrimaryColumn('uuid')
  id!: string;
  @Column('uuid')
  owner_id!: string;
  @Column('text')
  status!: string;
}

class OwnerGuard extends RealtimeRuleGuard<{ id: string }> {
  canRead(user: { id: string } | null) {
    return user ? { owner_id: user.id } : false;
  }
}

const U1 = '11111111-1111-1111-1111-111111111111';
const U2 = '22222222-2222-2222-2222-222222222222';
const row = (n: number) => `00000000-0000-0000-0000-${String(n).padStart(12, '0')}`;

let admin: Pool;
let ds: DataSource;
let repo: Repository<OrmScratch>;
let rls: RealtimeRls;

beforeAll(async () => {
  admin = new Pool({ connectionString: DSN });
  await admin.query(`
    CREATE TABLE IF NOT EXISTS orm_scratch (
      id uuid PRIMARY KEY,
      owner_id uuid NOT NULL,
      status text NOT NULL
    )`);
  ds = new DataSource({ type: 'postgres', url: DSN, entities: [OrmScratch], synchronize: false });
  await ds.initialize();
  repo = ds.getRepository(OrmScratch);
  rls = new RealtimeRls({
    models: [{ table: 'orm_scratch', name: 'res', primaryKey: 'id', guard: new OwnerGuard() }],
  });
});

afterAll(async () => {
  await ds?.destroy();
  await admin?.query('DROP TABLE IF EXISTS orm_scratch');
  await admin?.end();
});

beforeEach(async () => {
  await admin.query('TRUNCATE orm_scratch');
  // U1: 3 active + 1 archived; U2: 2 active. (6 rows total; U1-active = 3.)
  const values = [
    [row(1), U1, 'active'],
    [row(2), U1, 'active'],
    [row(3), U1, 'active'],
    [row(4), U1, 'archived'],
    [row(5), U2, 'active'],
    [row(6), U2, 'active'],
  ];
  for (const [id, owner, status] of values) {
    await admin.query('INSERT INTO orm_scratch (id, owner_id, status) VALUES ($1,$2,$3)', [
      id,
      owner,
      status,
    ]);
  }
});

describe('RLS scope -> TypeORM where (DB-side)', () => {
  it('toFindOptionsWhere filters at the database', async () => {
    const mine = await repo.find({ where: toFindOptionsWhere({ owner_id: U1 }) });
    expect(mine).toHaveLength(4); // all of U1's rows, none of U2's

    const someOwners = await repo.find({ where: toFindOptionsWhere({ owner_id: { $in: [U2] } }) });
    expect(someOwners.map((r) => r.owner_id)).toEqual([U2, U2]);
  });

  it('scopedFindWhere + findAndCount: pagination total is the FILTERED count', async () => {
    const { allowed, where } = await scopedFindWhere({
      rls,
      model: 'res',
      user: { id: U1 },
      where: { status: 'active' },
    });
    expect(allowed).toBe(true);

    const [items, total] = await repo.findAndCount({ where, take: 2, skip: 0 });
    // U1 has exactly 3 active rows — the total reflects the scoped set, not all 6 rows.
    expect(total).toBe(3);
    expect(items).toHaveLength(2);
    expect(items.every((r) => r.owner_id === U1 && r.status === 'active')).toBe(true);

    // page 2
    const [page2] = await repo.findAndCount({ where, take: 2, skip: 2 });
    expect(page2).toHaveLength(1);
  });

  it('denied user never reaches the DB with an unscoped query', async () => {
    const { allowed, where } = await scopedFindWhere({ rls, model: 'res', user: null });
    expect(allowed).toBe(false);
    // caller short-circuits on !allowed; `where` is never used for a fetch.
    expect(where).toEqual({});
  });
});
