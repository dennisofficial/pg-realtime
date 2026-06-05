import { RealtimeRls } from './rls';
import { ModelConfig, RealtimeRuleGuard, Row } from './types';

type User = { id: string } | null;

class OwnerGuard extends RealtimeRuleGuard<{ id: string }> {
  canRead(user: User) {
    return user ? { owner_id: user.id } : false;
  }
}

// Read what you own, but only update rows that are currently idle.
class IdleUpdateGuard extends RealtimeRuleGuard<{ id: string }> {
  canRead(user: User) {
    return user ? { owner_id: user.id } : false;
  }
  canUpdate(user: User) {
    return user ? { owner_id: user.id, status: 'idle' } : false;
  }
}

const models: ModelConfig[] = [
  { table: 'servers', name: 'servers', primaryKey: 'id', guard: new OwnerGuard() },
  { table: 'jobs', name: 'jobs', primaryKey: 'id', guard: new IdleUpdateGuard() },
  // snake_case column -> camelCase shape; guard written against the mapped shape.
  {
    table: 'tickets',
    name: 'tickets',
    primaryKey: 'id',
    mapRow: (r) => ({ ...r, ownerId: r.owner_id }),
    guard: new (class extends RealtimeRuleGuard<{ id: string }> {
      canRead(user: User) {
        return user ? { ownerId: user.id } : false;
      }
    })(),
  },
  { table: 'public_plans', name: 'plans', primaryKey: 'id' }, // no guard
];

const rls = new RealtimeRls({ models });
const u1 = { id: 'u1' };
const u2 = { id: 'u2' };

describe('RealtimeRls.scope', () => {
  it('returns the guard filter for an authorized user', async () => {
    expect(await rls.scope({ model: 'servers', user: u1 })).toEqual({
      allowed: true,
      filter: { owner_id: 'u1' },
    });
  });
  it('denies when the guard returns false', async () => {
    expect(await rls.scope({ model: 'servers', user: null })).toEqual({ allowed: false, filter: {} });
  });
  it('ANDs the caller filter into the scope', async () => {
    expect(await rls.scope({ model: 'servers', user: u1, filter: { status: 'RUNNING' } })).toEqual({
      allowed: true,
      filter: { $and: [{ owner_id: 'u1' }, { status: 'RUNNING' }] },
    });
  });
  it('allows everything when there is no guard', async () => {
    expect(await rls.scope({ model: 'plans', user: u1 })).toEqual({ allowed: true, filter: {} });
  });
});

describe('RealtimeRls.authorize', () => {
  const row: Row = { id: 's1', owner_id: 'u1', status: 'RUNNING' };

  it('permits the owner', async () => {
    expect(await rls.authorize({ model: 'servers', user: u1, row })).toBe(true);
  });
  it('denies a non-owner', async () => {
    expect(await rls.authorize({ model: 'servers', user: u2, row })).toBe(false);
  });
  it('denies when the guard denies outright', async () => {
    expect(await rls.authorize({ model: 'servers', user: null, row })).toBe(false);
  });
  it('applies mapRow before testing (snake_case row, camelCase guard)', async () => {
    const t: Row = { id: 't1', owner_id: 'u1' };
    expect(await rls.authorize({ model: 'tickets', user: u1, row: t })).toBe(true);
    expect(await rls.authorize({ model: 'tickets', user: u2, row: t })).toBe(false);
  });
});

describe('RealtimeRls action fallback', () => {
  it('uses canUpdate for the update action when defined (stricter than read)', async () => {
    const idle: Row = { id: 'j1', owner_id: 'u1', status: 'idle' };
    const running: Row = { id: 'j2', owner_id: 'u1', status: 'running' };
    // read: owner is enough
    expect(await rls.authorize({ model: 'jobs', user: u1, row: running, action: 'read' })).toBe(true);
    // update: must also be idle
    expect(await rls.authorize({ model: 'jobs', user: u1, row: idle, action: 'update' })).toBe(true);
    expect(await rls.authorize({ model: 'jobs', user: u1, row: running, action: 'update' })).toBe(
      false,
    );
  });
  it('falls back to canRead for actions without their own method', async () => {
    // servers has no canUpdate -> update falls back to canRead (owner check)
    const row: Row = { id: 's1', owner_id: 'u1', status: 'RUNNING' };
    expect(await rls.authorize({ model: 'servers', user: u1, row, action: 'update' })).toBe(true);
    expect(await rls.authorize({ model: 'servers', user: u2, row, action: 'delete' })).toBe(false);
  });
});

describe('RealtimeRls.filterRows', () => {
  it('keeps only the rows the user may access', async () => {
    const rows: Row[] = [
      { id: 1, owner_id: 'u1' },
      { id: 2, owner_id: 'u2' },
      { id: 3, owner_id: 'u1' },
    ];
    const out = await rls.filterRows({ model: 'servers', user: u1, rows });
    expect(out.map((r) => r.id)).toEqual([1, 3]);
  });
  it('returns [] when denied', async () => {
    const out = await rls.filterRows({ model: 'servers', user: null, rows: [{ id: 1, owner_id: 'u1' }] });
    expect(out).toEqual([]);
  });
});
