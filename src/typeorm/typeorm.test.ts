import { FindOperator } from 'typeorm';
import { RealtimeRls } from '../rls';
import { ModelConfig, RealtimeRuleGuard } from '../types';
import { scopedFindWhere, toFindOptionsWhere } from './index';

describe('toFindOptionsWhere', () => {
  it('keeps plain equality as a raw value', () => {
    expect(toFindOptionsWhere({ customer_id: 'u1' })).toEqual({ customer_id: 'u1' });
  });

  it('null -> IsNull', () => {
    const w = toFindOptionsWhere({ deleted_at: null }) as Record<string, FindOperator<unknown>>;
    expect(w.deleted_at).toBeInstanceOf(FindOperator);
    expect(w.deleted_at.type).toBe('isNull');
  });

  it('$in -> In, $nin -> Not(In)', () => {
    const wIn = toFindOptionsWhere({ org_id: { $in: ['a', 'b'] } }) as Record<string, FindOperator<unknown>>;
    expect(wIn.org_id.type).toBe('in');
    expect(wIn.org_id.value).toEqual(['a', 'b']);

    const wNin = toFindOptionsWhere({ org_id: { $nin: ['a'] } }) as Record<string, FindOperator<unknown>>;
    expect(wNin.org_id.type).toBe('not');
    // FindOperator.value unwraps the nested In, so this is the NOT IN (...) value set.
    expect(wNin.org_id.value).toEqual(['a']);
  });

  it('$ne -> Not, comparisons -> typeorm operators', () => {
    const ne = toFindOptionsWhere({ status: { $ne: 'archived' } }) as Record<string, FindOperator<unknown>>;
    expect(ne.status.type).toBe('not');
    expect(ne.status.value).toBe('archived');

    const ge = toFindOptionsWhere({ age: { $gte: 18 } }) as Record<string, FindOperator<unknown>>;
    expect(ge.age.type).toBe('moreThanOrEqual');
    expect(ge.age.value).toBe(18);
  });

  it('multiple operators on one field -> And', () => {
    const w = toFindOptionsWhere({ age: { $gte: 18, $lt: 65 } }) as Record<string, FindOperator<unknown>>;
    expect(w.age.type).toBe('and');
  });

  it('top-level $and merges distinct fields', () => {
    expect(toFindOptionsWhere({ $and: [{ customer_id: 'u1' }, { status: 'active' }] })).toEqual({
      customer_id: 'u1',
      status: 'active',
    });
  });

  it('top-level $or -> array of wheres', () => {
    expect(toFindOptionsWhere({ $or: [{ owner_id: 'u1' }, { shared: true }] })).toEqual([
      { owner_id: 'u1' },
      { shared: true },
    ]);
  });

  it('fails loud on anything it cannot translate faithfully', () => {
    expect(() => toFindOptionsWhere({ name: { $regex: 'x' } })).toThrow(/cannot translate/);
    expect(() => toFindOptionsWhere({ $nor: [] })).toThrow(/cannot translate/);
    expect(() => toFindOptionsWhere({ meta: { path: 1 } })).toThrow(/cannot translate/);
    expect(() => toFindOptionsWhere({ $and: [{ $or: [{ a: 1 }] }] })).toThrow(/cannot translate/);
    expect(() => toFindOptionsWhere({ $or: [{ a: 1 }], extra: 2 })).toThrow(/cannot translate/);
  });
});

describe('scopedFindWhere', () => {
  class OwnerGuard extends RealtimeRuleGuard<{ id: string }> {
    canRead(user: { id: string } | null) {
      return user ? { customer_id: user.id } : false;
    }
  }
  const models: ModelConfig[] = [
    { table: 'servers', name: 'servers', primaryKey: 'id', guard: new OwnerGuard() },
  ];
  const rls = new RealtimeRls({ models });

  it('returns the scope where, merged with the caller where', async () => {
    const { allowed, where } = await scopedFindWhere({
      rls,
      model: 'servers',
      user: { id: 'u1' },
      where: { status: 'RUNNING' },
    });
    expect(allowed).toBe(true);
    expect(where).toEqual({ customer_id: 'u1', status: 'RUNNING' });
  });

  it('denies (allowed:false) when the guard denies', async () => {
    const { allowed } = await scopedFindWhere({ rls, model: 'servers', user: null });
    expect(allowed).toBe(false);
  });
});
