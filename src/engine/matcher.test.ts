import { Query } from 'mingo';
import { ChangeEvent, MingoFilter, Row, RowDelta } from '../types';
import { applyMatch, MatchContext } from './matcher';

interface Harness {
  ctx: MatchContext;
  deltas: RowDelta[];
  documentIds: Set<string>;
  refetchCalls: number;
}

function harness(opts?: {
  filter?: MingoFilter;
  initialIds?: string[];
  replaying?: boolean;
  refetchOnUpdate?: boolean;
  refetchRow?: Row | null;
  mapRow?: (r: Row) => Row;
}): Harness {
  const deltas: RowDelta[] = [];
  const documentIds = new Set<string>(opts?.initialIds ?? []);
  let refetchCalls = 0;
  const ctx: MatchContext = {
    effQuery: new Query(opts?.filter ?? { status: 'active' }),
    documentIds,
    // Legacy tests below exercise the pre-existing Set-based transition table, which
    // is what runs during snapshotting/replaying (or as the no-oldRow fallback) —
    // default to `replaying: true` so they keep asserting that path unchanged.
    replaying: opts?.replaying ?? true,
    refetchOnUpdate: opts?.refetchOnUpdate ?? false,
    mapRow: opts?.mapRow ?? ((r) => r),
    refetch: async () => {
      refetchCalls += 1;
      return opts?.refetchRow ?? null;
    },
    emit: (d) => deltas.push(d),
  };
  return {
    ctx,
    deltas,
    documentIds,
    get refetchCalls() {
      return refetchCalls;
    },
  } as Harness;
}

function ev(p: Partial<ChangeEvent> & Pick<ChangeEvent, 'op' | 'pk'>): ChangeEvent {
  return {
    schema: 'public',
    table: 't',
    row: null,
    oldRow: null,
    toastIncomplete: false,
    lsn: '0/1',
    ...p,
  };
}

describe('applyMatch transition table (filter {status: active})', () => {
  it('insert matching -> add + tracks pk', async () => {
    const h = harness();
    await applyMatch(h.ctx, ev({ op: 'insert', pk: 'p1', row: { status: 'active' } }));
    expect(h.deltas).toEqual([{ kind: 'add', pk: 'p1', row: { status: 'active' } }]);
    expect(h.documentIds.has('p1')).toBe(true);
  });

  it('insert non-matching -> nothing', async () => {
    const h = harness();
    await applyMatch(h.ctx, ev({ op: 'insert', pk: 'p1', row: { status: 'archived' } }));
    expect(h.deltas).toEqual([]);
    expect(h.documentIds.size).toBe(0);
  });

  it('insert of an already-tracked pk -> deduped (no duplicate add)', async () => {
    // The snapshot LSN is a lower bound, so an insert already in the snapshot can be
    // replayed; it must not re-emit `add`.
    const h = harness({ initialIds: ['p1'] });
    await applyMatch(h.ctx, ev({ op: 'insert', pk: 'p1', row: { status: 'active' } }));
    expect(h.deltas).toEqual([]);
    expect(h.documentIds.has('p1')).toBe(true);
  });

  it('update entering the set (pk∉set, pass) -> add', async () => {
    const h = harness();
    await applyMatch(h.ctx, ev({ op: 'update', pk: 'p1', row: { status: 'active' } }));
    expect(h.deltas).toEqual([{ kind: 'add', pk: 'p1', row: { status: 'active' } }]);
    expect(h.documentIds.has('p1')).toBe(true);
  });

  it('update staying in the set (pk∈set, pass) -> update', async () => {
    const h = harness({ initialIds: ['p1'] });
    await applyMatch(h.ctx, ev({ op: 'update', pk: 'p1', row: { status: 'active' } }));
    expect(h.deltas).toEqual([{ kind: 'update', pk: 'p1', row: { status: 'active' } }]);
    expect(h.documentIds.has('p1')).toBe(true);
  });

  it('update leaving the set (pk∈set, fail) -> remove + drop', async () => {
    const h = harness({ initialIds: ['p1'] });
    await applyMatch(h.ctx, ev({ op: 'update', pk: 'p1', row: { status: 'archived' } }));
    expect(h.deltas).toEqual([{ kind: 'remove', pk: 'p1' }]);
    expect(h.documentIds.has('p1')).toBe(false);
  });

  it('update never-in-set & non-matching -> nothing', async () => {
    const h = harness();
    await applyMatch(h.ctx, ev({ op: 'update', pk: 'p1', row: { status: 'archived' } }));
    expect(h.deltas).toEqual([]);
  });

  it('delete of a tracked row -> remove + drop', async () => {
    const h = harness({ initialIds: ['p1'] });
    await applyMatch(h.ctx, ev({ op: 'delete', pk: 'p1' }));
    expect(h.deltas).toEqual([{ kind: 'remove', pk: 'p1' }]);
    expect(h.documentIds.has('p1')).toBe(false);
  });

  it('delete of an untracked row -> nothing', async () => {
    const h = harness();
    await applyMatch(h.ctx, ev({ op: 'delete', pk: 'p1' }));
    expect(h.deltas).toEqual([]);
  });

  it('applies mapRow before testing', async () => {
    // Raw rows are snake_case; the filter is on the mapped (camelCase) shape.
    const h = harness({
      filter: { isActive: true },
      mapRow: (r) => ({ ...r, isActive: r.is_active === 't' }),
    });
    await applyMatch(h.ctx, ev({ op: 'insert', pk: 'p1', row: { is_active: 't' } }));
    expect(h.deltas[0]).toMatchObject({ kind: 'add', pk: 'p1' });
  });
});

describe('applyMatch changedColumns', () => {
  it('with oldRow present, changedColumns contains exactly the mapped fields that changed', async () => {
    const h = harness({
      initialIds: ['p1'],
      mapRow: (r) => ({ status: r.status, label: r.label }),
    });
    await applyMatch(
      h.ctx,
      ev({
        op: 'update',
        pk: 'p1',
        row: { status: 'active', label: 'new' },
        oldRow: { status: 'active', label: 'old' },
      }),
    );
    expect(h.deltas).toEqual([
      {
        kind: 'update',
        pk: 'p1',
        row: { status: 'active', label: 'new' },
        changedColumns: ['label'],
      },
    ]);
  });

  it('without oldRow, changedColumns is undefined', async () => {
    const h = harness({ initialIds: ['p1'] });
    await applyMatch(h.ctx, ev({ op: 'update', pk: 'p1', row: { status: 'active' } }));
    expect(h.deltas).toEqual([{ kind: 'update', pk: 'p1', row: { status: 'active' } }]);
    expect((h.deltas[0] as { changedColumns?: string[] }).changedColumns).toBeUndefined();
  });
});

describe('applyMatch TOAST handling', () => {
  const toastEv = ev({
    op: 'update',
    pk: 'p1',
    row: { status: 'active', blob: undefined },
    toastIncomplete: true,
  });

  it('refetchOnUpdate + toastIncomplete -> refetches once and emits the full row', async () => {
    const h = harness({
      initialIds: ['p1'],
      refetchOnUpdate: true,
      refetchRow: { status: 'active', blob: 'FULL' },
    });
    await applyMatch(h.ctx, toastEv);
    expect(h.refetchCalls).toBe(1);
    expect(h.deltas).toEqual([{ kind: 'update', pk: 'p1', row: { status: 'active', blob: 'FULL' } }]);
  });

  it('refetchOnUpdate=false -> no refetch, emits the WAL row (placeholder kept)', async () => {
    const h = harness({ initialIds: ['p1'], refetchOnUpdate: false });
    await applyMatch(h.ctx, toastEv);
    expect(h.refetchCalls).toBe(0);
    expect(h.deltas).toEqual([
      { kind: 'update', pk: 'p1', row: { status: 'active', blob: undefined } },
    ]);
  });

  it('toastIncomplete=false -> never refetches even with refetchOnUpdate', async () => {
    const h = harness({ initialIds: ['p1'], refetchOnUpdate: true, refetchRow: { status: 'active' } });
    await applyMatch(
      h.ctx,
      ev({ op: 'update', pk: 'p1', row: { status: 'active' }, toastIncomplete: false }),
    );
    expect(h.refetchCalls).toBe(0);
  });
});

describe('applyMatch stateless classification (LIVE, REPLICA IDENTITY FULL — no set)', () => {
  // `replaying: false` + an event that carries `oldRow` is the steady-state path: the
  // filter is tested against the old and new row images, `documentIds` is never
  // consulted, and (per Harness) must stay untouched by every case below.

  it('insert matching -> add', async () => {
    const h = harness({ replaying: false });
    await applyMatch(h.ctx, ev({ op: 'insert', pk: 'p1', row: { status: 'active' } }));
    expect(h.deltas).toEqual([{ kind: 'add', pk: 'p1', row: { status: 'active' } }]);
    expect(h.documentIds.size).toBe(0);
  });

  it('insert non-matching -> nothing', async () => {
    const h = harness({ replaying: false });
    await applyMatch(h.ctx, ev({ op: 'insert', pk: 'p1', row: { status: 'archived' } }));
    expect(h.deltas).toEqual([]);
  });

  it('update still matching (pass_old & pass_new) -> update patch', async () => {
    const h = harness({ replaying: false });
    await applyMatch(
      h.ctx,
      ev({
        op: 'update',
        pk: 'p1',
        row: { status: 'active', label: 'new' },
        oldRow: { status: 'active', label: 'old' },
      }),
    );
    expect(h.deltas).toEqual([
      { kind: 'update', pk: 'p1', row: { status: 'active', label: 'new' }, changedColumns: ['label'] },
    ]);
    expect(h.documentIds.size).toBe(0);
  });

  it('update entering the filter (!pass_old & pass_new) -> add', async () => {
    const h = harness({ replaying: false });
    await applyMatch(
      h.ctx,
      ev({
        op: 'update',
        pk: 'p1',
        row: { status: 'active' },
        oldRow: { status: 'archived' },
      }),
    );
    expect(h.deltas).toEqual([{ kind: 'add', pk: 'p1', row: { status: 'active' } }]);
    expect(h.documentIds.size).toBe(0);
  });

  it('update leaving the filter (pass_old & !pass_new) -> remove', async () => {
    const h = harness({ replaying: false });
    await applyMatch(
      h.ctx,
      ev({
        op: 'update',
        pk: 'p1',
        row: { status: 'archived' },
        oldRow: { status: 'active' },
      }),
    );
    expect(h.deltas).toEqual([{ kind: 'remove', pk: 'p1' }]);
    expect(h.documentIds.size).toBe(0);
  });

  it('update irrelevant (!pass_old & !pass_new) -> nothing', async () => {
    const h = harness({ replaying: false });
    await applyMatch(
      h.ctx,
      ev({
        op: 'update',
        pk: 'p1',
        row: { status: 'archived' },
        oldRow: { status: 'archived' },
      }),
    );
    expect(h.deltas).toEqual([]);
  });

  it('delete of a matching row (pass_old) -> remove', async () => {
    const h = harness({ replaying: false });
    await applyMatch(h.ctx, ev({ op: 'delete', pk: 'p1', oldRow: { status: 'active' } }));
    expect(h.deltas).toEqual([{ kind: 'remove', pk: 'p1' }]);
    expect(h.documentIds.size).toBe(0);
  });

  it('delete of a non-matching row (!pass_old) -> nothing', async () => {
    const h = harness({ replaying: false });
    await applyMatch(h.ctx, ev({ op: 'delete', pk: 'p1', oldRow: { status: 'archived' } }));
    expect(h.deltas).toEqual([]);
  });
});

describe('applyMatch fallback (LIVE, but no oldRow — e.g. not REPLICA IDENTITY FULL)', () => {
  // Even once LIVE, an event with no old image can't compute pass_old, so the
  // engine must fall back to the legacy Set-membership decider for that event.

  it('update with no oldRow while live still classifies via documentIds', async () => {
    const h = harness({ replaying: false, initialIds: ['p1'] });
    await applyMatch(h.ctx, ev({ op: 'update', pk: 'p1', row: { status: 'archived' } }));
    expect(h.deltas).toEqual([{ kind: 'remove', pk: 'p1' }]);
    expect(h.documentIds.has('p1')).toBe(false);
  });

  it('delete with no oldRow while live still classifies via documentIds', async () => {
    const h = harness({ replaying: false, initialIds: ['p1'] });
    await applyMatch(h.ctx, ev({ op: 'delete', pk: 'p1' }));
    expect(h.deltas).toEqual([{ kind: 'remove', pk: 'p1' }]);
    expect(h.documentIds.has('p1')).toBe(false);
  });
});
