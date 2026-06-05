import { Query } from 'mingo';
import { ChangeEvent, RowDelta } from '../types';

// Mock the DB layer so we can script the snapshot rows + captured LSN, with no Postgres.
jest.mock('./db');
import * as db from './db';
import { ResolvedModel } from './resolved-model';
import { SubscriptionImpl } from './snapshot';

const mockedSnapshot = db.snapshotTable as jest.MockedFunction<typeof db.snapshotTable>;

const model: ResolvedModel = {
  name: 'servers',
  schema: 'public',
  table: 'servers',
  routingKey: 'public.servers',
  pkCols: ['id'],
  refetchOnUpdate: false,
  mapRow: (r) => r,
};

function makeSub(filter: Record<string, unknown>) {
  const deltas: RowDelta[] = [];
  const sub = new SubscriptionImpl({
    id: 'sub-1',
    model,
    effQuery: new Query(filter),
    coarse: undefined,
    user: null,
    pool: {} as never,
    snapshotMaxRows: 1000,
    onClose: () => {},
  });
  return { sub, deltas, attach: () => sub.on((d) => deltas.push(d)) };
}

function ev(p: Partial<ChangeEvent> & Pick<ChangeEvent, 'op' | 'pk' | 'lsn'>): ChangeEvent {
  return { schema: 'public', table: 'servers', row: null, oldRow: null, toastIncomplete: false, ...p };
}

const flush = async () => {
  for (let i = 0; i < 5; i++) await new Promise((r) => setImmediate(r));
};

beforeEach(() => mockedSnapshot.mockReset());

describe('snapshot↔stream consistency', () => {
  it('NO PHANTOM: a row deleted in the snapshot→live window is retracted', async () => {
    // Snapshot (taken at 0/100) includes r1.
    mockedSnapshot.mockResolvedValue({
      snapshotLsn: '0/100',
      rows: [{ id: 'r1', status: 'active' }],
    });
    const { sub, deltas, attach } = makeSub({ status: 'active' });

    // A DELETE commits in the window (lsn 0/200 > snapshotLsn) and is routed BEFORE
    // the first handler — it must be buffered, then replayed.
    sub.ingest(ev({ op: 'delete', pk: '["r1"]', lsn: '0/200' }));

    attach(); // triggers snapshot + replay
    await flush();

    expect(deltas).toEqual([
      { kind: 'data', rows: [{ pk: '["r1"]', row: { id: 'r1', status: 'active' } }] },
      { kind: 'remove', pk: '["r1"]' },
    ]);
  });

  it('drops events already reflected in the snapshot (lsn <= snapshotLsn)', async () => {
    mockedSnapshot.mockResolvedValue({
      snapshotLsn: '0/100',
      rows: [{ id: 'r1', status: 'active' }],
    });
    const { sub, deltas, attach } = makeSub({ status: 'active' });

    // An INSERT with lsn 0/050 <= snapshotLsn is already in the snapshot — must NOT re-add.
    sub.ingest(ev({ op: 'insert', pk: '["r2"]', lsn: '0/050', row: { id: 'r2', status: 'active' } }));

    attach();
    await flush();

    expect(deltas).toEqual([
      { kind: 'data', rows: [{ pk: '["r1"]', row: { id: 'r1', status: 'active' } }] },
    ]);
  });

  it('dedupes a replayed insert whose row is already in the snapshot (lower-bound LSN)', async () => {
    // With snapshotLsn captured as a lower bound, an insert already reflected in the
    // snapshot may be replayed with lsn > snapshotLsn — it must not duplicate `add`.
    mockedSnapshot.mockResolvedValue({
      snapshotLsn: '0/100',
      rows: [{ id: 'r1', status: 'active' }],
    });
    const { sub, deltas, attach } = makeSub({ status: 'active' });
    sub.ingest(ev({ op: 'insert', pk: '["r1"]', lsn: '0/200', row: { id: 'r1', status: 'active' } }));

    attach();
    await flush();

    expect(deltas).toEqual([
      { kind: 'data', rows: [{ pk: '["r1"]', row: { id: 'r1', status: 'active' } }] },
    ]);
  });

  it('applies live events after going LIVE', async () => {
    mockedSnapshot.mockResolvedValue({ snapshotLsn: '0/100', rows: [] });
    const { sub, deltas, attach } = makeSub({ status: 'active' });

    attach();
    await flush(); // snapshot done -> live

    sub.ingest(ev({ op: 'insert', pk: '["r3"]', lsn: '0/300', row: { id: 'r3', status: 'active' } }));
    await flush();

    expect(deltas).toEqual([
      { kind: 'data', rows: [] },
      { kind: 'add', pk: '["r3"]', row: { id: 'r3', status: 'active' } },
    ]);
  });

  it('snapshot membership uses the same mingo query (non-matching rows excluded)', async () => {
    mockedSnapshot.mockResolvedValue({
      snapshotLsn: '0/100',
      rows: [
        { id: 'r1', status: 'active' },
        { id: 'r2', status: 'archived' }, // coarse fetch returned it; mingo must exclude it
      ],
    });
    const { sub, deltas, attach } = makeSub({ status: 'active' });
    attach();
    await flush();

    expect(deltas).toEqual([
      { kind: 'data', rows: [{ pk: '["r1"]', row: { id: 'r1', status: 'active' } }] },
    ]);
  });
});
