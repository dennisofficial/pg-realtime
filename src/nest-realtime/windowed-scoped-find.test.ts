import type { DataSource, EntityTarget, FindManyOptions, ObjectLiteral } from 'typeorm';
import type { QuerySpec } from '../socketio/mux';
import { createWindowedScopedFind, encodeCursor } from './windowed-scoped-find';
import { Realtime } from './realtime.decorator';

@Realtime()
class Job {
  id!: string;
  createdAt!: string;
}

function makeMetadata() {
  return {
    target: Job,
    tableName: 'jobs',
    primaryColumns: [{ propertyName: 'id' }],
  };
}

function makeDataSource(metadata: ReturnType<typeof makeMetadata>): DataSource {
  return {
    entityMetadatas: [metadata],
    getMetadata: () => metadata,
  } as unknown as DataSource;
}

describe('createWindowedScopedFind', () => {
  function setup(exposed = new Map([['id', 'id'], ['createdAt', 'createdAt']])) {
    const metadata = makeMetadata();
    const dataSource = makeDataSource(metadata);
    const recordedOptions: FindManyOptions<unknown>[] = [];
    let nextRows: unknown[] = [{ id: '1', createdAt: '2020-01-01' }];

    const scopedFind = createWindowedScopedFind({
      dataSource,
      resolveExposed: () => exposed,
      runScopedFind: async <E extends ObjectLiteral>(
        _entity: EntityTarget<E>,
        opts: FindManyOptions<E>,
      ) => {
        recordedOptions.push(opts as FindManyOptions<unknown>);
        return nextRows as E[];
      },
    });

    return {
      scopedFind,
      recordedOptions,
      setNextRows: (rows: unknown[]) => (nextRows = rows),
    };
  }

  it('offset path: builds take/skip + order, projects output through @Expose', async () => {
    const { scopedFind, recordedOptions } = setup();
    const spec: QuerySpec = { sort: [['createdAt', 'asc']], limit: 10, offset: 5 };

    const result = await scopedFind('jobs', spec, {});

    expect(recordedOptions).toHaveLength(1);
    expect(recordedOptions[0]).toEqual({
      where: undefined,
      order: { createdAt: 'ASC' },
      take: 10,
      skip: 5,
    });
    expect(result).toEqual([{ pk: JSON.stringify(['1']), row: { id: '1', createdAt: '2020-01-01' } }]);
  });

  it('keyset path: builds the (col,pk) where + order + take, decoding the cursor', async () => {
    const { scopedFind, recordedOptions } = setup();
    const cursor = encodeCursor('2020-01-01', '1');
    const spec: QuerySpec = { sort: [['createdAt', 'asc']], limit: 10, after: cursor };

    await scopedFind('jobs', spec, {});

    expect(recordedOptions).toHaveLength(1);
    const opts = recordedOptions[0];
    expect(opts.take).toBe(10);
    expect(opts.order).toEqual({ createdAt: 'ASC', id: 'ASC' });
    expect(Array.isArray(opts.where)).toBe(true);
    const where = opts.where as Record<string, unknown>[];
    expect(where).toHaveLength(2);
    expect(where[0].createdAt).toBeDefined();
    expect(where[1].createdAt).toBeDefined();
    expect(where[1].id).toBeDefined();
  });

  it('throws when sorting by a field not exposed on the model', async () => {
    const { scopedFind } = setup(new Map([['id', 'id']]));
    const spec: QuerySpec = { sort: [['createdAt', 'asc']] };

    await expect(scopedFind('jobs', spec, {})).rejects.toThrow(/not exposed/);
  });

  it('throws when filtering by a field not exposed on the model', async () => {
    const { scopedFind } = setup(new Map([['id', 'id']]));
    const spec: QuerySpec = { filter: { createdAt: '2020-01-01' } };

    await expect(scopedFind('jobs', spec, {})).rejects.toThrow(/not exposed/);
  });

  it('throws on multi-column sort + keyset (spec.after)', async () => {
    const { scopedFind } = setup();
    const spec: QuerySpec = {
      sort: [
        ['createdAt', 'asc'],
        ['id', 'asc'],
      ],
      after: encodeCursor('2020-01-01', '1'),
    };

    await expect(scopedFind('jobs', spec, {})).rejects.toThrow(/exactly one sort column/);
  });

  it('throws when combining spec.offset with spec.after', async () => {
    const { scopedFind } = setup();
    const spec: QuerySpec = {
      sort: [['createdAt', 'asc']],
      after: encodeCursor('2020-01-01', '1'),
      offset: 5,
    };

    await expect(scopedFind('jobs', spec, {})).rejects.toThrow(/cannot combine spec.offset/);
  });

  it('throws for an unregistered model', async () => {
    const { scopedFind } = setup();
    await expect(scopedFind('not-a-model', {}, {})).rejects.toThrow(/no @Realtime entity registered/);
  });
});
