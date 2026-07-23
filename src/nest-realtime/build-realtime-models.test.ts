import type { DataSource } from 'typeorm';
import { buildRealtimeModels } from './build-realtime-models';
import { Realtime } from './realtime.decorator';

@Realtime()
class Job {
  id!: string;
  title!: string;
}

@Realtime({ name: 'job-links' })
class JobLink {
  jobId!: string;
  linkedJobId!: string;
}

class NotRealtime {
  id!: string;
}

function makeMetadata(target: Function, tableName: string, primaryDbNames: string[], columns: Array<{ propertyName: string; databaseName: string }>) {
  return {
    target,
    tableName,
    primaryColumns: primaryDbNames.map((databaseName) => ({ databaseName })),
    columns,
  };
}

function makeDataSource(entityMetadatas: unknown[]): DataSource {
  return { entityMetadatas } as unknown as DataSource;
}

describe('buildRealtimeModels', () => {
  it('skips entities without @Realtime', () => {
    const dataSource = makeDataSource([
      makeMetadata(NotRealtime, 'not_realtime', ['id'], [{ propertyName: 'id', databaseName: 'id' }]),
    ]);

    const models = buildRealtimeModels(dataSource, {
      resolveExposed: () => new Map([['id', 'id']]),
      buildGuard: () => undefined,
    });

    expect(models).toEqual([]);
  });

  it('builds a model with a single primary key, defaulting name to the table name', () => {
    const dataSource = makeDataSource([
      makeMetadata(Job, 'jobs', ['id'], [
        { propertyName: 'id', databaseName: 'id' },
        { propertyName: 'title', databaseName: 'title' },
      ]),
    ]);
    const guard = { canRead: () => true };

    const models = buildRealtimeModels(dataSource, {
      resolveExposed: (target) => (target === Job ? new Map([['id', 'id'], ['title', 'displayTitle']]) : new Map()),
      buildGuard: () => guard as never,
    });

    expect(models).toHaveLength(1);
    const [model] = models;
    expect(model.name).toBe('jobs');
    expect(model.table).toBe('jobs');
    expect(model.primaryKey).toBe('id');
    expect(model.replicaIdentityFull).toBe(true);
    expect(model.guard).toBe(guard);

    const mapped = model.mapRow!({ id: 'j1', title: 'Ship it' });
    expect(mapped).toEqual({ id: 'j1', displayTitle: 'Ship it' });
  });

  it('honors the @Realtime({ name }) override and composite primary keys', () => {
    const dataSource = makeDataSource([
      makeMetadata(JobLink, 'job_links', ['job_id', 'linked_job_id'], [
        { propertyName: 'jobId', databaseName: 'job_id' },
        { propertyName: 'linkedJobId', databaseName: 'linked_job_id' },
      ]),
    ]);

    const models = buildRealtimeModels(dataSource, {
      resolveExposed: () => new Map([['jobId', 'jobId'], ['linkedJobId', 'linkedJobId']]),
      buildGuard: () => undefined,
    });

    expect(models).toHaveLength(1);
    const [model] = models;
    expect(model.name).toBe('job-links');
    expect(model.table).toBe('job_links');
    expect(model.primaryKey).toEqual(['job_id', 'linked_job_id']);
  });

  it('throws when an entity has no primary column', () => {
    const dataSource = makeDataSource([makeMetadata(Job, 'jobs', [], [])]);

    expect(() =>
      buildRealtimeModels(dataSource, {
        resolveExposed: () => new Map([['id', 'id']]),
        buildGuard: () => undefined,
      }),
    ).toThrow(/no primary column/);
  });

  it('throws when resolveExposed returns nothing to publish', () => {
    const dataSource = makeDataSource([
      makeMetadata(Job, 'jobs', ['id'], [{ propertyName: 'id', databaseName: 'id' }]),
    ]);

    expect(() =>
      buildRealtimeModels(dataSource, {
        resolveExposed: () => new Map(),
        buildGuard: () => undefined,
      }),
    ).toThrow(/no exposed properties/);
  });
});
