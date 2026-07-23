import type { DataSource, EntityMetadata } from 'typeorm';
import type { ModelConfig, Row } from '../types';
import { getRealtimePublish } from './realtime.decorator';

/**
 * Host-supplied plugs `buildRealtimeModels` needs but must not own — keeping this package
 * free of a dependency on any particular field-exposure or auth scheme (e.g.
 * `@workspace/nestjs-rls`, which itself depends on `pg-realtime` and would cycle back).
 */
export interface BuildRealtimeModelsOptions {
  /** The `propertyName -> outputName` map of columns allowed onto the wire for this entity. */
  resolveExposed: (target: Function) => Map<string, string>;
  /** Builds the row-level auth guard for this entity. */
  buildGuard: (target: Function) => ModelConfig['guard'];
}

/**
 * Discovers every `@Realtime`-decorated TypeORM entity on `dataSource` and turns it into a
 * `ModelConfig` the realtime engine can serve: table/name/primary-key off the entity metadata,
 * `mapRow` off the host's `resolveExposed` plug, `guard` off the host's `buildGuard` plug, and
 * `replicaIdentityFull: true` (an `@Realtime` entity is expected to run under
 * `REPLICA IDENTITY FULL`, so updates always carry an old-row image).
 */
export function buildRealtimeModels(
  dataSource: DataSource,
  options: BuildRealtimeModelsOptions,
): ModelConfig[] {
  const models: ModelConfig[] = [];
  for (const metadata of dataSource.entityMetadatas) {
    if (typeof metadata.target !== 'function') continue;
    const publish = getRealtimePublish(metadata.target);
    if (!publish) continue;
    models.push(buildModel(metadata, publish, options));
  }
  return models;
}

function buildModel(
  metadata: EntityMetadata,
  publish: { name?: string },
  options: BuildRealtimeModelsOptions,
): ModelConfig {
  const target = metadata.target as Function;
  const exposed = options.resolveExposed(target);
  if (exposed.size === 0) {
    throw new Error(
      `@Realtime entity '${metadata.tableName}' has no exposed properties — nothing to publish`,
    );
  }

  const databaseNameByProperty = new Map<string, string>();
  for (const column of metadata.columns) {
    databaseNameByProperty.set(column.propertyName, column.databaseName);
  }

  const pkCols = metadata.primaryColumns.map((c) => c.databaseName);
  if (pkCols.length === 0) {
    throw new Error(`@Realtime entity '${metadata.tableName}' has no primary column`);
  }

  return {
    table: metadata.tableName,
    name: publish.name ?? metadata.tableName,
    primaryKey: pkCols.length === 1 ? pkCols[0] : pkCols,
    guard: options.buildGuard(target),
    replicaIdentityFull: true,
    mapRow: (raw: Row): Row => {
      const out: Row = {};
      for (const [property, outputName] of exposed) {
        const databaseName = databaseNameByProperty.get(property);
        if (!databaseName) {
          throw new Error(
            `@Realtime entity '${metadata.tableName}': exposed property '${property}' is not a mapped column`,
          );
        }
        out[outputName] = raw[databaseName];
      }
      return out;
    },
  };
}
