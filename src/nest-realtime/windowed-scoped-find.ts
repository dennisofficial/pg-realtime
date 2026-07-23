import {
  And,
  Equal,
  FindOperator,
  In,
  IsNull,
  LessThan,
  LessThanOrEqual,
  MoreThan,
  MoreThanOrEqual,
  Not,
  type DataSource,
  type EntityMetadata,
  type EntityTarget,
  type FindManyOptions,
  type FindOptionsOrder,
  type FindOptionsWhere,
  type ObjectLiteral,
} from 'typeorm';
import type { MingoFilter, Row } from '../types';
import type { QuerySpec } from '../socketio/mux';
import { getRealtimePublish } from './realtime.decorator';

/** An entity class constructor — typed narrower than `Function` per house lint rules. */
type EntityClass = new (...args: unknown[]) => object;

/** The decoded shape of a keyset cursor: the sort column's value and the tiebreaker PK. */
interface Cursor {
  v: unknown;
  pk: unknown;
}

/**
 * Host-supplied plugs `createWindowedScopedFind` needs but must not own — keeping this
 * package free of a dependency on any particular field-exposure or row-level-security scheme
 * (e.g. `@workspace/nestjs-rls`, which itself depends on `pg-realtime` and would cycle back).
 */
export interface WindowedScopedFindOptions {
  /** TypeORM DataSource to discover `@Realtime`-decorated entities off. */
  dataSource: DataSource;
  /** The `propertyName -> outputName` map of columns exposed for an entity — used for both
   *  the output projection and the sortable/filterable-column guardrail (reverse map). */
  resolveExposed: (target: Function) => Map<string, string>;
  /** Performs the actual (RLS-scoped, or otherwise access-controlled) fetch. The app plug —
   *  e.g. `(entity, opts) => db.scoped(entity).find(opts)`. */
  runScopedFind: <E extends ObjectLiteral>(
    entity: EntityTarget<E>,
    findOptions: FindManyOptions<E>,
  ) => Promise<E[]>;
}

/**
 * Builds a Mode B windowed-query `scopedFind` plug for {@link RealtimeNestConfig}: translates a
 * `QuerySpec` (filter/sort/limit/offset/after) into a scoped TypeORM fetch against the
 * `@Realtime`-decorated entity registered as `model`, then projects the results through the
 * host's `@Expose`-style column map.
 *
 * Handles: `spec.filter` -> `WHERE` (mingo-subset translation), `spec.sort` -> `ORDER BY`
 * (through the exposed-column reverse map), `spec.offset` via `take`/`skip`, `spec.after`
 * keyset-cursor pagination (single sort column + single-column PK only — multi-column keyset
 * is not supported and throws), and output projection via the exposed-column map. Never
 * imports `@workspace/nestjs-rls` — the actual scoped fetch comes in as {@link
 * WindowedScopedFindOptions.runScopedFind}.
 */
export function createWindowedScopedFind(
  options: WindowedScopedFindOptions,
): (model: string, spec: QuerySpec, principal: unknown) => Promise<Array<{ pk: string; row: Row }>> {
  let entityByModel: Map<string, EntityClass> | null = null;

  function resolveModel(model: string): { entity: EntityClass; metadata: EntityMetadata } {
    if (!entityByModel) {
      const byModel = new Map<string, EntityClass>();
      for (const metadata of options.dataSource.entityMetadatas) {
        if (typeof metadata.target !== 'function') continue;
        const publish = getRealtimePublish(metadata.target);
        if (!publish) continue;
        byModel.set(publish.name ?? metadata.tableName, metadata.target as EntityClass);
      }
      entityByModel = byModel;
    }

    const entity = entityByModel.get(model);
    if (!entity) throw new Error(`scopedFind: no @Realtime entity registered as "${model}"`);
    return { entity, metadata: options.dataSource.getMetadata(entity) };
  }

  async function findKeyset(
    entity: EntityClass,
    metadata: EntityMetadata,
    spec: QuerySpec,
    filterWhere: FindOptionsWhere<unknown> | FindOptionsWhere<unknown>[] | undefined,
    reverseExposed: Map<string, string>,
    model: string,
  ): Promise<unknown[]> {
    if (spec.offset !== undefined) {
      throw new Error(
        'scopedFind: cannot combine spec.offset with spec.after (keyset) pagination on ' +
          `"${model}" — keyset supersedes offset; pass one or the other`,
      );
    }
    if (!spec.sort || spec.sort.length !== 1) {
      throw new Error(
        `scopedFind: keyset pagination (spec.after) on "${model}" requires exactly one sort ` +
          'column — multi-column keyset sort is not supported',
      );
    }
    if (metadata.primaryColumns.length !== 1) {
      throw new Error(
        `scopedFind: keyset pagination on "${model}" requires a single-column primary key`,
      );
    }

    const [[field, direction]] = spec.sort;
    const property = reverseExposed.get(field);
    if (!property) {
      throw new Error(
        `scopedFind: field "${field}" is not exposed on "${model}" — cannot sort by it`,
      );
    }
    const pkProperty = metadata.primaryColumns[0].propertyName;

    const cursor = decodeCursor(spec.after);
    const dir: 'ASC' | 'DESC' = direction === 'asc' ? 'ASC' : 'DESC';
    const primaryOp = dir === 'ASC' ? MoreThan(cursor.v) : LessThan(cursor.v);

    const keysetBranches: Record<string, unknown>[] = [
      { [property]: primaryOp },
      { [property]: Equal(cursor.v), [pkProperty]: MoreThan(cursor.pk) },
    ];

    const order = { [property]: dir, [pkProperty]: 'ASC' } as FindOptionsOrder<unknown>;

    return options.runScopedFind(entity, {
      where: mergeKeysetWhere(filterWhere, keysetBranches),
      order,
      take: spec.limit,
    });
  }

  return async function scopedFind(
    model: string,
    spec: QuerySpec,
    _principal: unknown,
  ): Promise<Array<{ pk: string; row: Row }>> {
    const { entity, metadata } = resolveModel(model);
    const exposed = options.resolveExposed(entity);
    const reverseExposed = new Map(
      Array.from(exposed, ([property, outputName]) => [outputName, property]),
    );

    const where = spec.filter ? toWhere(spec.filter, reverseExposed, model) : undefined;

    const rows =
      spec.after !== undefined
        ? await findKeyset(entity, metadata, spec, where, reverseExposed, model)
        : await options.runScopedFind(entity, {
            where,
            order: spec.sort ? toOrder(spec.sort, reverseExposed, model) : undefined,
            take: spec.limit,
            skip: spec.offset,
          });

    const pkProperties = metadata.primaryColumns.map((c) => c.propertyName);
    return rows.map((row) => ({
      pk: buildPk(pkProperties, row as Record<string, unknown>),
      row: project(row as Record<string, unknown>, exposed),
    }));
  };
}

/** AND-merge each `filterWhere` branch (or none) with each keyset OR-branch — a cross
 *  product, matching how `@workspace/nestjs-rls` composes `$or` filters with scope. */
function mergeKeysetWhere(
  filterWhere: FindOptionsWhere<unknown> | FindOptionsWhere<unknown>[] | undefined,
  keysetBranches: Record<string, unknown>[],
): FindOptionsWhere<unknown>[] {
  if (filterWhere === undefined) return keysetBranches;
  const filterBranches = Array.isArray(filterWhere) ? filterWhere : [filterWhere];
  return filterBranches.flatMap((f) =>
    keysetBranches.map(
      (k) => andMergeWhere(f as Record<string, unknown>, k) as FindOptionsWhere<unknown>,
    ),
  );
}

function andMergeWhere(a: Record<string, unknown>, b: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...a };
  for (const [key, value] of Object.entries(b)) {
    out[key] = key in out ? And(asOperator(out[key]), asOperator(value)) : value;
  }
  return out;
}

function asOperator(value: unknown): FindOperator<unknown> {
  return value instanceof FindOperator ? value : Equal(value);
}

/** Encode a keyset cursor from a result row's sort-column value and PK — for callers that
 *  want to hand the client a cursor for the next page. Opaque on the wire (base64 JSON). */
export function encodeCursor(sortValue: unknown, pk: unknown): string {
  return Buffer.from(JSON.stringify({ v: sortValue, pk } satisfies Cursor)).toString('base64');
}

function decodeCursor(after: unknown): Cursor {
  if (typeof after !== 'string' || after.length === 0) {
    throw new Error('scopedFind: spec.after must be an opaque string cursor');
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(after, 'base64').toString('utf8'));
  } catch {
    throw new Error('scopedFind: spec.after is not a valid cursor');
  }
  if (typeof parsed !== 'object' || parsed === null || !('v' in parsed) || !('pk' in parsed)) {
    throw new Error('scopedFind: spec.after is not a valid cursor');
  }
  return parsed as Cursor;
}

function toWhere(
  filter: MingoFilter,
  reverseExposed: Map<string, string>,
  model: string,
): FindOptionsWhere<unknown> | FindOptionsWhere<unknown>[] {
  return toFindOptionsWhere(remapFields(filter, reverseExposed, model));
}

/** Translate wire (`@Expose`d) field names back to entity property names, recursing through
 *  `$and`/`$or`; any other field not exposed on the entity throws rather than silently
 *  leaking/hiding rows. */
function remapFields(
  filter: MingoFilter,
  reverseExposed: Map<string, string>,
  model: string,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(filter)) {
    if (key === '$and' || key === '$or') {
      out[key] = (value as Record<string, unknown>[]).map((branch) =>
        remapFields(branch, reverseExposed, model),
      );
      continue;
    }
    if (key.startsWith('$')) {
      throw new Error(`scopedFind: unsupported top-level operator "${key}" on "${model}"`);
    }
    const property = reverseExposed.get(key);
    if (!property) {
      throw new Error(
        `scopedFind: field "${key}" is not exposed on "${model}" — cannot filter by it`,
      );
    }
    out[property] = value;
  }
  return out;
}

function toOrder(
  sort: Array<[string, 'asc' | 'desc']>,
  reverseExposed: Map<string, string>,
  model: string,
): FindOptionsOrder<unknown> {
  const order: Record<string, 'ASC' | 'DESC'> = {};
  for (const [field, direction] of sort) {
    const property = reverseExposed.get(field);
    if (!property) {
      throw new Error(
        `scopedFind: field "${field}" is not exposed on "${model}" — cannot sort by it`,
      );
    }
    order[property] = direction === 'asc' ? 'ASC' : 'DESC';
  }
  return order;
}

function project(row: Record<string, unknown>, exposed: Map<string, string>): Row {
  const out: Row = {};
  for (const [property, outputName] of exposed) {
    out[outputName] = row[property];
  }
  return out;
}

function buildPk(pkProperties: string[], row: Record<string, unknown>): string {
  return JSON.stringify(pkProperties.map((p) => row[p] ?? null));
}

// ─── filter -> WHERE translation ──────────────────────────────────────────────
//
// A self-contained copy of `@workspace/nestjs-rls/typeorm`'s `toFindOptionsWhere` — this
// package cannot depend on `@workspace/nestjs-rls` (it depends on `pg-realtime` and would
// cycle back). Only the safe, unambiguous mingo subset is translated; anything that could
// mistranslate (and thus leak or hide rows) throws instead of guessing.

const COMPARISON: Record<string, (v: any) => FindOperator<any>> = {
  $eq: (v) => (v === null ? IsNull() : Equal(v)),
  $ne: (v) => (v === null ? Not(IsNull()) : Not(v)),
  $in: (v) => In(asArray(v, '$in')),
  $nin: (v) => Not(In(asArray(v, '$nin'))),
  $gt: (v) => MoreThan(v),
  $gte: (v) => MoreThanOrEqual(v),
  $lt: (v) => LessThan(v),
  $lte: (v) => LessThanOrEqual(v),
};

function toFindOptionsWhere<T = any>(
  filter: MingoFilter,
): FindOptionsWhere<T> | FindOptionsWhere<T>[] {
  const keys = Object.keys(filter);

  if (keys.includes('$or')) {
    if (keys.length > 1) {
      throw unsupported(
        '$or alongside sibling keys at the top level — wrap the siblings in $and, or build the query manually',
      );
    }
    return (filter.$or as MingoFilter[]).flatMap((branch) => {
      const w = toFindOptionsWhere<T>(branch);
      return Array.isArray(w) ? w : [w];
    });
  }

  const out: Record<string, unknown> = {};
  for (const key of keys) {
    const val = (filter as Record<string, unknown>)[key];
    if (key === '$and') {
      for (const sub of val as MingoFilter[]) {
        const w = toFindOptionsWhere<T>(sub);
        if (Array.isArray(w)) {
          throw unsupported('$or nested under $and — not representable as a TypeORM where');
        }
        mergeAnd(out, w as Record<string, unknown>);
      }
    } else if (key.startsWith('$')) {
      throw unsupported(`top-level operator "${key}"`);
    } else {
      out[key] = translateCondition(val, key);
    }
  }
  return out as FindOptionsWhere<T>;
}

function translateCondition(cond: unknown, field: string): unknown {
  if (cond === null) return IsNull();
  if (isScalar(cond)) return cond;
  if (cond && typeof cond === 'object' && !Array.isArray(cond)) {
    const entries = Object.entries(cond as Record<string, unknown>);
    if (entries.length === 0) throw unsupported(`empty condition for field "${field}"`);
    if (!entries.every(([op]) => op.startsWith('$'))) {
      throw unsupported(`nested-object / JSON-path condition for field "${field}"`);
    }
    const ops = entries.map(([op, v]) => {
      const fn = COMPARISON[op];
      if (!fn) {
        throw unsupported(
          `operator "${op}" on field "${field}" — keep filters to equality/$in/$nin/comparisons, or build the query manually`,
        );
      }
      return fn(v);
    });
    return ops.length === 1 ? ops[0] : And(...ops);
  }
  throw unsupported(`value ${JSON.stringify(cond)} for field "${field}"`);
}

function mergeAnd(target: Record<string, unknown>, source: Record<string, unknown>): void {
  for (const [k, v] of Object.entries(source)) {
    target[k] = k in target ? And(asOperator(target[k]), asOperator(v)) : v;
  }
}

function isScalar(v: unknown): boolean {
  return (
    typeof v === 'string' ||
    typeof v === 'number' ||
    typeof v === 'boolean' ||
    typeof v === 'bigint' ||
    v instanceof Date
  );
}

function asArray(v: unknown, op: string): unknown[] {
  if (!Array.isArray(v)) throw unsupported(`${op} expects an array`);
  return v;
}

function unsupported(msg: string): Error {
  return new Error(`scopedFind: cannot translate ${msg}`);
}
