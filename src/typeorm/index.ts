import {
  And,
  Equal,
  FindOperator,
  type FindOptionsWhere,
  In,
  IsNull,
  LessThan,
  LessThanOrEqual,
  MoreThan,
  MoreThanOrEqual,
  Not,
} from 'typeorm';
import { RealtimeRls } from '../rls';
import { GuardAction, MingoFilter } from '../types';

/**
 * Translate a guard's mingo scope into a TypeORM `FindOptionsWhere`, so row-level
 * security is enforced at the DATABASE — never by discarding rows after the fetch
 * (which wastes the query and breaks pagination counts).
 *
 * Optional peer: `typeorm`. Only the safe, unambiguous subset is translated; anything
 * that could mistranslate (and thus leak or hide rows) throws instead of guessing:
 *   - equality (incl. `null` -> IsNull)
 *   - $eq $ne $in $nin $gt $gte $lt $lte
 *   - top-level $and (merged) and $or (array of wheres)
 * Unsupported (throws): $regex, $not/$nor, JSON-path conditions, $or nested in $and, …
 */
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

export function toFindOptionsWhere<T = any>(
  filter: MingoFilter,
): FindOptionsWhere<T> | FindOptionsWhere<T>[] {
  const keys = Object.keys(filter);

  // Top-level $or -> an array of wheres (TypeORM's OR form).
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

/**
 * Convenience: resolve a model's guard scope for a user and return a TypeORM `where`
 * (the scope ANDed with your own `where`), ready for `repo.findAndCount`. `allowed:false`
 * means the guard denied access outright — short-circuit to an empty page.
 *
 *   const { allowed, where } = await scopedFindWhere({ rls, model: 'servers', user, where: { status: 'RUNNING' } });
 *   if (!allowed) return { items: [], total: 0 };
 *   const [items, total] = await serverRepo.findAndCount({ where, skip, take });
 */
export async function scopedFindWhere<T = any>(opts: {
  rls: RealtimeRls;
  model: string;
  user?: unknown;
  action?: GuardAction;
  where?: FindOptionsWhere<T>;
}): Promise<{ allowed: boolean; where: FindOptionsWhere<T> | FindOptionsWhere<T>[] }> {
  const { allowed, filter } = await opts.rls.scope({
    model: opts.model,
    user: opts.user,
    action: opts.action,
  });
  if (!allowed) return { allowed: false, where: {} as FindOptionsWhere<T> };

  const scoped = toFindOptionsWhere<T>(filter);
  if (!opts.where) return { allowed: true, where: scoped };

  // AND the caller's conditions into the scope. (Distinct fields is the norm for RLS;
  // a field present in both — caller wins on collision — should be composed by hand.)
  const extra = opts.where;
  const where = Array.isArray(scoped)
    ? scoped.map((w) => ({ ...w, ...extra }))
    : { ...scoped, ...extra };
  return { allowed: true, where };
}

// ─── internals ───────────────────────────────────────────────────────────────

function translateCondition(cond: unknown, field: string): unknown {
  if (cond === null) return IsNull();
  if (isScalar(cond)) return cond; // implicit equality
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
          `operator "${op}" on field "${field}" — keep RLS scopes to equality/$in/$nin/comparisons, or build the query manually`,
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

function asOperator(v: unknown): FindOperator<any> {
  return v instanceof FindOperator ? v : Equal(v);
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
  return new Error(`pg-realtime/typeorm: cannot translate ${msg}`);
}
