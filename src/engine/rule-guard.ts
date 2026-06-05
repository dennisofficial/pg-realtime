import { GuardAction, GuardDecision, MingoFilter, RealtimeRuleGuard, Row } from '../types';

/**
 * Combine several mingo filters with `$and`, dropping empty ones.
 *  - 0 effective filters -> {} (match everything the model allows)
 *  - 1 effective filter   -> that filter (no needless $and wrapper)
 *  - n                    -> { $and: [...] }
 */
export function mingoAnd(...filters: Array<MingoFilter | undefined | null>): MingoFilter {
  const real = filters.filter(
    (f): f is MingoFilter => !!f && typeof f === 'object' && Object.keys(f).length > 0,
  );
  if (real.length === 0) return {};
  if (real.length === 1) return real[0];
  return { $and: real };
}

export interface GuardResult {
  allowed: boolean;
  /** The mingo scope to AND into the effective query. `{}` when the guard allows all. */
  scope: MingoFilter;
}

/**
 * Evaluate a model's guard for a principal and action.
 *  - guard returns `false`        -> deny (allowed=false)
 *  - guard returns `true`/absent  -> allow all (scope {})
 *  - guard returns a MingoFilter  -> allow, scoped to that filter
 *
 * Write actions fall back to `canRead` when their method isn't defined, so a guard
 * that only declares read access also governs writes (you can mutate what you can
 * read) unless it tightens them. The scope is a mingo filter (not SQL), ANDed with
 * the caller's filter and evaluated by the one engine — the same on the realtime
 * path and the server-side authorizer.
 */
export async function applyGuard<R extends Row>(
  guard: RealtimeRuleGuard<any, R> | undefined,
  user: unknown,
  action: GuardAction = 'read',
): Promise<GuardResult> {
  if (!guard) return { allowed: true, scope: {} };
  const decide: (user: any) => GuardDecision =
    action === 'create'
      ? guard.canCreate ?? guard.canRead
      : action === 'update'
        ? guard.canUpdate ?? guard.canRead
        : action === 'delete'
          ? guard.canDelete ?? guard.canRead
          : guard.canRead;
  const result = await decide.call(guard, (user ?? null) as any);
  if (result === false) return { allowed: false, scope: {} };
  if (result === true) return { allowed: true, scope: {} };
  return { allowed: true, scope: result };
}
