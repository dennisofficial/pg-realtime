import { MingoFilter, RealtimeRuleGuard, Row } from '../types';

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
 * Evaluate a model's read guard for a principal.
 *  - guard returns `false`        -> deny (allowed=false)
 *  - guard returns `true`/absent  -> allow all (scope {})
 *  - guard returns a MingoFilter  -> allow, scoped to that filter
 *
 * The scope is a mingo filter (not SQL); it is ANDed with the client filter and
 * evaluated by the one engine on both the snapshot and the live path.
 */
export async function applyGuard<R extends Row>(
  guard: RealtimeRuleGuard<any, R> | undefined,
  user: unknown,
): Promise<GuardResult> {
  if (!guard) return { allowed: true, scope: {} };
  const result = await guard.canRead((user ?? null) as any);
  if (result === false) return { allowed: false, scope: {} };
  if (result === true) return { allowed: true, scope: {} };
  return { allowed: true, scope: result };
}
