import type { ComposedResource, Principal, QuerySpec } from '../socketio/mux';
import type { Row } from '../types';
import type { RealtimeResourceRegistry } from './realtime-resource.registry';

/** Plugs `resolveResource` needs, kept separate from the full `RealtimeNestConfig` so the
 * dispatcher stays a pure function testable without a Nest injector. */
export interface ResolveResourceDeps<P> {
  registry: RealtimeResourceRegistry;
  /** Mode B windowed-query loader (RLS-scoped ORDER BY/LIMIT). Optional — omit to disable Mode B. */
  scopedFind?: (
    model: string,
    spec: QuerySpec,
    principal: P,
  ) => Promise<Array<{ pk: string; row: Row }>>;
  withPrincipalContext: <T>(principal: P, fn: () => Promise<T> | T) => Promise<T>;
}

/**
 * The Mode A/B/composed dispatcher handed to `attachMux` as `resolveResource`. Extracted as a
 * pure function (no Nest, no socket) so it's directly unit-testable.
 *
 * 1. A name registered on the `RealtimeResourceRegistry` → a composed resource: triggers +
 *    `load()` off the registered definition, `load` wrapped in the host's principal context.
 * 2. No named resource, but the query carries `sort`/`limit`/`offset`/`after` and a
 *    `scopedFind` plug was supplied → a windowed entity (Mode B): one trigger on `name` itself,
 *    `load` delegates to `scopedFind`.
 * 3. Otherwise → `null`, falling through to the engine's plain streaming subscription (Mode A).
 */
export function resolveResource<P extends Principal>(
  name: string,
  spec: QuerySpec,
  principal: P,
  deps: ResolveResourceDeps<P>,
): ComposedResource | null {
  const def = deps.registry.get<P>(name);
  if (def) {
    const params = spec.filter ?? {};
    return {
      triggers: def.triggers(params),
      load: () => deps.withPrincipalContext(principal, () => def.load(params, principal)),
    };
  }

  const isWindowed = spec.sort !== undefined || spec.limit != null || spec.offset != null || spec.after !== undefined;
  if (isWindowed && deps.scopedFind) {
    const scopedFind = deps.scopedFind;
    return {
      // KNOWN LIMITATION: this trigger opens a full streaming subscription on `name`
      // (snapshotting the filtered set once) purely as a change signal to re-run `load` —
      // fine for now, a lighter "signal-only" trigger is a later scale optimization.
      triggers: [{ model: name, filter: spec.filter }],
      load: () => deps.withPrincipalContext(principal, () => scopedFind(name, spec, principal)),
    };
  }

  return null;
}
