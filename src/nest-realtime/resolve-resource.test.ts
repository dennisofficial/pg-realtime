import type { QuerySpec } from '../socketio/mux';
import { resolveResource } from './resolve-resource';
import { RealtimeResourceRegistry } from './realtime-resource.registry';

interface Principal {
  userId: string;
}

describe('resolveResource', () => {
  it('resolves a named resource off the registry, wrapping load in the principal context', async () => {
    const registry = new RealtimeResourceRegistry();
    const load = jest.fn().mockResolvedValue([{ pk: '1', row: { id: '1' } }]);
    const triggers = jest.fn().mockReturnValue([{ model: 'jobs', filter: { orgId: 'org1' } }]);
    registry.register<Principal>('job-feed', { triggers, load });

    const withPrincipalContext = jest.fn((principal, fn) => fn());
    const principal: Principal = { userId: 'u1' };
    const spec: QuerySpec = { filter: { orgId: 'org1' } };

    const resource = resolveResource('job-feed', spec, principal, {
      registry,
      withPrincipalContext,
    });

    expect(resource).not.toBeNull();
    expect(resource!.triggers).toEqual([{ model: 'jobs', filter: { orgId: 'org1' } }]);
    expect(triggers).toHaveBeenCalledWith({ orgId: 'org1' });

    const rows = await resource!.load();
    expect(rows).toEqual([{ pk: '1', row: { id: '1' } }]);
    expect(withPrincipalContext).toHaveBeenCalledWith(principal, expect.any(Function));
    expect(load).toHaveBeenCalledWith({ orgId: 'org1' }, principal);
  });

  it('resolves a windowed entity through scopedFind when sort/limit/offset/after is present and no named resource exists', async () => {
    const registry = new RealtimeResourceRegistry();
    const scopedFind = jest.fn().mockResolvedValue([{ pk: '2', row: { id: '2' } }]);
    const withPrincipalContext = jest.fn((principal, fn) => fn());
    const principal: Principal = { userId: 'u1' };
    const spec: QuerySpec = { filter: { orgId: 'org1' }, sort: [['createdAt', 'desc']], limit: 20 };

    const resource = resolveResource('jobs', spec, principal, {
      registry,
      scopedFind,
      withPrincipalContext,
    });

    expect(resource).not.toBeNull();
    expect(resource!.triggers).toEqual([{ model: 'jobs', filter: { orgId: 'org1' } }]);

    const rows = await resource!.load();
    expect(rows).toEqual([{ pk: '2', row: { id: '2' } }]);
    expect(withPrincipalContext).toHaveBeenCalledWith(principal, expect.any(Function));
    expect(scopedFind).toHaveBeenCalledWith('jobs', spec, principal);
  });

  it('does not resolve a windowed entity when scopedFind is not supplied', () => {
    const registry = new RealtimeResourceRegistry();
    const withPrincipalContext = jest.fn((principal, fn) => fn());
    const spec: QuerySpec = { limit: 20 };

    const resource = resolveResource('jobs', spec, { userId: 'u1' }, {
      registry,
      withPrincipalContext,
    });

    expect(resource).toBeNull();
  });

  it('falls through to plain streaming (returns null) when the query is unnamed and unwindowed', () => {
    const registry = new RealtimeResourceRegistry();
    const scopedFind = jest.fn();
    const withPrincipalContext = jest.fn((principal, fn) => fn());
    const spec: QuerySpec = { filter: { orgId: 'org1' } };

    const resource = resolveResource('jobs', spec, { userId: 'u1' }, {
      registry,
      scopedFind,
      withPrincipalContext,
    });

    expect(resource).toBeNull();
    expect(scopedFind).not.toHaveBeenCalled();
  });

  it('treats offset and after as windowing signals even without sort/limit', () => {
    const registry = new RealtimeResourceRegistry();
    const scopedFind = jest.fn().mockResolvedValue([]);
    const withPrincipalContext = jest.fn((principal, fn) => fn());

    const byOffset = resolveResource('jobs', { offset: 10 }, { userId: 'u1' }, {
      registry,
      scopedFind,
      withPrincipalContext,
    });
    const byAfter = resolveResource('jobs', { after: 'cursor' }, { userId: 'u1' }, {
      registry,
      scopedFind,
      withPrincipalContext,
    });

    expect(byOffset).not.toBeNull();
    expect(byAfter).not.toBeNull();
  });
});
