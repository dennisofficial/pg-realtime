import { RealtimeResourceRegistry } from './realtime-resource.registry';

describe('RealtimeResourceRegistry', () => {
  it('registers and returns a resource definition', () => {
    const registry = new RealtimeResourceRegistry();
    const def = {
      triggers: () => [{ model: 'jobs' }],
      load: async () => [],
    };

    registry.register('job-feed', def);

    expect(registry.get('job-feed')).toBe(def);
  });

  it('returns null for an unknown resource', () => {
    const registry = new RealtimeResourceRegistry();
    expect(registry.get('missing')).toBeNull();
  });

  it('throws on a duplicate registration', () => {
    const registry = new RealtimeResourceRegistry();
    const def = { triggers: () => [], load: async () => [] };

    registry.register('job-feed', def);

    expect(() => registry.register('job-feed', def)).toThrow(/already registered/);
  });
});
