import { getRealtimePublish, Realtime } from './realtime.decorator';

describe('@Realtime', () => {
  it('returns undefined for an undecorated class', () => {
    class Plain {}
    expect(getRealtimePublish(Plain)).toBeUndefined();
  });

  it('defaults to an empty options object', () => {
    @Realtime()
    class Job {}
    expect(getRealtimePublish(Job)).toEqual({});
  });

  it('stores the provided name', () => {
    @Realtime({ name: 'job-links' })
    class JobLink {}
    expect(getRealtimePublish(JobLink)).toEqual({ name: 'job-links' });
  });
});
