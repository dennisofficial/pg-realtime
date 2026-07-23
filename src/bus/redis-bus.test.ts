import superjson from 'superjson';
import { ChangeEvent } from '../types';

type MessageHandler = (channel: string, payload: string) => void;

class FakeRedis {
  publish = jest.fn(async (_channel: string, _payload: string) => 1);
  subscribe = jest.fn(async (_channel: string) => 1);
  quit = jest.fn(async () => 'OK');
  on = jest.fn((event: string, handler: MessageHandler | ((err: unknown) => void)) => {
    if (event === 'message') this.messageHandler = handler as MessageHandler;
  });
  messageHandler?: MessageHandler;
}

const instances: FakeRedis[] = [];

jest.mock('ioredis', () => {
  const ctor = jest.fn().mockImplementation(() => {
    const instance = new FakeRedis();
    instances.push(instance);
    return instance;
  });
  return {
    __esModule: true,
    default: ctor,
    Redis: ctor,
  };
});

import { RedisBus } from './redis-bus';

describe('RedisBus', () => {
  beforeEach(() => {
    instances.length = 0;
  });

  function makeEvent(): ChangeEvent {
    return {
      op: 'insert',
      schema: 'public',
      table: 'widgets',
      pk: '1',
      row: { id: 1, createdAt: new Date('2024-01-01T00:00:00Z') },
      oldRow: null,
      toastIncomplete: false,
      lsn: '0/0',
    };
  }

  it('publish() sends a superjson-serialized payload on the (publisher) connection', async () => {
    const bus = new RedisBus({ url: 'redis://localhost:6379' });
    const event = makeEvent();

    await bus.publish('pg_realtime:changes', event);

    expect(instances).toHaveLength(2); // publisher + subscriber
    const publisher = instances[0];
    expect(publisher.publish).toHaveBeenCalledTimes(1);
    const [wireChannel, payload] = publisher.publish.mock.calls[0];
    expect(wireChannel).toBe('pg_realtime_events:pg_realtime:changes');
    expect(payload).toBe(superjson.stringify(event));
  });

  it('subscribe() deserializes an incoming message via superjson and preserves Date fields', async () => {
    const bus = new RedisBus({ url: 'redis://localhost:6379' });
    const event = makeEvent();
    const received: ChangeEvent[] = [];

    await bus.subscribe('pg_realtime:changes', (e) => received.push(e));

    const subscriber = instances[1];
    expect(subscriber.subscribe).toHaveBeenCalledWith('pg_realtime_events:pg_realtime:changes');
    expect(subscriber.messageHandler).toBeDefined();

    subscriber.messageHandler!('pg_realtime_events:pg_realtime:changes', superjson.stringify(event));

    expect(received).toHaveLength(1);
    expect(received[0].row!.createdAt).toBeInstanceOf(Date);
    expect((received[0].row!.createdAt as Date).toISOString()).toBe('2024-01-01T00:00:00.000Z');
  });

  it('supports injected publisher/subscriber instances instead of a url', async () => {
    const publisher = new FakeRedis();
    const subscriber = new FakeRedis();
    const bus = new RedisBus({ publisher: publisher as any, subscriber: subscriber as any });

    await bus.publish('chan', makeEvent());

    expect(instances).toHaveLength(0); // no new connections created
    expect(publisher.publish).toHaveBeenCalledTimes(1);
  });
});
