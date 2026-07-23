import { RealtimeClient, FakeableSocket } from '../client/realtime-client';
import type { Envelope } from '../socketio/mux';
import { makeSocketDocumentOpener, makeSocketListOpener } from './socket-opener';
import type { SseMessage } from './index';

type Listener = (...args: any[]) => void;

/** Same no-network fake used by `client/realtime-client.test.ts`. */
class FakeSocket implements FakeableSocket {
  private readonly listeners = new Map<string, Set<Listener>>();
  emitted: Array<{ event: string; args: unknown[] }> = [];

  on(event: string, listener: Listener): this {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    set.add(listener);
    return this;
  }

  off(event: string, listener: Listener): this {
    this.listeners.get(event)?.delete(listener);
    return this;
  }

  emit(event: string, ...args: unknown[]): this {
    this.emitted.push({ event, args });
    return this;
  }

  push(event: string, ...args: unknown[]): void {
    for (const listener of [...(this.listeners.get(event) ?? [])]) listener(...args);
  }

  close(): void {}
  disconnect(): void {}
}

function dataEnvelope(subId: string, rows: Array<{ pk: string; row: Record<string, unknown> }>): Envelope {
  return { subId, op: 'data', rows };
}

describe('makeSocketListOpener', () => {
  it('forwards a snapshot as `data`, a live patch as `update` straight off onChange (no array diffing), and unsubscribes on abort', async () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const opener = makeSocketListOpener(client, 'threads');

    const controller = new AbortController();
    const seen: SseMessage[] = [];
    const openPromise = opener('threads', {
      signal: controller.signal,
      onmessage: (ev) => seen.push(ev),
    });

    socket.push('connect');
    socket.push(
      'rt',
      dataEnvelope('1', [{ pk: '1', row: { id: '1', text: 'hello', status: 'open' } }]),
    );

    expect(seen).toHaveLength(1);
    expect(seen[0].event).toBe('data');
    expect(JSON.parse(seen[0].data)).toEqual([
      { pk: '1', row: { id: '1', text: 'hello', status: 'open' } },
    ]);

    // A live patch update forwards directly as `update` off the `Collection`'s own
    // changed-field computation — no re-snapshot, no array diffing. Unrelated fields
    // (`text`) survive via the merged row the change event already carries.
    socket.push('rt', { subId: '1', op: 'update', pk: '1', patch: { status: 'closed' } });

    expect(seen).toHaveLength(2);
    expect(seen[1].event).toBe('update');
    expect(JSON.parse(seen[1].data)).toEqual({
      pk: '1',
      row: { id: '1', text: 'hello', status: 'closed' },
    });

    // A row leaving the result set forwards as `remove`.
    socket.push(
      'rt',
      dataEnvelope('1', []), // reconnect-style re-snapshot with the row gone
    );

    expect(seen).toHaveLength(3);
    expect(seen[2].event).toBe('remove');
    expect(JSON.parse(seen[2].data)).toEqual({ pk: '1' });

    controller.abort();
    await openPromise;

    // Teardown unsubscribed: further pushes are not observed.
    socket.push('rt', dataEnvelope('1', [{ pk: '1', row: { id: '1', status: 'open' } }]));
    expect(seen).toHaveLength(3);
  });

  it('forwards sort/limit/offset/after to client.query in the subscribe message', () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const controller = new AbortController();
    const opener = makeSocketListOpener(client, 'threads', {
      filter: { orgId: 'a' },
      sort: [['createdAt', 'desc']],
      limit: 20,
      offset: 40,
      after: { cursor: 'x' },
    });
    void opener('threads', { signal: controller.signal, onmessage: () => {} });

    socket.push('connect');

    const subscribeCalls = socket.emitted.filter((e) => e.event === 'subscribe');
    expect(subscribeCalls).toHaveLength(1);
    expect(subscribeCalls[0].args[0]).toMatchObject({
      model: 'threads',
      filter: { orgId: 'a' },
      sort: [['createdAt', 'desc']],
      limit: 20,
      offset: 40,
      after: { cursor: 'x' },
    });

    controller.abort();
  });

  it('reports an `add` for a newly-appearing row', async () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const opener = makeSocketListOpener(client, 'threads');

    const controller = new AbortController();
    const seen: SseMessage[] = [];
    void opener('threads', { signal: controller.signal, onmessage: (ev) => seen.push(ev) });

    socket.push('connect');
    socket.push('rt', dataEnvelope('1', [{ pk: '1', row: { id: '1' } }]));
    socket.push('rt', dataEnvelope('1', [{ pk: '1', row: { id: '1' } }, { pk: '2', row: { id: '2' } }]));

    expect(seen.map((m) => m.event)).toEqual(['data', 'add']);
    expect(JSON.parse(seen[1].data)).toEqual({ pk: '2', row: { id: '2' } });

    controller.abort();
  });
});

describe('makeSocketDocumentOpener', () => {
  it('forwards an initial row as `add`, a patch as `update`, and a null as `remove`', async () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const opener = makeSocketDocumentOpener(client, 'threads', '1');

    const controller = new AbortController();
    const seen: SseMessage[] = [];
    const openPromise = opener('threads/1', {
      signal: controller.signal,
      onmessage: (ev) => seen.push(ev),
    });

    socket.push('connect');
    socket.push('rt', dataEnvelope('1', [{ pk: '1', row: { id: '1', status: 'open' } }]));

    expect(seen).toHaveLength(1);
    expect(seen[0].event).toBe('add');
    expect(JSON.parse(seen[0].data)).toEqual({ pk: '1', row: { id: '1', status: 'open' } });

    socket.push('rt', { subId: '1', op: 'update', pk: '1', patch: { status: 'closed' } });

    expect(seen).toHaveLength(2);
    expect(seen[1].event).toBe('update');
    expect(JSON.parse(seen[1].data)).toEqual({ pk: '1', row: { id: '1', status: 'closed' } });

    socket.push('rt', dataEnvelope('1', []));

    expect(seen).toHaveLength(3);
    expect(seen[2].event).toBe('remove');
    expect(JSON.parse(seen[2].data)).toEqual({ pk: '1' });

    controller.abort();
    await openPromise;
  });
});
