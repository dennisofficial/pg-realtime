import { RealtimeClient, FakeableSocket } from './realtime-client';
import type { Envelope } from '../socketio/mux';

type Listener = (...args: any[]) => void;

/**
 * Minimal EventEmitter fake standing in for the socket.io-client `Socket` — no network,
 * no real connect handshake. Tests drive `connect`/`disconnect`/`rt` manually and record
 * every client → server emit for assertions.
 */
class FakeSocket implements FakeableSocket {
  readonly emitted: Array<{ event: string; args: unknown[] }> = [];
  private readonly listeners = new Map<string, Set<Listener>>();

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

  /** Simulate the server pushing an event down to the client. */
  push(event: string, ...args: unknown[]): void {
    for (const listener of [...(this.listeners.get(event) ?? [])]) listener(...args);
  }

  close(): void {}
  disconnect(): void {}
}

function dataEnvelope(subId: string, rows: Array<{ pk: string; row: Record<string, unknown> }>): Envelope {
  return { subId, op: 'data', rows };
}

describe('RealtimeClient', () => {
  it('applies a data snapshot then merges an update patch, leaving other fields intact', () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const q = client.query('threads');

    const seen: Array<Record<string, unknown>[]> = [];
    const unsubscribe = q.subscribe((rows) => seen.push(rows));

    socket.push('connect');
    socket.push(
      'rt',
      dataEnvelope('1', [{ pk: '1', row: { id: '1', text: 'hello', status: 'open' } }]),
    );
    expect(q.get()).toEqual([{ id: '1', text: 'hello', status: 'open' }]);

    socket.push('rt', { subId: '1', op: 'update', pk: '1', patch: { status: 'closed' } });
    expect(q.get()).toEqual([{ id: '1', text: 'hello', status: 'closed' }]);
    expect(seen.length).toBeGreaterThanOrEqual(2);

    unsubscribe();
  });

  it('select(fields) fires only when the patch touches a selected field', () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const q = client.query('threads');
    q.subscribe(() => {}); // keep the sub alive
    socket.push('connect');
    socket.push(
      'rt',
      dataEnvelope('1', [{ pk: '1', row: { id: '1', text: 'hello', status: 'open' } }]),
    );

    const statusListener = jest.fn();
    const textListener = jest.fn();
    const statusView = q.select(['status']);
    const textView = q.select(['text']);
    statusView.subscribe(statusListener);
    textView.subscribe(textListener);

    socket.push('rt', { subId: '1', op: 'update', pk: '1', patch: { status: 'closed' } });
    expect(statusListener).toHaveBeenCalledTimes(1);
    expect(textListener).not.toHaveBeenCalled();

    socket.push('rt', { subId: '1', op: 'update', pk: '1', patch: { text: 'updated' } });
    expect(statusListener).toHaveBeenCalledTimes(1);
    expect(textListener).toHaveBeenCalledTimes(1);
  });

  it('replays the registry on connect and re-emits subscribe for every active sub', () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const q1 = client.query('threads', { filter: { orgId: 'a' } });
    const q2 = client.document('org', 'org-1');

    q1.subscribe(() => {});
    q2.subscribe(() => {});

    // Before the first connect, listeners were added while disconnected — nothing sent yet.
    expect(socket.emitted.filter((e) => e.event === 'subscribe')).toHaveLength(0);

    socket.push('connect');
    const subscribeCalls = socket.emitted.filter((e) => e.event === 'subscribe');
    expect(subscribeCalls).toHaveLength(2);
    expect(subscribeCalls[0].args[0]).toMatchObject({ model: 'threads', filter: { orgId: 'a' } });
    expect(subscribeCalls[1].args[0]).toMatchObject({ model: 'org', pk: 'org-1' });

    // Simulate a reconnect (server state is ephemeral — client must replay again).
    socket.push('disconnect');
    socket.push('connect');
    expect(socket.emitted.filter((e) => e.event === 'subscribe')).toHaveLength(4);
  });

  it('carries sort/limit/offset/after in the subscribe message when provided', () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const q = client.query('threads', {
      filter: { orgId: 'a' },
      sort: [['createdAt', 'desc']],
      limit: 20,
      offset: 40,
      after: { cursor: 'x' },
    });

    q.subscribe(() => {});
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
  });

  it('reconnect re-snapshot reconciles against the cache: derives add/update/remove', () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const q = client.query('threads');

    const changes: string[] = [];
    q.subscribe((rows) => changes.push(JSON.stringify(rows)));

    socket.push('connect');
    socket.push(
      'rt',
      dataEnvelope('1', [
        { pk: '1', row: { id: '1', status: 'open' } },
        { pk: '2', row: { id: '2', status: 'open' } },
      ]),
    );
    expect(q.get()).toHaveLength(2);

    const statusListener = jest.fn();
    q.select(['status']).subscribe(statusListener);

    // Reconnect: server sends a fresh snapshot that dropped row "2", changed row "1"'s
    // status, and added row "3".
    socket.push('disconnect');
    socket.push('connect');
    socket.push(
      'rt',
      dataEnvelope('1', [
        { pk: '1', row: { id: '1', status: 'closed' } },
        { pk: '3', row: { id: '3', status: 'open' } },
      ]),
    );

    const rows = q.get();
    expect(rows).toHaveLength(2);
    expect(rows.find((r) => r.id === '1')).toEqual({ id: '1', status: 'closed' });
    expect(rows.find((r) => r.id === '3')).toEqual({ id: '3', status: 'open' });
    expect(rows.find((r) => r.id === '2')).toBeUndefined();
    // The reconciled row "1" status change should route through the field-selected view.
    expect(statusListener).toHaveBeenCalled();
  });

  it('preserves a Date-valued field through merge and field routing', () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const doc = client.document('threads', '1');

    const createdAt = new Date('2026-07-23T12:00:00.000Z');
    doc.subscribe(() => {});
    socket.push('connect');
    socket.push('rt', dataEnvelope('1', [{ pk: '1', row: { id: '1', createdAt, status: 'open' } }]));
    expect(doc.get()?.createdAt).toBeInstanceOf(Date);
    expect(doc.get()?.createdAt.getTime()).toBe(createdAt.getTime());

    const createdAtListener = jest.fn();
    doc.select(['createdAt']).subscribe(createdAtListener);

    socket.push('rt', { subId: '1', op: 'update', pk: '1', patch: { status: 'closed' } });
    expect(createdAtListener).not.toHaveBeenCalled();
    expect(doc.get()?.createdAt).toBeInstanceOf(Date);
    expect(doc.get()?.createdAt.getTime()).toBe(createdAt.getTime());
  });

  it('onChange delivers the precise add/update/remove stream select() reads internally', () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const q = client.query('threads');

    const seen: string[] = [];
    q.onChange((changes) => {
      for (const change of changes) seen.push(JSON.stringify(change));
    });

    socket.push('connect');
    socket.push(
      'rt',
      dataEnvelope('1', [{ pk: '1', row: { id: '1', text: 'hello', status: 'open' } }]),
    );
    // Initial snapshot delivers as a sequence of `add` changes (same as `select()` sees).
    expect(seen).toEqual([
      JSON.stringify({ op: 'add', pk: '1', row: { id: '1', text: 'hello', status: 'open' } }),
    ]);

    socket.push('rt', { subId: '1', op: 'update', pk: '1', patch: { status: 'closed' } });
    expect(JSON.parse(seen[1])).toEqual({
      op: 'update',
      pk: '1',
      row: { id: '1', text: 'hello', status: 'closed' },
      changedFields: ['status'],
    });

    socket.push('rt', { subId: '1', op: 'remove', pk: '1' });
    expect(JSON.parse(seen[2])).toEqual({ op: 'remove', pk: '1' });
  });

  it('ref-counts listeners: unsubscribe emits unsubscribe only after the last listener drops', () => {
    const socket = new FakeSocket();
    const client = new RealtimeClient({ url: 'unused', socket });
    const q = client.query('threads');

    socket.push('connect');
    const unsubA = q.subscribe(() => {});
    const unsubB = q.select(['status']).subscribe(() => {});

    expect(socket.emitted.filter((e) => e.event === 'subscribe')).toHaveLength(1);

    unsubA();
    expect(socket.emitted.filter((e) => e.event === 'unsubscribe')).toHaveLength(0);

    unsubB();
    expect(socket.emitted.filter((e) => e.event === 'unsubscribe')).toHaveLength(1);
  });
});
