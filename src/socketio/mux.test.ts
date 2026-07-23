import { RowDelta } from '../types';
import { attachMux, projectDelta } from './mux';

describe('projectDelta', () => {
  it('passes through a data snapshot unchanged', () => {
    const delta: RowDelta = { kind: 'data', rows: [{ pk: '1', row: { id: 1, name: 'a' } }] };
    expect(projectDelta('s1', delta)).toEqual({
      subId: 's1',
      op: 'data',
      rows: [{ pk: '1', row: { id: 1, name: 'a' } }],
    });
  });

  it('passes through an add delta unchanged', () => {
    const delta: RowDelta = { kind: 'add', pk: '2', row: { id: 2, name: 'b' } };
    expect(projectDelta('s1', delta)).toEqual({
      subId: 's1',
      op: 'add',
      pk: '2',
      row: { id: 2, name: 'b' },
    });
  });

  it('passes through a remove delta unchanged', () => {
    const delta: RowDelta = { kind: 'remove', pk: '3' };
    expect(projectDelta('s1', delta)).toEqual({ subId: 's1', op: 'remove', pk: '3' });
  });

  it('projects an update to only the changed columns when changedColumns is present', () => {
    const delta: RowDelta = {
      kind: 'update',
      pk: '4',
      row: { id: 4, name: 'new-name', status: 'active' },
      changedColumns: ['name'],
    };
    expect(projectDelta('s1', delta)).toEqual({
      subId: 's1',
      op: 'update',
      pk: '4',
      patch: { name: 'new-name' },
    });
  });

  it('projects an update to the full row when changedColumns is undefined', () => {
    const delta: RowDelta = {
      kind: 'update',
      pk: '5',
      row: { id: 5, name: 'x', status: 'active' },
    };
    expect(projectDelta('s1', delta)).toEqual({
      subId: 's1',
      op: 'update',
      pk: '5',
      patch: { id: 5, name: 'x', status: 'active' },
    });
  });

  it('suppresses an update whose changedColumns is present but empty', () => {
    const delta: RowDelta = {
      kind: 'update',
      pk: '6',
      row: { id: 6, name: 'x' },
      changedColumns: [],
    };
    expect(projectDelta('s1', delta)).toBeNull();
  });
});

// ─── attachMux wiring, using fake sockets/engine (no real network) ─────────────

type Listener = (...args: unknown[]) => void;

class FakeSocket {
  handshake = { auth: { token: 'good' } };
  emitted: Array<{ event: string; payload: unknown }> = [];
  private readonly listeners = new Map<string, Listener[]>();
  disconnected = false;

  on(event: string, listener: Listener): void {
    const list = this.listeners.get(event) ?? [];
    list.push(listener);
    this.listeners.set(event, list);
  }

  emit(event: string, payload?: unknown): void {
    this.emitted.push({ event, payload });
  }

  disconnect(_close?: boolean): void {
    this.disconnected = true;
  }

  trigger(event: string, ...args: unknown[]): void {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }
}

class FakeIO {
  private handler?: (socket: FakeSocket) => void;
  on(event: string, handler: (socket: FakeSocket) => void): void {
    if (event === 'connection') this.handler = handler;
  }
  connect(socket: FakeSocket): void {
    this.handler?.(socket);
  }
}

class FakeSubscription {
  closed = false;
  private handler?: (delta: RowDelta) => void;
  on(handler: (delta: RowDelta) => void): void {
    this.handler = handler;
  }
  close(): void {
    this.closed = true;
  }
  push(delta: RowDelta): void {
    this.handler?.(delta);
  }
}

function fakeEngine(subsByModel: Map<string, FakeSubscription>) {
  return {
    openSubscription: jest.fn(async (args: { model: string }) => {
      const sub = subsByModel.get(args.model);
      if (!sub) throw new Error(`unknown model: ${args.model}`);
      return sub;
    }),
  } as unknown as import('../engine/engine').RealtimeEngine;
}

async function flush(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
}

describe('attachMux wiring', () => {
  it('opens a subscription on subscribe and forwards projected deltas', async () => {
    const sub = new FakeSubscription();
    const engine = fakeEngine(new Map([['widgets', sub]]));
    const io = new FakeIO();
    attachMux(io as any, engine, { authenticate: async () => ({ id: 'u1' }) });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    socket.trigger('subscribe', { subId: 'sub-1', model: 'widgets' });
    await flush();

    expect(engine.openSubscription).toHaveBeenCalledWith({
      model: 'widgets',
      user: { id: 'u1' },
      filter: undefined,
      pk: undefined,
    });

    sub.push({ kind: 'add', pk: '1', row: { id: 1 } });
    expect(socket.emitted).toContainEqual({
      event: 'rt',
      payload: { subId: 'sub-1', op: 'add', pk: '1', row: { id: 1 } },
    });
  });

  it('closes the subscription on unsubscribe', async () => {
    const sub = new FakeSubscription();
    const engine = fakeEngine(new Map([['widgets', sub]]));
    const io = new FakeIO();
    attachMux(io as any, engine, { authenticate: async () => ({ id: 'u1' }) });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    socket.trigger('subscribe', { subId: 'sub-1', model: 'widgets' });
    await flush();

    socket.trigger('unsubscribe', { subId: 'sub-1' });
    expect(sub.closed).toBe(true);
  });

  it('closes all subscriptions on disconnect', async () => {
    const subA = new FakeSubscription();
    const subB = new FakeSubscription();
    const engine = fakeEngine(
      new Map([
        ['a', subA],
        ['b', subB],
      ]),
    );
    const io = new FakeIO();
    attachMux(io as any, engine, { authenticate: async () => ({ id: 'u1' }) });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    socket.trigger('subscribe', { subId: 's-a', model: 'a' });
    socket.trigger('subscribe', { subId: 's-b', model: 'b' });
    await flush();

    socket.trigger('disconnect');

    expect(subA.closed).toBe(true);
    expect(subB.closed).toBe(true);
  });

  it('emits an error envelope and does not register a subscription when openSubscription throws', async () => {
    const engine = fakeEngine(new Map());
    const io = new FakeIO();
    attachMux(io as any, engine, { authenticate: async () => ({ id: 'u1' }) });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    socket.trigger('subscribe', { subId: 'sub-1', model: 'nonexistent' });
    await flush();

    expect(socket.emitted).toContainEqual({
      event: 'rt',
      payload: { subId: 'sub-1', op: 'error', message: 'unknown model: nonexistent' },
    });
  });

  it('resolveResource returning null falls through to the plain engine path', async () => {
    const sub = new FakeSubscription();
    const engine = fakeEngine(new Map([['widgets', sub]]));
    const io = new FakeIO();
    attachMux(io as any, engine, {
      authenticate: async () => ({ id: 'u1' }),
      resolveResource: () => null,
    });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    socket.trigger('subscribe', { subId: 'sub-1', model: 'widgets' });
    await flush();

    expect(engine.openSubscription).toHaveBeenCalledWith({
      model: 'widgets',
      user: { id: 'u1' },
      filter: undefined,
      pk: undefined,
    });
  });

  it('disconnects the socket when authenticate rejects', async () => {
    const engine = fakeEngine(new Map());
    const io = new FakeIO();
    attachMux(io as any, engine, {
      authenticate: async () => {
        throw new Error('bad token');
      },
    });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    expect(socket.disconnected).toBe(true);
    expect(socket.emitted).toContainEqual({
      event: 'rt',
      payload: { subId: '', op: 'error', message: 'bad token' },
    });
  });
});

// ─── attachMux wiring — composed resources ─────────────────────────────────

describe('attachMux composed resources', () => {
  function fakeMultiEngine(subsByModel: Map<string, FakeSubscription>) {
    const opened: Array<{ model: string; filter?: unknown; pk?: unknown }> = [];
    const engine = {
      openSubscription: jest.fn(async (args: { model: string; filter?: unknown; pk?: unknown }) => {
        opened.push({ model: args.model, filter: args.filter, pk: args.pk });
        const sub = subsByModel.get(args.model);
        if (!sub) throw new Error(`unknown model: ${args.model}`);
        return sub;
      }),
    } as unknown as import('../engine/engine').RealtimeEngine;
    return { engine, opened };
  }

  it('opens one trigger subscription per resource trigger and emits an initial snapshot', async () => {
    const triggerA = new FakeSubscription();
    const triggerB = new FakeSubscription();
    const { engine, opened } = fakeMultiEngine(
      new Map([
        ['a', triggerA],
        ['b', triggerB],
      ]),
    );
    const io = new FakeIO();
    let loadCalls = 0;
    let seenSpec: unknown;
    attachMux(io as any, engine, {
      authenticate: async () => ({ id: 'u1' }),
      resolveResource: (name, spec) => {
        seenSpec = spec;
        return name === 'joined-feed'
          ? {
              triggers: [{ model: 'a' }, { model: 'b' }],
              load: async () => {
                loadCalls += 1;
                return [{ pk: '1', row: { id: 1 } }];
              },
            }
          : null;
      },
    });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    socket.trigger('subscribe', {
      subId: 'sub-1',
      model: 'joined-feed',
      filter: { status: 'open' },
    });
    await flush();

    expect(seenSpec).toMatchObject({ filter: { status: 'open' } });
    expect(opened).toEqual([
      { model: 'a', filter: undefined, pk: undefined },
      { model: 'b', filter: undefined, pk: undefined },
    ]);
    expect(loadCalls).toBe(1);
    expect(socket.emitted).toContainEqual({
      event: 'rt',
      payload: { subId: 'sub-1', op: 'data', rows: [{ pk: '1', row: { id: 1 } }] },
    });
  });

  it('carries sort/limit/offset/after through to resolveResource as a QuerySpec', async () => {
    const triggerA = new FakeSubscription();
    const { engine } = fakeMultiEngine(new Map([['a', triggerA]]));
    const io = new FakeIO();

    let seenSpec: unknown;
    attachMux(io as any, engine, {
      authenticate: async () => ({ id: 'u1' }),
      resolveResource: (_name, spec) => {
        seenSpec = spec;
        return { triggers: [{ model: 'a' }], load: async () => [] };
      },
    });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    socket.trigger('subscribe', {
      subId: 'sub-1',
      model: 'joined-feed',
      sort: [['createdAt', 'desc']],
      limit: 20,
      offset: 40,
      after: { cursor: 'x' },
    });
    await flush();

    expect(seenSpec).toEqual({
      filter: undefined,
      sort: [['createdAt', 'desc']],
      limit: 20,
      offset: 40,
      after: { cursor: 'x' },
      pk: undefined,
    });
  });

  it('re-runs load() and re-emits when a trigger fires, coalescing overlapping triggers', async () => {
    const triggerA = new FakeSubscription();
    const triggerB = new FakeSubscription();
    const { engine } = fakeMultiEngine(
      new Map([
        ['a', triggerA],
        ['b', triggerB],
      ]),
    );
    const io = new FakeIO();

    let loadCalls = 0;
    let resolveLoad: (() => void) | undefined;
    attachMux(io as any, engine, {
      authenticate: async () => ({ id: 'u1' }),
      resolveResource: () => ({
        triggers: [{ model: 'a' }, { model: 'b' }],
        load: async () => {
          loadCalls += 1;
          if (loadCalls === 2) {
            // Block the second run (triggered below) so a third overlapping trigger
            // firing while it's in flight coalesces into exactly one trailing re-run.
            await new Promise<void>((resolve) => {
              resolveLoad = resolve;
            });
          }
          return [{ pk: String(loadCalls), row: { n: loadCalls } }];
        },
      }),
    });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    socket.trigger('subscribe', { subId: 'sub-1', model: 'joined-feed' });
    await flush();
    expect(loadCalls).toBe(1);

    // Fire trigger A: starts the (blocking) second load.
    triggerA.push({ kind: 'add', pk: '1', row: {} });
    await flush();
    expect(loadCalls).toBe(2);

    // Fire trigger B twice while the second load is still in flight — both collapse
    // into a single pending re-run.
    triggerB.push({ kind: 'add', pk: '2', row: {} });
    triggerB.push({ kind: 'add', pk: '3', row: {} });
    await flush();
    expect(loadCalls).toBe(2); // still in flight, not yet re-run

    resolveLoad?.();
    await flush();
    await flush();

    expect(loadCalls).toBe(3);
    expect(socket.emitted).toContainEqual({
      event: 'rt',
      payload: { subId: 'sub-1', op: 'data', rows: [{ pk: '3', row: { n: 3 } }] },
    });
  });

  it('closes all trigger subscriptions on unsubscribe and on disconnect', async () => {
    const triggerA = new FakeSubscription();
    const triggerB = new FakeSubscription();
    const { engine } = fakeMultiEngine(
      new Map([
        ['a', triggerA],
        ['b', triggerB],
      ]),
    );
    const io = new FakeIO();
    attachMux(io as any, engine, {
      authenticate: async () => ({ id: 'u1' }),
      resolveResource: () => ({
        triggers: [{ model: 'a' }, { model: 'b' }],
        load: async () => [],
      }),
    });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    socket.trigger('subscribe', { subId: 'sub-1', model: 'joined-feed' });
    await flush();

    socket.trigger('unsubscribe', { subId: 'sub-1' });
    expect(triggerA.closed).toBe(true);
    expect(triggerB.closed).toBe(true);

    // Re-subscribe to exercise the disconnect path too.
    triggerA.closed = false;
    triggerB.closed = false;
    socket.trigger('subscribe', { subId: 'sub-2', model: 'joined-feed' });
    await flush();

    socket.trigger('disconnect');
    expect(triggerA.closed).toBe(true);
    expect(triggerB.closed).toBe(true);
  });

  it('emits an error envelope when load() rejects and does not crash the socket', async () => {
    const triggerA = new FakeSubscription();
    const { engine } = fakeMultiEngine(new Map([['a', triggerA]]));
    const io = new FakeIO();
    attachMux(io as any, engine, {
      authenticate: async () => ({ id: 'u1' }),
      resolveResource: () => ({
        triggers: [{ model: 'a' }],
        load: async () => {
          throw new Error('load boom');
        },
      }),
    });

    const socket = new FakeSocket();
    io.connect(socket);
    await flush();

    socket.trigger('subscribe', { subId: 'sub-1', model: 'joined-feed' });
    await flush();

    expect(socket.emitted).toContainEqual({
      event: 'rt',
      payload: { subId: 'sub-1', op: 'error', message: 'load boom' },
    });
  });
});
