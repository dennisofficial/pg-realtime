import { RowDelta, Subscription } from '../types';
import { formatSse, pipeToSse, ssePayload } from './index';

describe('formatSse / ssePayload', () => {
  it('maps each delta kind to an SSE event with the right payload', () => {
    const data: RowDelta = { kind: 'data', rows: [{ pk: '["1"]', row: { id: 1 } }] };
    expect(ssePayload(data)).toEqual([{ pk: '["1"]', row: { id: 1 } }]);
    expect(formatSse(data)).toBe(
      `event: data\ndata: ${JSON.stringify([{ pk: '["1"]', row: { id: 1 } }])}\n\n`,
    );

    const add: RowDelta = { kind: 'add', pk: '["2"]', row: { id: 2 } };
    expect(ssePayload(add)).toEqual({ pk: '["2"]', row: { id: 2 } });
    expect(formatSse(add)).toBe(`event: add\ndata: {"pk":"[\\"2\\"]","row":{"id":2}}\n\n`);

    const remove: RowDelta = { kind: 'remove', pk: '["3"]' };
    expect(ssePayload(remove)).toEqual({ pk: '["3"]' });
    expect(formatSse(remove)).toBe(`event: remove\ndata: {"pk":"[\\"3\\"]"}\n\n`);
  });

  it('keeps each delta on a single data line (JSON escapes newlines)', () => {
    const add: RowDelta = { kind: 'add', pk: 'x', row: { note: 'line1\nline2' } };
    const out = formatSse(add);
    // exactly two newlines from framing + the terminator; none injected by the payload
    expect(out.split('\n').filter((l) => l.startsWith('data:')).length).toBe(1);
    expect(out.endsWith('\n\n')).toBe(true);
  });
});

// Minimal fake subscription to drive pipeToSse without an engine.
function fakeSub() {
  let handler: ((d: RowDelta) => void) | undefined;
  let closed = false;
  const sub: Subscription = {
    id: 'fake',
    on: (h) => {
      handler = h;
    },
    close: () => {
      closed = true;
    },
  };
  return {
    sub,
    emit: (d: RowDelta) => handler?.(d),
    get closed() {
      return closed;
    },
  };
}

describe('pipeToSse', () => {
  it('writes formatted deltas to the sink and closes the sub on cleanup', () => {
    const f = fakeSub();
    const writes: string[] = [];
    const cleanup = pipeToSse(f.sub, { write: (c) => writes.push(c) }, { heartbeatMs: 0 });

    f.emit({ kind: 'data', rows: [] });
    f.emit({ kind: 'add', pk: 'p1', row: { id: 1 } });
    expect(writes).toEqual([
      formatSse({ kind: 'data', rows: [] }),
      formatSse({ kind: 'add', pk: 'p1', row: { id: 1 } }),
    ]);

    cleanup();
    expect(f.closed).toBe(true);
    f.emit({ kind: 'add', pk: 'p2', row: {} }); // after cleanup -> ignored
    expect(writes).toHaveLength(2);
  });

  it('cleans up (and closes the sub) when a write throws', () => {
    const f = fakeSub();
    let calls = 0;
    const cleanup = pipeToSse(
      f.sub,
      {
        write: () => {
          calls += 1;
          throw new Error('client gone');
        },
      },
      { heartbeatMs: 0 },
    );
    f.emit({ kind: 'data', rows: [] });
    expect(calls).toBe(1);
    expect(f.closed).toBe(true);
    cleanup(); // idempotent
  });

  it('emits heartbeat comments on the interval', () => {
    jest.useFakeTimers();
    try {
      const f = fakeSub();
      const writes: string[] = [];
      const cleanup = pipeToSse(f.sub, { write: (c) => writes.push(c) }, { heartbeatMs: 1000 });
      jest.advanceTimersByTime(2500);
      expect(writes.filter((w) => w === ': ping\n\n')).toHaveLength(2);
      cleanup();
      jest.advanceTimersByTime(2000);
      expect(writes.filter((w) => w === ': ping\n\n')).toHaveLength(2); // stopped
    } finally {
      jest.useRealTimers();
    }
  });
});
