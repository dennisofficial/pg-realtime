import * as http from 'http';
import { AddressInfo } from 'net';
import { Pool } from 'pg';
import { RealtimeEngine } from '../../src/engine/engine';
import { pipeToSse } from '../../src/sse';

/**
 * Real HTTP + SSE end-to-end: an http.Server pipes `engine.openSubscription` deltas
 * through `pipeToSse`, and a `fetch`-based reader parses the event stream — proving
 * the SSE transport carries data/add/update/remove against logical-replication Postgres.
 */
jest.setTimeout(30000);

const DSN =
  process.env.PG_REALTIME_TEST_DSN ??
  'postgresql://postgres:postgres@localhost:5434/pgrealtime_test';

const ID = (n: number) => `00000000-0000-0000-0000-${String(n).padStart(12, '0')}`;

let admin: Pool;
let engine: RealtimeEngine;
let server: http.Server;
let baseURL: string;
const slotName = 'pgrt_sse_slot';
const publicationName = 'pgrt_sse_pub';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function waitFor(pred: () => boolean, label: string, timeoutMs = 10000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error(`timeout: ${label}`);
    await sleep(25);
  }
}

interface SseEvent {
  event: string;
  data: string;
}

/** Minimal fetch-based SSE reader for the test. */
async function openSse(url: string): Promise<{ events: SseEvent[]; close: () => void }> {
  const controller = new AbortController();
  const events: SseEvent[] = [];
  const res = await fetch(url, { signal: controller.signal });
  const reader = (res.body as ReadableStream<Uint8Array>).getReader();
  const decoder = new TextDecoder();
  let buf = '';
  void (async () => {
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let idx: number;
        while ((idx = buf.indexOf('\n\n')) >= 0) {
          const block = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          if (block.startsWith(':') || block === '') continue; // heartbeat / blank
          const ev: Partial<SseEvent> = {};
          for (const line of block.split('\n')) {
            if (line.startsWith('event:')) ev.event = line.slice(6).trim();
            else if (line.startsWith('data:')) ev.data = line.slice(5).trim();
          }
          if (ev.event) events.push(ev as SseEvent);
        }
      }
    } catch {
      /* aborted */
    }
  })();
  return { events, close: () => controller.abort() };
}

beforeAll(async () => {
  admin = new Pool({ connectionString: DSN });
  await admin.query(`
    CREATE TABLE IF NOT EXISTS sse_scratch (
      id uuid PRIMARY KEY,
      tenant text NOT NULL,
      status text NOT NULL,
      label text
    )`);

  engine = new RealtimeEngine({
    connectionString: DSN,
    slotName,
    publicationName,
    models: [{ table: 'sse_scratch', name: 'scratch', primaryKey: 'id' }],
  });
  await engine.start();

  server = http.createServer((req, res) => {
    void (async () => {
      try {
        const u = new URL(req.url ?? '/', 'http://localhost');
        const model = u.searchParams.get('model');
        const filter = u.searchParams.get('filter');
        if (!model) {
          res.writeHead(400).end('missing model');
          return;
        }
        const sub = await engine.openSubscription({
          model,
          filter: filter ? JSON.parse(filter) : undefined,
        });
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache, no-transform',
          Connection: 'keep-alive',
        });
        const cleanup = pipeToSse(sub, res, { heartbeatMs: 0, onClose: () => res.end() });
        res.on('close', cleanup);
      } catch (err) {
        res.writeHead(500).end(String(err));
      }
    })();
  });
  await new Promise<void>((resolve) => server.listen(0, resolve));
  baseURL = `http://127.0.0.1:${(server.address() as AddressInfo).port}/realtime`;
});

afterAll(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()));
  await engine.stop();
  for (let i = 0; i < 25; i++) {
    const { rows } = await admin.query<{ active: boolean }>(
      'SELECT active FROM pg_replication_slots WHERE slot_name=$1',
      [slotName],
    );
    if (rows.length === 0 || !rows[0].active) {
      if (rows.length) await admin.query('SELECT pg_drop_replication_slot($1)', [slotName]);
      break;
    }
    await sleep(150);
  }
  await admin.query(`DROP PUBLICATION IF EXISTS "${publicationName}"`);
  await admin.query('DROP TABLE IF EXISTS sse_scratch');
  await admin.end();
});

beforeEach(async () => {
  await admin.query('TRUNCATE sse_scratch');
});

describe('SSE transport end-to-end', () => {
  it('streams snapshot + add/update/remove over a real HTTP SSE connection', async () => {
    await admin.query(
      `INSERT INTO sse_scratch (id, tenant, status, label) VALUES ($1,'t1','active','r1')`,
      [ID(1)],
    );

    const url = `${baseURL}?model=scratch&filter=${encodeURIComponent(
      JSON.stringify({ tenant: 't1', status: 'active' }),
    )}`;
    const sse = await openSse(url);

    try {
      // initial snapshot
      await waitFor(() => sse.events.some((e) => e.event === 'data'), 'data event');
      const data = JSON.parse(sse.events.find((e) => e.event === 'data')!.data) as Array<{
        row: { label: string };
      }>;
      expect(data.map((d) => d.row.label)).toEqual(['r1']);

      // add
      await admin.query(
        `INSERT INTO sse_scratch (id, tenant, status, label) VALUES ($1,'t1','active','r2')`,
        [ID(2)],
      );
      await waitFor(() => sse.events.some((e) => e.event === 'add'), 'add event');
      const add = JSON.parse(sse.events.find((e) => e.event === 'add')!.data) as {
        row: { label: string };
      };
      expect(add.row.label).toBe('r2');

      // update (stays in set)
      await admin.query(`UPDATE sse_scratch SET label='r2b' WHERE id=$1`, [ID(2)]);
      await waitFor(() => sse.events.some((e) => e.event === 'update'), 'update event');

      // remove (leaves the set)
      await admin.query(`UPDATE sse_scratch SET status='archived' WHERE id=$1`, [ID(2)]);
      await waitFor(() => sse.events.some((e) => e.event === 'remove'), 'remove event');
    } finally {
      sse.close();
    }
  });
});
