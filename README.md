# @workspace/pg-realtime

Reusable, **read-only** realtime-over-Postgres engine. It captures row changes via
**logical replication (WAL)** and fans each change out to only the subscriptions
whose **mingo query** matches it — the "smart fan-out" pattern, on Postgres.

Think Supabase's capture mechanism (WAL, no triggers) married to the
Firestore/Meteor "match-on-write" idea, shaped like a Mongo realtime library
(`onQuery` / `onDocument`, `data`/`add`/`update`/`remove`).

```
Postgres WAL ─► ReplicationSource (leader only) ─► ChangeEvent
            ─► PubSubBus ─► every replica ─► per-session mingo matcher ─► RowDelta ─► client
```

## What it gives you

- **Arbitrary filtered live queries** — subscribe with a Mongo-style filter; get the
  initial result set then `add`/`update`/`remove` deltas as rows enter/leave it.
- **Row-level security** via a `RealtimeRuleGuard` whose `canRead` returns a mingo
  scope that is ANDed into the subscription. One engine (mingo) decides membership on
  both the snapshot and the live path, so they can never diverge.
- **Pluggable leadership and fan-out** — defaults need no Redis; swap in the Redis
  adapters for multi-replica deployments.
- **Correct snapshots** — no phantom rows across the snapshot→live window (see below).

## Postgres prerequisites

- `wal_level = logical`, `max_replication_slots >= 1`, `max_wal_senders >= 1`
- A role with the `REPLICATION` attribute
- A **direct** connection (connection poolers like PgBouncer do not speak the
  replication protocol)
- Each watched table needs a primary key (or an explicit `ModelConfig.primaryKey`)

## Quick start

```ts
import { RealtimeEngine } from '@workspace/pg-realtime';

const engine = new RealtimeEngine({
  connectionString: process.env.DATABASE_URL!,
  slotName: 'pg_realtime_slot',
  publicationName: 'pg_realtime_pub',
  models: [
    { table: 'servers', primaryKey: 'id' },
    // large columns clients render -> refetch the full row on a TOAST-incomplete update
    { table: 'blog_posts', primaryKey: 'id', refetchOnUpdate: true },
  ],
});
await engine.start(); // leader ensures the publication + slot, then consumes the WAL

const sub = await engine.openSubscription({
  model: 'servers',
  user: currentUser,
  filter: { status: 'RUNNING' },
});
sub.on((delta) => {
  /* delta.kind: 'data' | 'add' | 'update' | 'remove' */
});
```

### Over socket.io (+ the client)

```ts
// server
import { attachSocketIO } from '@workspace/pg-realtime/socketio';
attachSocketIO(io, engine, { extractUser: (s) => userFromCookie(s) });

// browser
import { PgRealtimeClient } from '@workspace/pg-realtime/client';
const client = new PgRealtimeClient({ baseURL });
const handle = client.onQuery('servers', { status: 'RUNNING' }, (rows) => render(rows));
// const handle = client.onDocument('servers', JSON.stringify([id]), (row) => render(row));
handle.unsubscribe();
```

### NestJS

```ts
import { PgRealtimeModule, PG_REALTIME_ENGINE } from '@workspace/pg-realtime/nest';
@Module({ imports: [PgRealtimeModule.forRoot(engineConfig)] })
export class AppModule {}
// inject @Inject(PG_REALTIME_ENGINE) engine: RealtimeEngine and attachSocketIO in your gateway
```

### Multi-replica (Redis leader + fan-out)

Exactly one process consumes the slot; every replica matches its own sessions.

```ts
import { RedisLeaderElector } from '@workspace/pg-realtime/leader/redis';
import { RedisPubSubBus } from '@workspace/pg-realtime/bus/redis';

new RealtimeEngine({
  /* ...connectionString, slotName, publicationName, models... */
  leader: new RedisLeaderElector({ client: redis }),
  bus: new RedisPubSubBus({ publisher: redis, createSubscriber: () => redis.duplicate().connect() }),
});
```

## The TOAST caveat (`refetchOnUpdate`)

On an UPDATE, Postgres omits unchanged **TOASTed** (large `text`/`jsonb`/…) columns
from the WAL image — **regardless of `REPLICA IDENTITY`**. Predicate columns are
expected to be small (always present), so *matching* is unaffected. But the row
*payload* may be missing a large column's current value. Set `refetchOnUpdate: true`
on such tables: the engine refetches the full row by PK before emitting, but only on
updates that actually dropped a TOAST column. `REPLICA IDENTITY FULL` is **not**
required (the per-session Set + the PK on deletes cover old-state) and does **not**
fix the TOAST hole.

## Snapshot ↔ stream consistency

A subscription buffers live changes from the moment it registers, captures
`pg_current_wal_lsn()` as a **lower bound**, takes its snapshot in a `REPEATABLE READ`
transaction, then replays buffered changes with `lsn > snapshotLsn` (the matcher
dedupes by PK). Capturing the LSN *before* the snapshot is deliberate: a row's commit
WAL record precedes its visibility, so a lower bound makes a dropped row **practically
impossible** (any row not in the snapshot is replayed) — the residual cost is
occasionally replaying an already-present row, which the insert dedup absorbs. A row
deleted in the window is retracted rather than left as a phantom. The only *provably*
zero-window version aligns the snapshot to the slot via `pg_export_snapshot()` (a noted
future hardening). See `src/engine/snapshot.ts`, `src/engine/db.ts`, and the unit +
integration tests (including a concurrent-write snapshot-race test).

## Scaling — honestly

Fan-out is **O(connected queries) per write, per replica** (the same curve as Meteor
and minimongo): every live subscription re-runs its mingo query on every change to its
table. The wins here are **capability** (arbitrary live filtered queries), robustness
(WAL is ordered/resumable; no `LISTEN/NOTIFY` 8 KB cap; no refetch-per-change),
reuse, and pluggable infra — **not** Firestore/Supabase index-backed scaling. The
named future lever is **predicate indexing**: bucket subscriptions by equality key so
a write only fans out to matching buckets.

## Development

```bash
pnpm build            # tsup -> dist (CJS + ESM)
pnpm typecheck        # tsc --noEmit (tsup does not type-check)
pnpm test             # unit suite (no services needed)

docker compose -f docker-compose.test.yml up -d --wait
pnpm test:integration # end-to-end against logical-replication Postgres
docker compose -f docker-compose.test.yml down -v
```

## Status

Engine-only first cut. **Test-covered:** the engine path — change capture, matching,
snapshot consistency (incl. a concurrent-write race test), auth guard, TOAST refetch —
via unit tests + integration tests against logical-replication Postgres. **Shipped but
not yet covered by automated tests:** the socket.io transport, the `PgRealtimeClient`,
and the Redis leader/bus adapters (the engine is exercised directly, not through the
wire). Not yet wired into a consuming app. No write path. No predicate indexing.
