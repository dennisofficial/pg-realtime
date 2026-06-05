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
  both the snapshot and the live path, so they can never diverge — and the *same*
  guards authorize server-side reads/mutations via `RealtimeRls` (one source of truth).
- **Pure Postgres, single stack** — leadership (advisory lock) and fan-out
  (LISTEN/NOTIFY) use Postgres itself; no Redis, no extra infrastructure. A single
  process needs neither.
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

### Multi-replica

Exactly one process consumes the slot; every replica matches its own sessions. Two
things are coordinated across replicas — slot ownership and change fan-out — and both
use **Postgres itself**, no extra stack.

If the WAL consumer runs in a guaranteed single instance, you need *neither*: the
default `NoopLeaderElector` + `InProcessBus` already cover a one-process deployment.

```ts
import { PgAdvisoryLockLeaderElector, PgNotifyBus } from '@workspace/pg-realtime';

new RealtimeEngine({
  /* ...connectionString, slotName, publicationName, models... */
  // Leader election via a session advisory lock (one connection; auto-fails-over when
  // the holder disconnects). Only needed if the consumer itself runs in >1 replica.
  leader: new PgAdvisoryLockLeaderElector({ connectionString: directUrl, lockName: 'pg_realtime' }),
  // Fan-out via LISTEN/NOTIFY. Caveat: a single change's row image must fit Postgres's
  // 8 KB NOTIFY payload cap (fine for small rows; oversized ones are logged + dropped).
  bus: new PgNotifyBus({ connectionString: directUrl }),
});
```

The `LeaderElector` and `PubSubBus` interfaces are public, so a different backend (e.g.
Redis) can be supplied per project — but the package ships only the Postgres-native
implementations.

## Authorization — one source of truth

A `RealtimeRuleGuard` returns a **mingo filter** (not a yes/no) describing what a user
may access. That single definition drives the realtime subscription, a server-side
list, and a server-side row check — they can't drift apart.

```ts
class ServersGuard extends RealtimeRuleGuard<AuthUser> {
  canRead(user: AuthUser | null) {
    return user ? { customer_id: user.id } : false; // deny when unauthenticated
  }
  // canCreate/canUpdate/canDelete are optional — each falls back to canRead.
  // Each takes only the user and returns a filter; read/update/delete test it against
  // the existing row, create against the candidate row.
}

models: [{ table: 'servers', primaryKey: 'id', guard: new ServersGuard() }];
```

**Server-side checks** use `RealtimeRls`, built from the *same* models config. It's
standalone — no running engine — so the process that handles a mutation (e.g. your
panel backend) can authorize against the same guards the consumer uses:

```ts
import { RealtimeRls } from '@workspace/pg-realtime';

const rls = new RealtimeRls({ models, connectionString });

// power-on a server: fetch + authorize in one call
const server = await rls.get({ model: 'servers', user, pk: [serverId] });
if (!server) throw new ForbiddenException(); // not found OR not permitted (same outcome)
// ...proceed to power it on

// or check a row you already fetched (pure, no DB):
if (!(await rls.authorize({ model: 'servers', user, row: server }))) throw new ForbiddenException();

// or fetch the authorized set — identical to what a subscription's snapshot returns:
const myServers = await rls.query({ model: 'servers', user, filter: { status: 'RUNNING' } });
```

`scope` / `authorize` / `filterRows` are pure (no database); `query` / `get` need a
pool. Inside the engine's process, `engine.rls` is the same authorizer pre-wired to the
engine's pool.

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
the `RealtimeRls` server-side authorizer (pure checks + DB-backed `query`/`get`), and
the Postgres-native adapters (advisory-lock election, NOTIFY round-trip, and a
two-engine "one consumer fans out to every replica" end-to-end), via unit + integration
tests against logical-replication Postgres. **Shipped but not yet covered by automated
tests:** the socket.io transport and the `PgRealtimeClient`. Not yet wired into a
consuming app. No write path. No predicate indexing.
