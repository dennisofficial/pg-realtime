# @workspace/pg-realtime

Reusable, **read-only** realtime-over-Postgres engine. It captures row changes via
**logical replication (WAL)** and fans each change out to only the subscriptions whose
**mingo query** matches it — the "smart fan-out" pattern, on Postgres, with **no extra
infrastructure** (leadership and fan-out use Postgres itself).

Think Supabase's capture mechanism (WAL, no triggers) married to the Firestore/Meteor
"match-on-write" idea, shaped like a Mongo realtime library (`onQuery` / `onDocument`,
`data`/`add`/`update`/`remove`).

```
Postgres WAL ─► ReplicationSource (leader only) ─► ChangeEvent
            ─► PubSubBus ─► every replica ─► per-session mingo matcher ─► RowDelta ─► client
```

## Contents

- [What it gives you](#what-it-gives-you)
- [Packages & exports](#packages--exports)
- [Postgres prerequisites](#postgres-prerequisites)
- [Quick start](#quick-start)
- [Subscriptions & deltas](#subscriptions--deltas)
- [Transports](#transports) — [socket.io](#socketio) · [SSE](#sse) · [NestJS](#nestjs-module)
- [Authorization (one source of truth)](#authorization--one-source-of-truth) — [guards](#guards) · [server-side `RealtimeRls`](#server-side-checks-realtimerls) · [TypeORM](#enforcing-rls-in-your-orm-typeorm)
- [Multi-replica (leader + bus)](#multi-replica)
- [Configuration reference](#configuration-reference)
- [How it works](#how-it-works)
- [Snapshot ↔ stream consistency](#snapshot--stream-consistency)
- [The TOAST caveat](#the-toast-caveat-refetchonupdate)
- [Scaling, honestly](#scaling-honestly)
- [Development](#development)
- [Status](#status)

## What it gives you

- **Arbitrary filtered live queries** — subscribe with a Mongo-style filter; get the
  initial result set, then `add`/`update`/`remove` deltas as rows enter/leave it.
- **Row-level security from one definition** — a `RealtimeRuleGuard` returns a mingo
  filter that scopes the live subscription, the server-side authorizer (`RealtimeRls`),
  *and* (via the TypeORM helper) your own ORM fetches. One source of truth; they can't
  drift apart.
- **Pure Postgres, single stack** — leadership (advisory lock) and fan-out
  (LISTEN/NOTIFY) use Postgres itself; no Redis, no extra service. A single process
  needs neither.
- **Correct snapshots** — no phantom rows across the snapshot→live window.
- **Transport-agnostic** — the engine emits deltas; ship them over WebSockets
  (socket.io) or SSE.

## Packages & exports

The core needs only `pg`, `pg-logical-replication`, and `mingo`. Each adapter lives
behind a subpath and pulls its own **optional peer** (install it only if you use that
subpath).

| Import | Optional peer | Provides |
|---|---|---|
| `@workspace/pg-realtime` | — | `RealtimeEngine`, `RealtimeRls`, `RealtimeRuleGuard`, types, `NoopLeaderElector`, `InProcessBus`, `PgAdvisoryLockLeaderElector`, `PgNotifyBus` |
| `@workspace/pg-realtime/socketio` | `socket.io` | `attachSocketIO(io, engine)` — server binding |
| `@workspace/pg-realtime/client` | `socket.io-client` | `PgRealtimeClient` — browser client (`onQuery`/`onDocument`) |
| `@workspace/pg-realtime/sse` | — | `pipeToSse`, `formatSse` — framework-agnostic SSE server |
| `@workspace/pg-realtime/sse/client` | `@microsoft/fetch-event-source` | `SseClient` — browser SSE client |
| `@workspace/pg-realtime/nest` | `@nestjs/common`, `rxjs` | `PgRealtimeModule`, `sseObservable` (for `@Sse()`) |
| `@workspace/pg-realtime/typeorm` | `typeorm` | `toFindOptionsWhere`, `scopedFindWhere` — RLS as a TypeORM `where` |

## Postgres prerequisites

- `wal_level = logical`, `max_replication_slots >= 1`, `max_wal_senders >= 1`
- A role with the `REPLICATION` attribute
- A **direct** connection — connection poolers (PgBouncer) don't speak the replication
  protocol
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
// sub.close() when done
await engine.stop(); // on shutdown
```

## Subscriptions & deltas

`engine.openSubscription(args)` returns a `Subscription`. The snapshot begins when you
attach the first handler with `.on()`, so the **first delta is always `data`**, followed
by incremental deltas.

`openSubscription` args:

| field | type | notes |
|---|---|---|
| `model` | `string` | model `name` (or table) to subscribe to |
| `user` | `unknown` | the principal, passed to the guard |
| `filter` | mingo filter | ANDed with the guard's scope |
| `pk` | `string` | **document mode**: subscribe to a single row by its stringified PK |

**Query mode** (`filter`) tracks a result set; **document mode** (`pk`) tracks one row.
A `pk` is the stringified primary key — `JSON.stringify([id])` for a single-column key,
`JSON.stringify([a, b])` for a composite one.

The delta vocabulary (`RowDelta`), identical across socket.io and SSE:

| `kind` | payload | meaning |
|---|---|---|
| `data` | `{ rows: Array<{ pk, row }> }` | initial snapshot (always first) |
| `add` | `{ pk, row }` | a row entered the result set |
| `update` | `{ pk, row }` | a row in the set changed |
| `remove` | `{ pk }` | a row left the set (updated-out or deleted) |

Clients keep a `Map<pk, row>` and reconcile: `data` fills it, `add`/`update` set, `remove`
deletes. The bundled clients do this for you.

## Transports

The engine is transport-agnostic. Pick WebSockets or SSE — both carry the same deltas.

### socket.io

```ts
// server
import { attachSocketIO } from '@workspace/pg-realtime/socketio';
attachSocketIO(io, engine, { extractUser: (socket) => userFromCookie(socket) });
// one socket = one subscription; spec rides in the connection handshake auth:
//   io(url, { auth: { model: 'servers', filter: { status: 'RUNNING' } } })

// browser
import { PgRealtimeClient } from '@workspace/pg-realtime/client';
const client = new PgRealtimeClient({ baseURL });
const handle = client.onQuery('servers', { status: 'RUNNING' }, (rows) => render(rows));
// client.onDocument('servers', JSON.stringify([id]), (row) => render(row));
handle.unsubscribe();
```

### SSE

Read-only server→client stream — the natural fit for a subscription model, and a drop-in
for an existing `EventSource`/RTK-Query frontend. The spec rides in query params; the
user comes from your request auth.

```ts
// server — NestJS @Sse() (each delta becomes a MessageEvent whose name is the delta kind)
import { sseObservable } from '@workspace/pg-realtime/nest';

@Sse('servers/realtime')
stream(@GetUser() user: AuthUser, @Query('filter') filter?: string): Observable<MessageEvent> {
  return sseObservable(() =>
    engine.openSubscription({ model: 'servers', user, filter: filter ? JSON.parse(filter) : undefined }),
  );
}

// server — framework-agnostic (Express / Fastify / raw http / Next.js route handler)
import { pipeToSse } from '@workspace/pg-realtime/sse';
res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform' });
const cleanup = pipeToSse(sub, res); // attaching starts the snapshot; sends heartbeats
res.on('close', cleanup);            // close the subscription on disconnect
```

```ts
// browser — built on @microsoft/fetch-event-source: auth headers, openWhenHidden,
// exponential-backoff reconnect, AbortController cancel (production posture, not raw EventSource)
import { SseClient } from '@workspace/pg-realtime/sse/client';
const client = new SseClient({
  baseURL: '/servers/realtime',
  headers: async () => ({ Authorization: `Bearer ${await token()}` }), // refreshed per (re)connect
});
const handle = client.onQuery('servers', { status: 'RUNNING' }, (rows) => render(rows));
handle.unsubscribe();
```

### NestJS module

```ts
import { PgRealtimeModule, PG_REALTIME_ENGINE } from '@workspace/pg-realtime/nest';

@Module({ imports: [PgRealtimeModule.forRoot(engineConfig)] }) // or forRootAsync({ useFactory, inject })
export class AppModule {}
// inject @Inject(PG_REALTIME_ENGINE) engine: RealtimeEngine; serve via attachSocketIO or @Sse()+sseObservable.
// The module manages engine.start()/stop() on the app lifecycle.
```

## Authorization — one source of truth

### Guards

A `RealtimeRuleGuard` returns a **mingo filter** (not a yes/no) describing what a user
may access. That one definition drives the subscription, the server-side authorizer, and
your ORM fetches.

```ts
class ServersGuard extends RealtimeRuleGuard<AuthUser> {
  canRead(user: AuthUser | null) {
    return user ? { customer_id: user.id } : false; // false denies; true allows all; a filter scopes
  }
  // canCreate / canUpdate / canDelete are optional — each falls back to canRead.
  // Every method takes only the user and returns a filter; read/update/delete test it
  // against the existing row, create against the candidate row.
}

models: [{ table: 'servers', primaryKey: 'id', guard: new ServersGuard() }];
```

The guard's filter is ANDed into every subscription, so a customer subscribing to
`{ status: 'RUNNING' }` effectively gets `{ status: 'RUNNING', customer_id: <them> }` —
enforced server-side, on the snapshot and the live stream both.

### Server-side checks (`RealtimeRls`)

`RealtimeRls` is built from the *same* models config and is **standalone** — no running
engine — so the process that handles a mutation (e.g. your panel backend) authorizes
against the same guards the consumer uses.

```ts
import { RealtimeRls } from '@workspace/pg-realtime';

const rls = new RealtimeRls({ models, connectionString });

// power-on a server: fetch + authorize in one call
const server = await rls.get({ model: 'servers', user, pk: [serverId] });
if (!server) throw new ForbiddenException(); // not found OR not permitted (same outcome)

// check a row you already fetched (pure, no DB):
if (!(await rls.authorize({ model: 'servers', user, row: server }))) throw new ForbiddenException();

// fetch the authorized set — identical to what a subscription's snapshot returns:
const myServers = await rls.query({ model: 'servers', user, filter: { status: 'RUNNING' } });
```

| method | DB? | returns |
|---|---|---|
| `scope({ model, user, action?, filter? })` | no | `{ allowed, filter }` — the effective mingo filter |
| `authorize({ model, user, row, action? })` | no | `boolean` for an already-fetched row |
| `filterRows({ model, user, rows, action?, filter? })` | no | the subset the user may access |
| `query({ model, user, filter?, pk? })` | yes | authorized rows (raw `pg`, not your ORM) |
| `get({ model, user, pk, action? })` | yes | the row, or `null` if missing **or** unauthorized |

`action` is `'read' \| 'create' \| 'update' \| 'delete'` (default `'read'`). Inside the
engine's process, `engine.rls` is the same authorizer pre-wired to the engine's pool.

### Enforcing RLS in your ORM (TypeORM)

When you fetch with your own ORM, apply the scope **at the database** — filtering after
the fetch wastes the query and corrupts pagination (your total/`skip`/`take` would be
computed on rows the user can't see). `@workspace/pg-realtime/typeorm` translates the
guard's mingo scope into a `FindOptionsWhere`:

```ts
import { scopedFindWhere, toFindOptionsWhere } from '@workspace/pg-realtime/typeorm';

// guard scope ∧ your conditions, as a TypeORM where — pagination stays correct
const { allowed, where } = await scopedFindWhere({
  rls, model: 'servers', user, where: { status: 'RUNNING' },
});
if (!allowed) return { items: [], total: 0 };
const [items, total] = await serverRepo.findAndCount({ where, skip, take }); // total = scoped count

// or translate a filter yourself
const rows = await serverRepo.find({ where: toFindOptionsWhere({ customer_id: user.id }) });
```

It translates only the subset where mingo and SQL agree exactly — equality, `null`→
`IsNull`, `$eq`/`$ne`/`$in`/`$nin`/`$gt`/`$gte`/`$lt`/`$lte`, top-level `$and`/`$or` — and
**throws** on anything it can't represent faithfully (`$regex`, JSON-path, `$or` nested in
`$and`), so it never silently mistranslates into a leaky `where`. Keep RLS guard scopes to
that subset (they almost always are).

## Multi-replica

Exactly one process consumes the slot; every replica matches its own sessions. Two things
are coordinated across replicas — slot ownership and change fan-out — and both use
**Postgres itself**.

If the WAL consumer runs in a guaranteed single instance, you need *neither*: the default
`NoopLeaderElector` + `InProcessBus` already cover a one-process deployment.

```ts
import { PgAdvisoryLockLeaderElector, PgNotifyBus } from '@workspace/pg-realtime';

new RealtimeEngine({
  /* ...connectionString, slotName, publicationName, models... */
  // Leader election via a session advisory lock — no TTL, one extra connection;
  // it auto-fails-over when the holder disconnects. Only needed if the *consumer*
  // runs in >1 replica.
  leader: new PgAdvisoryLockLeaderElector({ connectionString: directUrl, lockName: 'pg_realtime' }),
  // Fan-out via LISTEN/NOTIFY. Caveat: a change's row image must fit Postgres's 8 KB
  // NOTIFY payload cap (fine for small rows; oversized ones are logged + dropped).
  bus: new PgNotifyBus({ connectionString: directUrl }),
});
```

The `LeaderElector` and `PubSubBus` interfaces are public, so a project could supply a
different backend (e.g. Redis) — but the package ships only the Postgres implementations.

## Configuration reference

### `EngineConfig`

| field | type | default | notes |
|---|---|---|---|
| `connectionString` | `string` | — | libpq DSN; **direct** (non-pooled) for the WAL connection |
| `slotName` | `string` | — | logical replication slot |
| `publicationName` | `string` | — | publication, scoped to the models' tables (created if absent) |
| `models` | `ModelConfig[]` | — | the tables to watch |
| `consume` | `boolean` | `true` | `false` = **follower**: serves subscriptions off the bus but never reads the slot (run in the HTTP tier while one always-on process consumes) |
| `leader` | `LeaderElector` | `NoopLeaderElector` | who consumes the slot |
| `bus` | `PubSubBus` | `InProcessBus` | cross-replica fan-out |
| `channel` | `string` | `'pg_realtime:changes'` | bus channel name |
| `snapshotMaxRows` | `number` | `10000` | defensive cap on snapshot rows per subscription |
| `logger` | `Logger` | no-op | logs only startup, stale/recovery transitions, and failures |

### `ModelConfig`

| field | type | default | notes |
|---|---|---|---|
| `table` | `string` | — | table name as it appears in the publication / WAL relation |
| `schema` | `string` | `'public'` | |
| `name` | `string` | `table` | logical model name clients subscribe by |
| `primaryKey` | `string \| string[]` | replica-identity key columns | used for the per-session Set, delete identity, and refetch |
| `refetchOnUpdate` | `boolean` | `false` | refetch the full row by PK on a TOAST-incomplete update (see below) |
| `mapRow` | `(raw) => row` | identity | map a raw snake_case PG row to the shape your guards/clients expect |
| `guard` | `RealtimeRuleGuard` | — | row-level security |
| `coarseScope` | `(user) => { text, values }` | — | optional SQL pre-filter for the snapshot (perf only; **must be a superset** — mingo enforces security) |

**Column naming / `mapRow`.** The matcher operates on the raw WAL row (snake_case column
names). If your guards and clients speak camelCase, supply `mapRow` to convert; it runs
before mingo, on both the snapshot and the live path. If your entity properties are
already snake_case (matching columns), `mapRow` is identity.

## How it works

1. **Capture (leader only).** A logical replication connection (`pg-logical-replication`,
   `pgoutput`) streams committed inserts/updates/deletes from the WAL. Each transaction is
   buffered and, on `commit`, every change is stamped with the transaction's
   `commitEndLsn` — the visibility-ordered LSN — then handed downstream and acknowledged.
   Acking only after delivery makes it at-least-once; the per-session Set + PK dedup makes
   redelivery idempotent.
2. **Normalize.** Each pgoutput message becomes a `ChangeEvent` (`op`, `schema`, `table`,
   `pk`, `row`, `oldRow`, `toastIncomplete`, `lsn`) keyed by raw column names.
3. **Fan out.** The leader publishes each `ChangeEvent` on the `PubSubBus`; *every* replica
   subscribes and matches its own local sessions ("Redis Oplog" shape — decode once, match
   per replica). In a single process the default `InProcessBus` makes this a direct call.
4. **Match.** Each subscription holds a compiled mingo query and a `documentIds` Set. For
   each change it computes a transition — `add`/`update`/`remove` — using the Set as its
   memory of prior membership. This is the smart fan-out: a change only reaches the
   subscriptions whose query matches it.
5. **Snapshot.** On the first handler, the subscription takes a consistent initial result
   set and reconciles it with concurrently-buffered changes (next section).

The engine is restart-safe: publication/slot creation is idempotent, and a non-leader just
subscribes to the bus and matches.

## Snapshot ↔ stream consistency

A subscription buffers live changes from the moment it registers, captures
`pg_current_wal_lsn()` as a **lower bound**, takes its snapshot in a `REPEATABLE READ`
transaction, then replays buffered changes with `lsn > snapshotLsn` (the matcher dedupes by
PK). Capturing the LSN *before* the snapshot is deliberate: a row's commit WAL record
precedes its visibility, so a lower bound makes a dropped row **practically impossible**
(any row not in the snapshot is replayed) — the residual cost is occasionally replaying an
already-present row, which the insert dedup absorbs. A row deleted in the window is
retracted rather than left as a phantom. The only *provably* zero-window version aligns the
snapshot to the slot via `pg_export_snapshot()` (a noted future hardening). See
`src/engine/snapshot.ts`, `src/engine/db.ts`, and the concurrent-write snapshot-race test.

## The TOAST caveat (`refetchOnUpdate`)

On an UPDATE, Postgres omits unchanged **TOASTed** (large `text`/`jsonb`/…) columns from
the WAL image — **regardless of `REPLICA IDENTITY`**. Predicate columns are expected to be
small (always present), so *matching* is unaffected. But the row *payload* may be missing a
large column's current value. Set `refetchOnUpdate: true` on such tables: the engine
refetches the full row by PK before emitting, but only on updates that actually dropped a
TOAST column. `REPLICA IDENTITY FULL` is **not** required (the per-session Set + the PK on
deletes cover old-state) and does **not** fix the TOAST hole.

## Scaling, honestly

Fan-out is **O(connected queries) per write, per replica** (the same curve as Meteor and
minimongo): every live subscription re-runs its mingo query on every change to its table.
The wins here are **capability** (arbitrary live filtered queries), robustness (WAL is
ordered/resumable; no `LISTEN/NOTIFY` 8 KB cap on the capture path; no refetch-per-change),
reuse, and pluggable infra — **not** Firestore/Supabase index-backed scaling. The named
future lever is **predicate indexing**: bucket subscriptions by equality key so a write only
fans out to matching buckets.

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

First cut, **not yet wired into a consuming app**. No write path. No predicate indexing.

**Test-covered** (unit + integration against logical-replication Postgres): the engine path
(change capture, matching, snapshot consistency incl. a concurrent-write race test, the auth
guard, TOAST refetch); the `RealtimeRls` authorizer (pure checks + DB-backed `query`/`get`);
the TypeORM RLS translator (`toFindOptionsWhere`/`scopedFindWhere`, incl. a real-DataSource
`findAndCount` pagination test); the Postgres-native adapters (advisory-lock election, NOTIFY
round-trip, and a two-engine "one consumer fans out to every replica" end-to-end); and the
SSE transport (`formatSse`/`pipeToSse` + a real HTTP SSE end-to-end).

**Shipped but not yet covered by automated tests:** the socket.io transport + `PgRealtimeClient`,
the NestJS `sseObservable` helper, and the `SseClient` browser client.
