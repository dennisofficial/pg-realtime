# @workspace/pg-realtime

Reusable, **read-only** realtime-over-Postgres engine. It captures row changes via
**logical replication (WAL)**, diffs each changed row exactly once at the source, and
fans the result out to only the client subscriptions whose **mingo query** matches it —
the "smart fan-out" pattern, with a single multiplexed socket and field-level patches.

Think Supabase's capture mechanism (WAL, no triggers) married to the Firestore/Meteor
"match-on-write" idea, with a GraphQL-style single-socket live-query transport on top.

```
Postgres WAL
   │  logical slot — ONE leader process holds it (advisory lock)
   ▼
LEADER: decode pgoutput → FULL-diff old/new → ChangeEvent{ table, pk, op, oldRow, lsn }
   │
   ▼  PubSubBus.publish()                         ← the change bus (in-process / NOTIFY / Redis)
   ├───────────────┬────────────────┐
   ▼               ▼                ▼
 server A        server B         server C        (every server subscribes; gets every event)
   │ local matcher: test event vs subs of ITS OWN connected clients (mingo + changedColumns)
   ▼
 ONE socket.io connection per client, MUXED by subId  (superjson parser → Date/BigInt survive)
   ▼
 RealtimeClient SDK: normalized cache, apply snapshot/patch, route by changed field → component
```

**Layer ownership**
- **Server compares** (FULL diff, once per event, at the leader) → emits patches.
- **Client caches + routes** (never re-diffs) → per-field re-render suppression.
- **RLS is the one gate** (a `RealtimeRuleGuard` mingo policy) — the same predicate scopes
  the SQL snapshot and the live matcher; nothing is enforced client-side.
- **Client owns its subscription list** — server per-socket state is ephemeral; the client
  replays its subs on every (re)connect and reconciles the fresh snapshot against its cache.

## Contents

- [Install](#install)
- [Core concepts](#core-concepts)
- [Postgres prerequisites](#postgres-prerequisites)
- [Server setup](#server-setup) — [`RealtimeEngine`](#realtimeengine-standalone) · [NestJS `PgRealtimeModule`](#nestjs-pgrealtimemodule) · [choosing a bus](#choosing-a-bus) · [leader election](#leader-election)
- [Field-level patches (`REPLICA IDENTITY FULL`)](#field-level-patches-replica-identity-full)
- [The multiplexed socket transport](#the-multiplexed-socket-transport)
- [Client](#client)
- [RTK Query binding](#rtk-query-binding)
- [Older transports (SSE, one-socket-per-subscription)](#older-transports-sse-one-socket-per-subscription)
- [RLS integration](#rls-integration)
- [NestJS integration (`/nest-realtime`)](#nestjs-integration-nest-realtime)
- [Scaling notes](#scaling-notes)
- [Development](#development)

## Install

```bash
pnpm add @workspace/pg-realtime
```

The core (`pg`, `pg-logical-replication`, `mingo`, `superjson`) has no optional peers to
install. Everything else lives behind a subpath export and needs its own peer:

| Subpath | Peer(s) to add | Provides |
|---|---|---|
| `@workspace/pg-realtime` | — | `RealtimeEngine`, `RealtimeRls`, `RealtimeRuleGuard`, types, `InProcessBus`, `NoopLeaderElector`, `PgNotifyBus`, `PgAdvisoryLockLeaderElector`, `RedisBus`, `lsn` helpers |
| `@workspace/pg-realtime/nest` | `@nestjs/common`, `rxjs` | `PgRealtimeModule`, `PG_REALTIME_ENGINE`, `sseObservable` |
| `@workspace/pg-realtime/nest-realtime` | `@nestjs/common`, `@nestjs/core`, `socket.io`, `typeorm` | `RealtimeNestModule`, `Realtime`/`getRealtimePublish`, `buildRealtimeModels`, `RealtimeResourceRegistry`, `resolveResource` — the packaged NestJS gateway/discovery glue, see [NestJS integration](#nestjs-integration-nest-realtime) |
| `@workspace/pg-realtime/socketio` | `socket.io` | `attachMux`, `superjsonParser`, `projectDelta`, (legacy) `attachSocketIO` |
| `@workspace/pg-realtime/client` | `socket.io-client` | `RealtimeClient` (single-socket), `PgRealtimeClient` (legacy one-socket-per-sub) |
| `@workspace/pg-realtime/rtk` | — (browser-safe, no Node code) | `streamList`, `streamDocument`, `makeSocketListOpener`, `makeSocketDocumentOpener` |
| `@workspace/pg-realtime/sse` | — | `pipeToSse`, `formatSse` — framework-agnostic SSE server |
| `@workspace/pg-realtime/sse/client` | `@microsoft/fetch-event-source` | `SseClient` — browser SSE client |
| `@workspace/pg-realtime/typeorm` | `typeorm` | `toFindOptionsWhere`, `scopedFindWhere` — RLS scope as a TypeORM `where` |

All peers are declared `optional` in `package.json` — install only what the subpaths you
import need.

## Core concepts

- **`ModelConfig`** — one entry per published table: `table`/`schema`, `name` (what
  clients subscribe by, default = `table`), `primaryKey`, `mapRow` (raw snake_case row →
  client shape), `guard` (row-level security), `coarseScope` (perf-only SQL pre-filter),
  `refetchOnUpdate` (TOAST safety), `replicaIdentityFull` (documentation hint — see
  below).
- **`RealtimeRuleGuard`** — an abstract class with `canRead`/`canCreate`/`canUpdate`/
  `canDelete`, each returning a `GuardDecision`: a mingo filter (scope the rows), `true`
  (allow all), or `false` (deny outright). The realtime engine only ever calls `canRead`
  (or the guard's own fallback to it) to scope a subscription — one gate for both the
  snapshot SELECT and the live matcher, so they can never diverge.
- **Leader election (`LeaderElector`)** — exactly one process may hold the logical
  replication slot. `NoopLeaderElector` (default, always leader — for single-instance
  deployments) or `PgAdvisoryLockLeaderElector` (session advisory lock, no TTL, auto
  fails over when the holder's connection drops).
- **The bus (`PubSubBus`)** — how the leader's decoded `ChangeEvent`s reach every other
  process's local matcher: `InProcessBus` (default, same-process only), `PgNotifyBus`
  (Postgres LISTEN/NOTIFY, no extra infra), or `RedisBus` (the scale path, no payload
  cap). See [choosing a bus](#choosing-a-bus).

## Postgres prerequisites

- `wal_level = logical`, `max_replication_slots >= 1`, `max_wal_senders >= 1`
- A role with the `REPLICATION` attribute
- A **direct** connection for `EngineConfig.connectionString` — connection poolers
  (PgBouncer) don't speak the replication protocol
- Each watched table needs a primary key (or an explicit `ModelConfig.primaryKey`)

## Server setup

### `RealtimeEngine` (standalone)

```ts
import { RealtimeEngine } from '@workspace/pg-realtime';

const engine = new RealtimeEngine({
  connectionString: process.env.DATABASE_URL!,
  slotName: 'pg_realtime_slot',
  publicationName: 'pg_realtime_pub',
  models: [
    { table: 'servers', primaryKey: 'id' },
    { table: 'blog_posts', primaryKey: 'id', refetchOnUpdate: true }, // large columns
  ],
});
await engine.start(); // leader ensures the publication + slot, then consumes the WAL
// ... attach a transport (see below) ...
await engine.stop();  // on shutdown
```

`engine.openSubscription({ model, user, filter?, pk? })` returns a `Subscription`
(`.on(handler)`, `.close()`) whose first delta is always `{ kind: 'data', rows }` (the
snapshot), followed by `add`/`update`/`remove` deltas. `engine.rls` exposes a
`RealtimeRls` authorizer pre-wired to the engine's models/pool — see
[RLS integration](#rls-integration).

### NestJS `PgRealtimeModule`

`PgRealtimeModule` is deliberately **transport-free** — it only owns the `RealtimeEngine`
lifecycle (`onModuleInit` → `engine.start()`, `onApplicationShutdown` → `engine.stop()`).
You wire a socket.io gateway yourself (see
[the multiplexed socket transport](#the-multiplexed-socket-transport)).

```ts
import { PgRealtimeModule, PG_REALTIME_ENGINE } from '@workspace/pg-realtime/nest';

@Module({
  imports: [
    PgRealtimeModule.forRootAsync({
      inject: [EnvService],
      useFactory: (env: EnvService) => ({
        connectionString: env.get('DATABASE_URL'),
        slotName: 'pg_realtime_slot',
        publicationName: 'pg_realtime_pub',
        leader: new PgAdvisoryLockLeaderElector({ connectionString: env.get('DATABASE_URL') }),
        bus: new RedisBus({ url: env.get('REDIS_URL') }),
        // `models` here is optional — every `forFeature` contribution below is merged in.
      }),
    }),
  ],
})
export class AppModule {}
```

Individual feature modules can contribute models without the root module knowing about
them, via `PgRealtimeModule.forFeature({ imports, inject, useFactory })` in their own
`imports: []` — `useFactory` returns `ModelConfig[]` built from that feature's own DI
deps (e.g. a `TypeOrmModule.forFeature([Entity])`). `forRoot`/`forRootAsync` must be
evaluated after the feature modules import (an `AppModule` importing them first already
guarantees this) so every contribution is registered before the engine is constructed.
Inject the engine anywhere with `@Inject(PG_REALTIME_ENGINE) engine: RealtimeEngine`.

### Choosing a bus

| Bus | Infra | Payload cap | Use when |
|---|---|---|---|
| `InProcessBus` (default) | none | none | single process, or in tests |
| `PgNotifyBus` | Postgres only | **~8000 bytes** (`PAYLOAD_LIMIT = 7990`) — larger changes are logged and dropped | multi-replica, no extra service, rows stay small |
| `RedisBus` | Redis (`ioredis`, optional peer) | none | multi-replica at scale, or any table with wide/hot rows |

```ts
// Postgres-native, no Redis:
import { PgNotifyBus } from '@workspace/pg-realtime';
bus: new PgNotifyBus({ connectionString: directUrl });

// Scale path, no NOTIFY cap — either a url or pre-connected ioredis instances:
import { RedisBus } from '@workspace/pg-realtime';
bus: new RedisBus({ url: process.env.REDIS_URL });
// or: new RedisBus({ publisher, subscriber }) — Redis pub/sub needs two connections,
// since a connection in subscribe mode can't issue other commands.
```

Every process — leader or follower — subscribes to the bus and matches its own locally
connected sockets; the bus never decides membership, it only fans out the leader's
already-diffed `ChangeEvent`s. `RedisBus` serializes with `superjson` (so `Date`/`BigInt`
survive); `PgNotifyBus` serializes with plain `JSON.stringify` (bigints coerced to
strings) — keep that in mind if you rely on BigInt columns crossing the bus.

Set `EngineConfig.consume: false` to run a **follower**: it subscribes to the bus and
serves subscriptions but never opens the replication slot — useful for an HTTP tier that
scales to zero while one always-on process holds the slot. A follower still needs a real
bus (not `InProcessBus`) to receive changes from the consumer.

### Leader election

Single instance: the default `NoopLeaderElector` (always leader) is correct — don't
configure anything. Multiple replicas running the WAL consumer:

```ts
import { PgAdvisoryLockLeaderElector } from '@workspace/pg-realtime';
leader: new PgAdvisoryLockLeaderElector({ connectionString: directUrl, lockName: 'pg_realtime' });
```

A session-level `pg_advisory_lock` — no TTL to tune, one extra connection per instance,
auto-fails-over when the holder's connection drops.

## Field-level patches (`REPLICA IDENTITY FULL`)

By default Postgres's WAL UPDATE image only carries the **new** row (`REPLICA IDENTITY
DEFAULT`), so the engine can't tell which columns actually changed. Opt a table in:

```sql
ALTER TABLE jobs REPLICA IDENTITY FULL;
```

With `REPLICA IDENTITY FULL`, the WAL UPDATE message also carries the **old** row image.
The engine captures it as `ChangeEvent.oldRow`; the matcher (`engine/matcher.ts`) then
computes `changedColumns = diffColumns(mapRow(oldRow), mapRow(newRow))` **once, at the
point of match**, and attaches it to the `update` `RowDelta`. `socketio/mux.ts`'s
`projectDelta` turns that into the wire envelope:

- `changedColumns` present and non-empty → `patch` = only those keys (minimal bytes).
- `changedColumns` present but **empty** (no exposed field actually changed) → the
  envelope is **suppressed** (`projectDelta` returns `null`) — e.g. an UPDATE that only
  touched an un-exposed column.
- `changedColumns` **undefined** (table not `REPLICA IDENTITY FULL`, no old image) →
  `patch` is the full mapped row — still correct, just not minimal.

Set `ModelConfig.replicaIdentityFull: true` as a documentation/validation hint — it does
**not** change behavior; the engine keys off whether `ChangeEvent.oldRow` is actually
present, not off this flag.

**Tradeoff:** `REPLICA IDENTITY FULL` makes every UPDATE's WAL record carry the entire
old row, not just the primary key — write amplification on wide/hot tables. Opt in per
table (only tables clients subscribe to), and leave wide/rarely-subscribed tables off it;
they still work correctly, just with full-row `update` payloads instead of patches. Note
separately: Postgres omits unchanged **TOASTed** columns from *any* UPDATE image
regardless of `REPLICA IDENTITY` — see `ModelConfig.refetchOnUpdate` in
[RLS integration](#rls-integration) below for that (distinct) caveat.

## The multiplexed socket transport

**Server** — construct `io` with the `superjsonParser` (so `Date`/`BigInt` survive the
wire) and bind `attachMux`:

```ts
import { Server } from 'socket.io';
import { attachMux, superjsonParser } from '@workspace/pg-realtime/socketio';

const io = new Server(httpServer, {
  parser: superjsonParser,
  cors: { origin: FRONTEND_HOST, credentials: true },
});

attachMux(io, engine, {
  authenticate: (handshake) => authenticateFromCookie(handshake), // throw/reject to refuse the connection
});
```

`authenticate(handshake) => Principal | Promise<Principal>` runs **once per socket
connection** (not per subscription) and its result is the `user` passed to every model
guard this socket's subscriptions open. Throwing rejects the connection.

One connection carries **many** logical subscriptions, keyed by a client-chosen `subId`,
all multiplexed onto a single `'rt'` event:

**Client → server** (control, plain socket.io events):
- `subscribe { subId, model, filter?, pk? }`
- `unsubscribe { subId }`

**Server → client** (`'rt'` event, envelope shape from `projectDelta`):
- `{ subId, op: 'data', rows: [{ pk, row }] }` — snapshot, once per subscribe
- `{ subId, op: 'add', pk, row }` — row entered the result set (full row)
- `{ subId, op: 'update', pk, patch }` — **only changed exposed fields** (see above)
- `{ subId, op: 'remove', pk }` — row left the result set
- `{ subId, op: 'error', message }`

`add`/`remove` always carry the full row/pk regardless of field selection (they're
membership changes); only `update` is patched.

**NestJS gateway example** (mirrors the shape Atlas's `RealtimeGateway` uses — bind after
the http server is listening, auth via an httpOnly cookie or a JWT):

```ts
@Injectable()
export class RealtimeGateway implements OnApplicationBootstrap, OnApplicationShutdown {
  private io: Server | null = null;

  constructor(
    private readonly httpAdapterHost: HttpAdapterHost,
    @Inject(PG_REALTIME_ENGINE) private readonly engine: RealtimeEngine,
    private readonly jwtService: JwtService,
  ) {}

  onApplicationBootstrap(): void {
    const httpServer = this.httpAdapterHost.httpAdapter.getHttpServer();
    this.io = new Server(httpServer, { parser: superjsonParser, cors: { credentials: true } });
    attachMux(this.io, this.engine, { authenticate: (hs) => this.authenticate(hs) });
  }

  async onApplicationShutdown(): Promise<void> {
    if (this.io) await new Promise<void>((resolve) => this.io!.close(() => resolve()));
  }

  private readonly authenticate = async (handshake: Socket['handshake']) => {
    const token = readCookie(handshake.headers.cookie, 'access_token') ?? (handshake.auth as any)?.token;
    if (!token) throw new Error('No authentication token provided');
    const { sub } = await this.jwtService.verifyAccessToken(token);
    return this.userRepo.findOneByOrFail({ id: sub }); // this is the `user` guards receive
  };
}
```

Why superjson: plain `JSON.stringify` turns `Date` into a string and drops `BigInt`
entirely; `superjsonParser` (an `Encoder`/`Decoder` pair matching socket.io-parser's
contract) round-trips both, so a `createdAt: Date` column on the wire deserializes back
into a real `Date` on the client with no manual coercion.

## Client

`RealtimeClient` (from `@workspace/pg-realtime/client`) owns **one** socket.io
connection and multiplexes every `query`/`document` call over it:

```ts
import { RealtimeClient } from '@workspace/pg-realtime/client';

interface Models {
  jobs: { id: string; status: string; createdAt: Date };
  threads: { id: string; jobId: string };
}

const client = new RealtimeClient<Models>({
  url: SOCKET_URL,
  auth: async () => ({ token: await getAccessToken() }), // resolved fresh on every (re)connect
  withCredentials: true, // send cookies cross-origin (httpOnly-cookie auth)
  transports: ['websocket'], // default
});

const jobs = client.query('jobs', { filter: { status: 'RUNNING' } });
const unsub = jobs.subscribe((rows) => render(rows)); // fires on any add/update/remove
jobs.get(); // current normalized snapshot, synchronously

const doc = client.document('jobs', JSON.stringify([jobId]));
doc.subscribe((row) => render(row)); // row is `T | null`

unsub(); // ref-counted: closes the server-side sub once the last listener unsubscribes
client.close(); // tears down the socket and every subscription
```

`query`/`document` are **ref-counted**: the server-side `subscribe` message is sent on
the first listener (0→1) and `unsubscribe` on the last (N→0); the underlying `Collection`
resets its cache on close, so a resubscribe always starts fresh.

**Field-level routing** — `select(fields)` returns a derived view that only notifies when
an `update`'s changed keys intersect `fields` (`add`/`remove` are membership changes and
always notify):

```ts
const status = jobs.select(['status']);
status.subscribe((rows) => renderStatusOnly(rows)); // ignores updates to unselected fields
```

`onChange(listener)` exposes the precise per-change stream (`{ op: 'add'|'update'|'remove', pk, row, changedFields? }`)
that `select()` itself filters on — the one place a consumer (e.g. an RTK opener) can
read exact deltas without re-diffing.

**Reconnect guarantee** — the client, not the server, owns the subscription list. On
every `connect` event it replays every active `{ subId, spec }` pair; each re-subscribe
gets a fresh `data` snapshot from the server, and the client's `Collection` **reconciles**
that snapshot against whatever it already cached, deriving synthetic `add`/`update`
(with a real `changedFields`)/`remove` events for listeners — so a dropped connection
never silently misses a change. `update` envelopes themselves are never re-diffed: the
server's `patch` keys ARE the changed-field set.

## RTK Query binding

`@workspace/pg-realtime/rtk` is dependency-free at the top level (safe to import in any
frontend) and exposes `streamList`/`streamDocument` — generic `onCacheEntryAdded` helpers
that reconcile an `SseOpener`'s `data`/`add`/`update`/`remove` messages into RTK Query's
cache — plus `makeSocketListOpener`/`makeSocketDocumentOpener`, which adapt a
`RealtimeClient` into that same `SseOpener` shape so the two helpers can be driven off the
single socket instead of a per-endpoint SSE stream:

```ts
import { RealtimeClient } from '@workspace/pg-realtime/client';
import { makeSocketListOpener, makeSocketDocumentOpener, streamList, streamDocument } from '@workspace/pg-realtime/rtk';

const client = new RealtimeClient<Models>({ url: SOCKET_URL, auth: getAuthHeader }); // one app singleton

export const jobsApi = createApi({
  endpoints: (build) => ({
    getJobs: build.query<Job[], { status?: string }>({
      query: (arg) => ({ url: '/jobs', params: arg }),
      onCacheEntryAdded: (arg, api) =>
        streamList({
          url: 'jobs', // label only — not used to reach the network, the socket is already open
          open: makeSocketListOpener(client, 'jobs', { filter: arg }),
          lifecycle: api,
        }),
    }),
    getJob: build.query<Job, string>({
      query: (id) => ({ url: `/jobs/${id}` }),
      onCacheEntryAdded: (id, api) =>
        streamDocument({
          url: `jobs/${id}`,
          open: makeSocketDocumentOpener(client, 'jobs', id),
          lifecycle: api,
        }),
    }),
  }),
});
```

`streamList` keeps an internal `Map<pk,row>` (preserves row identity/order across
patches); `streamDocument` keeps a single cached row (`remove` keeps the last-known value
by default; pass `onRemove` to change that).

**Component-level memoization** — RTK Query's default hook re-renders on *any* cache
change even though the underlying patches are already minimal. Narrow a component's
subscription with `selectFromResult`:

```ts
const { status } = useGetJobQuery(id, {
  selectFromResult: ({ data }) => ({ status: data?.status }),
});
```

## Older transports (SSE, one-socket-per-subscription)

Two earlier bindings still ship and remain useful outside a single-socket setup:

- **`attachSocketIO(io, engine, { extractUser? })`** (`socketio/index.ts`) — one socket
  *per* subscription; the spec rides in the connection handshake `auth`
  (`io(url, { auth: { model, filter, pk } })`); events are plain `data`/`add`/`update`/
  `remove`/`exception` (no envelope, no patches). Paired client: `PgRealtimeClient`
  (`onQuery`/`onDocument`) from `@workspace/pg-realtime/client`. Superseded by
  `attachMux`/`RealtimeClient` for anything that wants one long-lived connection.
- **SSE** (`@workspace/pg-realtime/sse`, `/sse/client`, and `sseObservable` from
  `/nest`) — a read-only server→client stream, a natural drop-in for an existing
  `EventSource`/RTK-Query frontend that isn't moving to sockets. `pipeToSse(sub, sink)`
  streams a `Subscription`'s deltas as SSE events (event name = delta kind); `sseObservable`
  adapts that to an `Observable<MessageEvent>` for a NestJS `@Sse()` handler; `SseClient`
  is the browser counterpart, built on `@microsoft/fetch-event-source` for auth headers
  and backoff reconnect. `@workspace/pg-realtime/rtk`'s `streamList`/`streamDocument`
  also work directly against an SSE opener (that's their original shape); the socket
  variant above is an adapter on top.

Neither older path carries `changedColumns`/patches — SSE and the legacy socket binding
always send the full mapped row on `update`.

## RLS integration

A `RealtimeRuleGuard` is the one row-level-security definition for a model — it drives
the subscription's snapshot+live scope, `RealtimeRls` (server-side authorization), and
(via the TypeORM helper) your own ORM fetches:

```ts
class ServersGuard extends RealtimeRuleGuard<AuthUser> {
  canRead(user: AuthUser | null) {
    return user ? { customer_id: user.id } : false; // filter scopes; true allows all; false denies
  }
  // canCreate/canUpdate/canDelete are optional — each falls back to canRead.
}
models: [{ table: 'servers', primaryKey: 'id', guard: new ServersGuard() }];
```

`engine.rls` (in-process) or a standalone `new RealtimeRls({ models, pool })` exposes
`scope`/`authorize`/`filterRows` (pure, no DB) and `query`/`get` (DB-backed, same
coarse-SQL + mingo path a subscription's snapshot uses) — so a server-side mutation
handler authorizes against the exact same guard a live subscription enforces.
`@workspace/pg-realtime/typeorm`'s `scopedFindWhere`/`toFindOptionsWhere` translate a
guard's mingo scope into a TypeORM `where`, so your own `findAndCount` stays
RLS-scoped **at the database** (correct pagination) instead of filtering after the fetch.

**Atlas's bridge**: `@workspace/nestjs-rls/pg-realtime` exports `rlsGuard(entity,
resolveClaims)`, which wraps an `@Rls`-decorated entity's policy (from
`@workspace/nestjs-rls`) as a `RealtimeRuleGuard` — one `@Rls` policy authorizes SQL
reads (via `db.scoped`), realtime subscriptions, and realtime server-side checks. This
bridge is NOT part of `@workspace/pg-realtime` itself; it lives in the `nestjs-rls`
package as an integration adapter. If your project doesn't use `nestjs-rls`, write a
`RealtimeRuleGuard` subclass directly (as above) — it's a small abstract class with no
required framework.

## NestJS integration (`/nest-realtime`)

`@workspace/pg-realtime/nest-realtime` packages the **entire** gateway/discovery/registry
glue — entity discovery, the socket.io gateway (bound after the http server starts
listening), and the named-resource registry — behind one `RealtimeNestModule.forRootAsync`.
A new project supplies only a handful of **plugs** (auth, column exposure, row-level
guard, CLS/RLS context seeding); the module itself is deliberately cycle-free and never
imports `@workspace/nestjs-rls` or any host-specific code — every RLS/DB/auth-specific
concern comes in as a field on `RealtimeNestConfig`.

### Entity annotations

Two decorators mark an entity publishable and pick its exposed columns:

- **`@Realtime({ name? })`** (from `@workspace/pg-realtime/nest-realtime`) — class
  decorator; an entity without it is never discovered/subscribable, regardless of its
  columns. `name` defaults to the table name and is what clients subscribe by.
- **`@Expose(alias?)`** (from `@workspace/nestjs-rls`) — property decorator; the
  secure-by-default column allowlist. A property left undecorated never reaches the wire,
  even on a `@Realtime` entity. `getExposed(Entity)` (also from `nestjs-rls`) returns the
  `propertyName -> outputName` map and walks the prototype chain, so a shared base class's
  exposures (e.g. `createdAt` on `TimestampedEntity`) are inherited.

```ts
import { Realtime } from '@workspace/pg-realtime/nest-realtime';
import { Expose, Rls } from '@workspace/nestjs-rls';

@Entity({ name: 'jobs' })
@Realtime()
@Rls<Job, AppClaims>((c) => ({ orgId: { $in: c.orgIds } }))
export class Job {
  @Expose()
  id!: string;

  @Expose()
  status!: string;

  internalNotes!: string; // never exposed, never published
}
```

`@Realtime` entities are expected to run under `REPLICA IDENTITY FULL` (for field-level
patches — see [above](#field-level-patches-replica-identity-full)); write the
`ALTER TABLE ... REPLICA IDENTITY FULL;` migration by hand (TypeORM has no concept of it —
the one exception to "generate migrations, never hand-write").

### `RealtimeNestModule.forRootAsync(options)`

`options` is `{ imports?, inject?, useFactory: (...args) => RealtimeNestConfig | Promise<RealtimeNestConfig> }`
— the same async-provider shape as every other Nest `forRootAsync`. `RealtimeNestConfig`:

| Field | What it's for | What you pass (NestJS + `nestjs-rls`) |
|---|---|---|
| `dataSource` | `DataSource` to discover `@Realtime` entities off | `getDataSourceToken()` injected from `@nestjs/typeorm` |
| `engine` | Slot/publication/bus/leader config (`RootEngineConfig`, same shape as `PgRealtimeModule`) | your `connectionString`/`slotName`/`publicationName`/`bus`/`leader` |
| `authenticate` | Resolves the principal from a socket handshake; throw/reject to refuse the connection | your JWT/cookie lookup, e.g. an app `RealtimeAuthService.authenticate(handshake)` |
| `withPrincipalContext` | Wraps every composed-resource `load()` (and Mode B `scopedFind()`) in the app's CLS/RLS context | `(principal, fn) => cls.run(() => { cls.set(CLS_USER, principal); return fn(); })` via `ClsService` |
| `resolveExposed` | Discovery plug: `propertyName -> outputName` map for an entity | `getExposed` from `@workspace/nestjs-rls` |
| `buildGuard` | Discovery plug: row-level auth guard for an entity | `(entity) => rlsGuard(entity, resolveClaims)` from `@workspace/nestjs-rls/pg-realtime` |
| `scopedFind?` | Mode B windowed-query loader (RLS-scoped `ORDER BY`/`LIMIT` fetch). Omit to disable windowed subscriptions | a `ScopedFindService` — see [Query modes](#query-modes--client-usage) below |
| `path?` | socket.io path | — |
| `cors?` | `{ origin, credentials? }` for the socket.io server | `{ origin: env.get('FRONTEND_HOST'), credentials: true }` |
| `logger?` | passed through to `attachMux` | — |

Full example, mirroring Atlas's `AppModule`:

```ts
import { getDataSourceToken } from '@nestjs/typeorm';
import { getExposed } from '@workspace/nestjs-rls';
import { RLS_CONTEXT, type RlsContextConfig } from '@workspace/nestjs-rls/nest';
import { rlsGuard } from '@workspace/nestjs-rls/pg-realtime';
import { RealtimeNestModule, type RealtimeNestConfig } from '@workspace/pg-realtime/nest-realtime';
import type { Principal } from '@workspace/pg-realtime/socketio';
import { ClsService } from 'nestjs-cls';
import type { Socket } from 'socket.io';
import type { DataSource } from 'typeorm';

@Module({
  imports: [
    RealtimeNestModule.forRootAsync({
      imports: [AtlasRealtimeModule], // whatever feeds `engine`'s connectionString/bus/leader
      inject: [
        EnvService,
        getDataSourceToken(),
        RLS_CONTEXT,
        RealtimeAuthService,
        ScopedFindService,
        ClsService,
      ],
      useFactory: (
        env: EnvService,
        dataSource: DataSource,
        ctx: RlsContextConfig,
        realtimeAuthService: RealtimeAuthService,
        scopedFindService: ScopedFindService,
        cls: ClsService,
      ): RealtimeNestConfig => ({
        dataSource,
        engine: atlasRealtimeConfig(env),
        authenticate: (handshake: Socket['handshake']) =>
          realtimeAuthService.authenticate(handshake) as unknown as Promise<Principal>,
        withPrincipalContext: (principal, fn) =>
          Promise.resolve(
            cls.run(() => {
              cls.set(CLS_USER, principal);
              return fn();
            }),
          ),
        resolveExposed: getExposed,
        buildGuard: (entity) => rlsGuard(entity, ctx.resolveClaims),
        scopedFind: (model, spec, principal) => scopedFindService.find(model, spec, principal),
        cors: { origin: env.get('FRONTEND_HOST'), credentials: true },
      }),
    }),
  ],
})
export class AppModule {}
```

`forRootAsync` is `global: true` and exports `RealtimeResourceRegistry` and
`PG_REALTIME_ENGINE` — no need to import it again in feature modules. Internally it wires
`buildRealtimeModels(dataSource, { resolveExposed, buildGuard })` (discovers every
`@Realtime` entity into a `ModelConfig[]`) into an inner `PgRealtimeModule.forRootAsync`,
merges in anything already on `engine.models`, and boots one `RealtimeGateway` that binds
`attachMux` onto the host's http server once it starts listening.

### Named composed resources

Not every subscription is one entity's rows — a client sometimes wants a derived/joined
view keyed by a logical name rather than a raw model. Inject `RealtimeResourceRegistry`
and `register(name, definition)` (typically from a feature module's `onModuleInit`):

```ts
import { Injectable, type OnModuleInit } from '@nestjs/common';
import { RealtimeResourceRegistry } from '@workspace/pg-realtime/nest-realtime';

@Injectable()
export class OrgRealtimeResourcesService implements OnModuleInit {
  constructor(
    private readonly registry: RealtimeResourceRegistry,
    private readonly orgs: OrgService,
  ) {}

  onModuleInit(): void {
    this.registry.register<User>('org_members', {
      // one or more underlying model subscriptions this resource re-fires on
      triggers: (params) => [{ model: 'organization_members', filter: { orgId: params.orgId } }],
      // re-run on every matching trigger change; returns the ordered snapshot
      load: async (params, principal) => {
        const members = await this.orgs.membersOf(principal.id, params.orgId as string);
        return members.map((m) => ({ pk: m.userId, row: m as unknown as Record<string, unknown> }));
      },
    });
  }
}
```

A client subscribes to `'org_members'` (with `filter: { orgId }`) exactly like a plain
model — `resolveResource` finds it on the registry first (before falling through to Mode
B/Mode A), wraps `load()` in `withPrincipalContext`, and re-runs it (pushing a fresh
ordered snapshot) whenever any of `triggers()`'s underlying models change for that filter.
Good for joins/aggregates a single `ModelConfig` can't express — `load()` can hit the DB
however it needs to (raw query, multiple repos, etc.), as long as it returns
`Array<{ pk, row }>` scoped to `principal`.

### Query modes / client usage

Two ways a client subscription is served, dispatched by `resolveResource` per-subscribe:

- **Mode A (unbounded stream)** — the default for a plain `@Realtime` model with no
  `sort`/`limit`/`offset`/`after` in the query: the engine's own snapshot+live-match path,
  field-level `update` patches per [above](#field-level-patches-replica-identity-full).
- **Mode B (windowed)** — the query spec carries `sort`/`limit`/`offset`/`after`
  (`client.query(model, { filter, sort, limit, offset })`) and a `scopedFind` plug was
  configured: one trigger subscription on the model as a change signal, but the actual
  rows come from re-running `scopedFind(model, spec, principal)` — a real
  `ORDER BY ... LIMIT ... OFFSET ...` fetch — on every matching change, so windowed/paged/
  sorted lists stay correct instead of drifting via client-side patch application.
  `spec.after` (keyset pagination) is accepted on the wire but not yet implemented by any
  shipped `scopedFind` — throw until you add it.

A `scopedFind` plug sketch (RLS-scoped find + `@Expose` projection + a sortable-column
guardrail):

```ts
@Injectable()
export class ScopedFindService {
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly db: Db, // nestjs-rls's `db.scoped(Entity)`
  ) {}

  async find(model: string, spec: QuerySpec, principal: unknown): Promise<Array<{ pk: string; row: Row }>> {
    if (spec.after !== undefined) throw new Error('scopedFind: keyset pagination not yet implemented');

    const entity = this.resolveEntity(model); // via getRealtimePublish() over dataSource.entityMetadatas
    const exposed = getExposed(entity); // property -> outputName
    const reverseExposed = new Map(Array.from(exposed, ([p, o]) => [o, p]));

    // reject any filter/sort field that isn't `@Expose()`d — never leak/hide rows by an
    // un-exposed column name, and never let a client sort by a column that isn't allowlisted
    const where = spec.filter ? toFindOptionsWhere(this.remap(spec.filter, reverseExposed)) : undefined;
    const order = spec.sort ? this.remapOrder(spec.sort, reverseExposed) : undefined;

    const rows = await this.db.scoped(entity).find({ where, order, take: spec.limit, skip: spec.offset });
    return rows.map((row) => ({ pk: this.pk(row), row: this.project(row, exposed) }));
  }
}
```

wired in as `scopedFind: (model, spec, principal) => scopedFindService.find(model, spec, principal)`
on `RealtimeNestConfig`.

Client-side, both modes are driven through the same `RealtimeClient.query(model, spec)`
(see [Client](#client)) and RTK helpers (see [RTK Query binding](#rtk-query-binding)) —
Mode A vs Mode B is entirely a server-side dispatch decision keyed off whether the query
spec is windowed and whether `scopedFind` was configured.

### The plug/responsibility split

| Package-provided (`@workspace/pg-realtime/nest-realtime`) | App-provided (plugs) |
|---|---|
| `RealtimeNestModule` (gateway, discovery, boot wiring) | `authenticate` — how a handshake resolves a principal |
| `@Realtime()` decorator + `buildRealtimeModels` discovery | `@Expose()`/`resolveExposed` — which columns/entities are policy (from `nestjs-rls` or hand-rolled) |
| `RealtimeResourceRegistry` (storage + wiring for named resources) | The named resources themselves — `register(name, { triggers, load })` per feature module |
| `resolveResource` (Mode A/B/composed dispatch) | `buildGuard` — the row-level auth guard per entity (e.g. `rlsGuard`) |
| The socket.io gateway (`attachMux` binding, superjson parser, CORS/path) | `withPrincipalContext` — CLS/RLS context seeding for `load()`/`scopedFind()` |
| — | `scopedFind` — the actual windowed-query DB fetch (Mode B) |
| — | `engine` — connection string, slot/publication names, bus, leader election |

## Scaling notes

- **Pluggable bus** — `PubSubBus` is a two-method interface (`publish`/`subscribe`); this
  package ships `InProcessBus`/`PgNotifyBus`/`RedisBus`, but a project can supply another
  backend (NATS, etc.) as long as it implements the interface. Cutting the bus over is
  all-or-nothing across every replica (they must agree on the wire format) — there's no
  staged/dual-bus mode.
- **Per-server subscription indexing** — each `RealtimeEngine` instance indexes its own
  live subscriptions `routingKey (schema.table) -> Set<Subscription>`
  (`subsByTable` in `engine/engine.ts`); a bus event only walks the subscriptions on its
  own table, not the whole registry. There is currently **no further indexing by
  predicate/`coarseScope`** — every subscription on a table re-runs its mingo query on
  every change to that table (`O(connected queries)` per write, per replica). A named
  future lever is bucketing subscriptions by an equality key so a write only fans out to
  matching buckets.
- **One WAL consumer** — regardless of bus choice, only the process holding the
  `LeaderElector` reads the replication slot; every other process (leader or follower)
  only matches changes it receives off the bus. This is a hard constraint, not a
  configuration choice — Postgres logical replication has one reader per slot.
- **Reconnect** — no durable per-client event queues or missed-event replay; a dropped
  connection re-subscribes and gets a fresh snapshot, reconciled against the client's
  cache (see [Client](#client)). The replication slot retains WAL for leader failover,
  not for client catch-up.

## Development

```bash
pnpm build            # tsup -> dist (CJS + ESM)
pnpm typecheck        # tsc --noEmit (tsup does not type-check)
pnpm test             # unit suite (no services needed)

docker compose -f docker-compose.test.yml up -d --wait
pnpm test:integration # end-to-end against logical-replication Postgres
docker compose -f docker-compose.test.yml down -v
```
