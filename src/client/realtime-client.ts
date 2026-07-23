import { io } from 'socket.io-client';
import { superjsonParser } from '../socketio/parser-superjson';
import type { Envelope } from '../socketio/mux';
import { Row } from '../types';

/**
 * Structural subset of `socket.io-client`'s `Socket` this module relies on — lets tests
 * inject a plain EventEmitter fake with no network, mirroring how `parser-superjson.test.ts`
 * fakes the wire without a real server.
 */
export interface FakeableSocket {
  on(event: string, listener: (...args: any[]) => void): unknown;
  off(event: string, listener: (...args: any[]) => void): unknown;
  emit(event: string, ...args: any[]): unknown;
  close?(): void;
  disconnect?(): void;
}

export interface RealtimeClientOptions {
  /** Server URL (optionally including a namespace path). Ignored when `socket` is injected. */
  url: string;
  /** Handshake auth, resolved fresh on every (re)connect. May be async. */
  auth?: () => Record<string, unknown> | Promise<Record<string, unknown>>;
  transports?: Array<'websocket' | 'polling'>;
  /** Send credentials (cookies) with the handshake — needed for httpOnly-cookie auth across origins. */
  withCredentials?: boolean;
  /** Inject a fake socket (tests) instead of opening a real connection. */
  socket?: FakeableSocket;
}

export type Unsubscribe = () => void;

export interface LiveQuery<R extends Row = Row> {
  /** Current normalized snapshot. Empty until the server's first `data` envelope arrives. */
  get(): R[];
  /** Fires on any add/update/remove. Ref-counted: the underlying subscription opens on the
   * first listener and closes when the last one unsubscribes. */
  subscribe(listener: (rows: R[]) => void): Unsubscribe;
  /** Derived view that only notifies when an update's changed fields intersect `fields`
   * (add/remove always notify — they're membership changes). */
  select(fields: string[]): SelectedView<R[]>;
  /** The precise change stream `select()` filters internally — one event per add/update/
   * remove, batched per envelope. `update` carries both the merged row and the exact
   * changed-field set, so a consumer (e.g. an `SseMessage` router) never has to re-diff. */
  onChange(listener: (changes: ChangeEvent<R>[]) => void): Unsubscribe;
}

export interface LiveDoc<R extends Row = Row> {
  get(): R | null;
  subscribe(listener: (row: R | null) => void): Unsubscribe;
  select(fields: string[]): SelectedView<R | null>;
  /** See `LiveQuery.onChange`. */
  onChange(listener: (changes: ChangeEvent<R>[]) => void): Unsubscribe;
}

export interface SelectedView<T> {
  get(): T;
  subscribe(listener: (value: T) => void): Unsubscribe;
}

/**
 * The precise per-change event a `Collection` derives from `data`/`add`/`update`/`remove`
 * envelopes — the ONE diff computation the client does. `select(fields)` and `onChange`
 * both read straight off this stream; nothing downstream re-diffs.
 */
export type ChangeEvent<R extends Row = Row> =
  | { op: 'add'; pk: string; row: R }
  | { op: 'update'; pk: string; row: R; changedFields: string[] }
  | { op: 'remove'; pk: string };

/**
 * Normalized `Map<pk,row>` for one subscription. Applies `data`/`add`/`update`/`remove`
 * envelopes and is the ONLY place the client diffs — and only for a re-snapshot on
 * reconnect, where it reconciles the fresh snapshot against the current cache to derive
 * add/update(changed fields)/remove. `update` never re-diffs: the patch's own keys ARE
 * the changed-field set.
 */
class Collection {
  private readonly rows = new Map<string, Row>();
  private hasSnapshot = false;
  private readonly listeners = new Set<(changes: ChangeEvent[]) => void>();

  getRows(): Row[] {
    return [...this.rows.values()];
  }

  subscribeRaw(fn: (changes: ChangeEvent[]) => void): Unsubscribe {
    this.listeners.add(fn);
    return () => {
      this.listeners.delete(fn);
    };
  }

  /** Drop the cache when the subscription closes — a resubscribe always starts fresh. */
  reset(): void {
    this.rows.clear();
    this.hasSnapshot = false;
  }

  applyEnvelope(envelope: Envelope): void {
    switch (envelope.op) {
      case 'data':
        this.applyData(envelope.rows);
        break;
      case 'add':
        this.applyAdd(envelope.pk, envelope.row);
        break;
      case 'update':
        this.applyUpdate(envelope.pk, envelope.patch);
        break;
      case 'remove':
        this.applyRemove(envelope.pk);
        break;
      case 'error':
        break; // surfaced separately (see RealtimeClient.onError, if/when added)
    }
  }

  private applyData(rows: Array<{ pk: string; row: Row }>): void {
    if (!this.hasSnapshot) {
      this.hasSnapshot = true;
      const changes: ChangeEvent[] = [];
      for (const { pk, row } of rows) {
        this.rows.set(pk, row);
        changes.push({ op: 'add', pk, row });
      }
      this.notify(changes);
      return;
    }

    // Reconnect re-snapshot: reconcile against the current cache instead of blindly
    // replacing it, so listeners see derived add/update(changed fields)/remove.
    const incoming = new Map(rows.map((r) => [r.pk, r.row]));
    const changes: ChangeEvent[] = [];

    for (const [pk, row] of incoming) {
      const existing = this.rows.get(pk);
      if (!existing) {
        this.rows.set(pk, row);
        changes.push({ op: 'add', pk, row });
        continue;
      }
      const changedFields = diffFields(existing, row);
      if (changedFields.length > 0) {
        this.rows.set(pk, row);
        changes.push({ op: 'update', pk, row, changedFields });
      }
    }
    for (const pk of [...this.rows.keys()]) {
      if (!incoming.has(pk)) {
        this.rows.delete(pk);
        changes.push({ op: 'remove', pk });
      }
    }
    this.notify(changes);
  }

  private applyAdd(pk: string, row: Row): void {
    this.rows.set(pk, row);
    this.notify([{ op: 'add', pk, row }]);
  }

  private applyUpdate(pk: string, patch: Row): void {
    const existing = this.rows.get(pk);
    if (!existing) return; // stale patch for a row we don't have (e.g. raced unsubscribe)
    const row = { ...existing, ...patch };
    this.rows.set(pk, row);
    this.notify([{ op: 'update', pk, row, changedFields: Object.keys(patch) }]);
  }

  private applyRemove(pk: string): void {
    if (!this.rows.has(pk)) return;
    this.rows.delete(pk);
    this.notify([{ op: 'remove', pk }]);
  }

  private notify(changes: ChangeEvent[]): void {
    if (changes.length === 0) return;
    for (const listener of [...this.listeners]) listener(changes);
  }
}

function diffFields(a: Row, b: Row): string[] {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  const out: string[] = [];
  for (const key of keys) {
    if (!Object.is(a[key], b[key])) out.push(key);
  }
  return out;
}

function matchesFields(changes: ChangeEvent[], fields: string[] | undefined): boolean {
  if (!fields) return true;
  for (const change of changes) {
    if (change.op !== 'update') return true; // add/remove: membership change, always notify
    if (change.changedFields.some((f) => fields.includes(f))) return true;
  }
  return false;
}

interface SubscribeSpec {
  model: string;
  filter?: Record<string, unknown>;
  pk?: string;
  sort?: Array<[string, 'asc' | 'desc']>;
  limit?: number;
  offset?: number;
  /** Opaque cursor (app-defined shape) for keyset pagination. */
  after?: unknown;
}

interface RegistryEntry {
  spec: SubscribeSpec;
  collection: Collection;
  refCount: number;
}

/**
 * Framework-agnostic, zero-React realtime client. Owns ONE multiplexed socket.io
 * connection (superjson parser, matching the server's `attachMux`) and fans it out to
 * many logical subscriptions keyed by a client-generated `subId`. The client owns the
 * subscription list — server per-socket state is ephemeral, so every (re)connect replays
 * the full registry and each fresh snapshot is reconciled against the existing cache.
 */
export class RealtimeClient<Models extends Record<string, any> = Record<string, any>> {
  private readonly socket: FakeableSocket;
  private readonly registry = new Map<string, RegistryEntry>();
  private nextSubId = 1;
  private connected = false;

  constructor(private readonly opts: RealtimeClientOptions) {
    this.socket = opts.socket ?? this.openSocket();
    this.socket.on('connect', this.handleConnect);
    this.socket.on('disconnect', this.handleDisconnect);
    this.socket.on('rt', this.handleEnvelope);
  }

  query<N extends keyof Models & string>(
    model: N,
    opts: {
      filter?: Record<string, unknown>;
      sort?: Array<[string, 'asc' | 'desc']>;
      limit?: number;
      offset?: number;
      /** Opaque cursor (app-defined shape) for keyset pagination. */
      after?: unknown;
    } = {},
  ): LiveQuery<Models[N]> {
    const collection = new Collection();
    const subId = this.register(
      {
        model,
        filter: opts.filter,
        sort: opts.sort,
        limit: opts.limit,
        offset: opts.offset,
        after: opts.after,
      },
      collection,
    );
    return new LiveQueryHandle<Models[N]>(this, subId, collection);
  }

  document<N extends keyof Models & string>(model: N, pk: string): LiveDoc<Models[N]> {
    const collection = new Collection();
    const subId = this.register({ model, pk }, collection);
    return new LiveDocHandle<Models[N]>(this, subId, collection);
  }

  /** Tear down the connection and every subscription. */
  close(): void {
    this.socket.off('connect', this.handleConnect);
    this.socket.off('disconnect', this.handleDisconnect);
    this.socket.off('rt', this.handleEnvelope);
    this.registry.clear();
    this.socket.close?.();
    this.socket.disconnect?.();
  }

  /** @internal ref-count a handle's active listeners; opens the server-side sub on 0→1. */
  addListener(subId: string): void {
    const entry = this.registry.get(subId);
    if (!entry) return;
    entry.refCount += 1;
    if (entry.refCount === 1 && this.connected) {
      this.socket.emit('subscribe', { subId, ...entry.spec });
    }
  }

  /** @internal closes the server-side sub on N→0. */
  removeListener(subId: string): void {
    const entry = this.registry.get(subId);
    if (!entry) return;
    entry.refCount = Math.max(0, entry.refCount - 1);
    if (entry.refCount === 0) {
      if (this.connected) this.socket.emit('unsubscribe', { subId });
      entry.collection.reset();
    }
  }

  private register(spec: SubscribeSpec, collection: Collection): string {
    const subId = String(this.nextSubId++);
    this.registry.set(subId, { spec, collection, refCount: 0 });
    return subId;
  }

  private openSocket() {
    const auth = this.opts.auth;
    return io(this.opts.url, {
      parser: superjsonParser,
      transports: this.opts.transports ?? ['websocket'],
      withCredentials: this.opts.withCredentials ?? false,
      auth: (cb: (data: Record<string, unknown>) => void) => {
        Promise.resolve(auth ? auth() : {})
          .then((data) => cb(data))
          .catch(() => cb({}));
      },
    });
  }

  private readonly handleConnect = (): void => {
    this.connected = true;
    // Server per-socket state is ephemeral — replay every active sub so it gets a fresh
    // snapshot that the Collection reconciles against whatever it already cached.
    for (const [subId, entry] of this.registry) {
      if (entry.refCount > 0) {
        this.socket.emit('subscribe', { subId, ...entry.spec });
      }
    }
  };

  private readonly handleDisconnect = (): void => {
    this.connected = false;
  };

  private readonly handleEnvelope = (envelope: Envelope): void => {
    this.registry.get(envelope.subId)?.collection.applyEnvelope(envelope);
  };
}

abstract class BaseHandle<R extends Row> {
  protected constructor(
    private readonly client: RealtimeClient<any>,
    protected readonly subId: string,
    protected readonly collection: Collection,
  ) {}

  /** Ref-counted raw subscription to the collection's change stream — the ONE place both
   * `select()` and `onChange()` attach; neither re-diffs. */
  protected bindRaw(handler: (changes: ChangeEvent<R>[]) => void): Unsubscribe {
    this.client.addListener(this.subId);
    const unsubRaw = this.collection.subscribeRaw(handler as (changes: ChangeEvent[]) => void);
    let closed = false;
    return () => {
      if (closed) return;
      closed = true;
      unsubRaw();
      this.client.removeListener(this.subId);
    };
  }

  protected bindListener(fields: string[] | undefined, notify: () => void): Unsubscribe {
    return this.bindRaw((changes) => {
      if (matchesFields(changes, fields)) notify();
    });
  }
}

class LiveQueryHandle<R extends Row> extends BaseHandle<R> implements LiveQuery<R> {
  constructor(client: RealtimeClient<any>, subId: string, collection: Collection) {
    super(client, subId, collection);
  }

  get(): R[] {
    return this.collection.getRows() as R[];
  }

  subscribe(listener: (rows: R[]) => void): Unsubscribe {
    return this.bindListener(undefined, () => listener(this.get()));
  }

  select(fields: string[]): SelectedView<R[]> {
    return {
      get: () => this.get(),
      subscribe: (listener) => this.bindListener(fields, () => listener(this.get())),
    };
  }

  onChange(listener: (changes: ChangeEvent<R>[]) => void): Unsubscribe {
    return this.bindRaw(listener);
  }
}

class LiveDocHandle<R extends Row> extends BaseHandle<R> implements LiveDoc<R> {
  constructor(client: RealtimeClient<any>, subId: string, collection: Collection) {
    super(client, subId, collection);
  }

  get(): R | null {
    const rows = this.collection.getRows();
    return (rows[0] as R | undefined) ?? null;
  }

  subscribe(listener: (row: R | null) => void): Unsubscribe {
    return this.bindListener(undefined, () => listener(this.get()));
  }

  select(fields: string[]): SelectedView<R | null> {
    return {
      get: () => this.get(),
      subscribe: (listener) => this.bindListener(fields, () => listener(this.get())),
    };
  }

  onChange(listener: (changes: ChangeEvent<R>[]) => void): Unsubscribe {
    return this.bindRaw(listener);
  }
}
