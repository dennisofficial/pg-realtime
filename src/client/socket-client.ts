import { io, Socket } from 'socket.io-client';

export interface PgRealtimeClientOptions {
  /** Server URL (optionally including a namespace path). */
  baseURL: string;
  /** Extra handshake auth merged into every subscription (e.g. a token). May be async. */
  auth?:
    | Record<string, unknown>
    | (() => Record<string, unknown> | Promise<Record<string, unknown>>);
  transports?: Array<'websocket' | 'polling'>;
}

export interface Subscription {
  unsubscribe(): void;
}

interface DataEntry {
  pk: string;
  row: Record<string, unknown>;
}

/**
 * Client mirroring the Mongo reference's `onQuery` / `onDocument`. Each
 * subscription opens one socket whose handshake auth carries the spec, then keeps
 * a local `Map<pk,row>` reconciled by `data`/`add`/`update`/`remove` deltas.
 */
export class PgRealtimeClient<Models extends Record<string, any> = Record<string, any>> {
  constructor(private readonly opts: PgRealtimeClientOptions) {}

  /** Subscribe to a filtered query. `onChange` fires with the full result set on every delta. */
  onQuery<N extends keyof Models & string>(
    model: N,
    filter: Record<string, unknown>,
    onChange: (rows: Array<Models[N]>) => void,
    onError?: (err: unknown) => void,
  ): Subscription {
    return this.open({ model, filter }, (cache) =>
      onChange([...cache.values()].map((e) => e.row as Models[N])),
    onError);
  }

  /** Subscribe to a single row by its stringified PK. `onChange` fires with the row or null. */
  onDocument<N extends keyof Models & string>(
    model: N,
    pk: string,
    onChange: (row: Models[N] | null) => void,
    onError?: (err: unknown) => void,
  ): Subscription {
    return this.open({ model, pk }, (cache) => {
      const first = cache.values().next();
      onChange((first.done ? null : (first.value.row as Models[N])) ?? null);
    }, onError);
  }

  private open(
    spec: { model: string; filter?: Record<string, unknown>; pk?: string },
    emit: (cache: Map<string, DataEntry>) => void,
    onError?: (err: unknown) => void,
  ): Subscription {
    const cache = new Map<string, DataEntry>();
    const base = this.opts.auth;

    const socket: Socket = io(this.opts.baseURL, {
      transports: this.opts.transports ?? ['websocket'],
      forceNew: true,
      auth: (cb: (data: Record<string, unknown>) => void) => {
        Promise.resolve(typeof base === 'function' ? base() : base ?? {})
          .then((b) => cb({ ...b, ...spec }))
          .catch(() => cb({ ...spec }));
      },
    });

    socket.on('data', (rows: DataEntry[]) => {
      cache.clear();
      for (const e of rows) cache.set(e.pk, e);
      emit(cache);
    });
    socket.on('add', (e: DataEntry) => {
      cache.set(e.pk, e);
      emit(cache);
    });
    socket.on('update', (e: DataEntry) => {
      cache.set(e.pk, e);
      emit(cache);
    });
    socket.on('remove', (e: { pk: string }) => {
      cache.delete(e.pk);
      emit(cache);
    });
    socket.on('exception', (err: unknown) => onError?.(err));
    socket.on('connect_error', (err: unknown) => onError?.(err));

    return {
      unsubscribe: () => {
        socket.close();
      },
    };
  }
}
