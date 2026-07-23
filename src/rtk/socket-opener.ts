/**
 * Adapts a `RealtimeClient` (one multiplexed socket.io connection, see `../client`) into
 * the `SseOpener` shape `streamList`/`streamDocument` already consume — so RTK Query's
 * `onCacheEntryAdded` can drive off the socket instead of a per-endpoint SSE stream,
 * with zero changes to those two helpers.
 *
 * Usage (one app-singleton `RealtimeClient`, reused by every endpoint):
 *
 *   const client = new RealtimeClient<Models>({ url: SOCKET_URL, auth: getAuthHeader });
 *
 *   const api = createApi({
 *     endpoints: (build) => ({
 *       getThreads: build.query<Thread[], { status?: string }>({
 *         query: (arg) => ({ url: '/threads', params: arg }),
 *         onCacheEntryAdded: (arg, api) =>
 *           streamList({
 *             url: 'threads',
 *             open: makeSocketListOpener(client, 'threads', { filter: arg }),
 *             lifecycle: api,
 *           }),
 *       }),
 *       getThread: build.query<Thread, string>({
 *         query: (id) => ({ url: `/threads/${id}` }),
 *         onCacheEntryAdded: (id, api) =>
 *           streamDocument({
 *             url: `threads/${id}`,
 *             open: makeSocketDocumentOpener(client, 'threads', id),
 *             lifecycle: api,
 *           }),
 *       }),
 *     }),
 *   });
 *
 * On the component side, prefer `useGetThreadsQuery(arg, { selectFromResult })` to narrow
 * re-renders to the fields a component actually reads — the client already ships minimal
 * patches (`changedColumns`) end-to-end, but RTK Query's default hook re-renders on ANY
 * cache change; `selectFromResult` is what turns that into field-level memoization on the
 * React side. E.g.:
 *
 *   const { status } = useGetThreadQuery(id, {
 *     selectFromResult: ({ data }) => ({ status: data?.status }),
 *   });
 *
 * `url` is not used to reach the network here (the socket is already connected and
 * multiplexed by `subId`) — it's kept only because `streamList`/`streamDocument` require
 * one; pass any stable label (e.g. the model name).
 */

import type { RealtimeClient } from '../client/realtime-client';
import type { SseMessage, SseOpener } from './index';

/** Column(s) identifying a row. Kept for API compatibility with callers shaping the
 * `{ pk, row }` payloads `streamList`/`streamDocument` expect — the pk itself is no longer
 * computed locally (see `makeSocketListOpener`): it comes straight off each `ChangeEvent`,
 * which already carries the server-assigned pk. */
export type PkSpec = string | string[];

/**
 * List mode: wraps `client.query(model, { filter }).onChange(...)` — the client's own
 * per-change stream (the same one `select()` filters on), so this never re-diffs an array.
 * The initial snapshot arrives as a batch of `add` changes (see `Collection.applyData`);
 * every change after that forwards 1:1 as an `add`/`update`/`remove` `SseMessage`.
 */
export function makeSocketListOpener<
  Models extends Record<string, any>,
  N extends keyof Models & string,
>(
  client: RealtimeClient<Models>,
  model: N,
  args: {
    filter?: Record<string, unknown>;
    pk?: PkSpec;
    sort?: Array<[string, 'asc' | 'desc']>;
    limit?: number;
    offset?: number;
    /** Opaque cursor (app-defined shape) for keyset pagination. */
    after?: unknown;
  } = {},
): SseOpener {
  return (_url, init) => {
    const query = client.query(model, {
      filter: args.filter,
      sort: args.sort,
      limit: args.limit,
      offset: args.offset,
      after: args.after,
    });
    let sawFirst = false;

    const emit = (msg: SseMessage) => init.onmessage(msg);

    const unsubscribe = query.onChange((changes) => {
      try {
        if (!sawFirst) {
          sawFirst = true;
          const entries = changes
            .filter((c) => c.op === 'add')
            .map((c) => ({ pk: c.pk, row: c.row }));
          emit({ event: 'data', data: JSON.stringify(entries) });
          return;
        }

        for (const change of changes) {
          if (change.op === 'add' || change.op === 'update') {
            emit({ event: change.op, data: JSON.stringify({ pk: change.pk, row: change.row }) });
          } else {
            emit({ event: 'remove', data: JSON.stringify({ pk: change.pk }) });
          }
        }
      } catch (err) {
        init.onerror?.(err);
      }
    });

    return new Promise<void>((resolve) => {
      const cleanup = () => {
        unsubscribe();
        resolve();
      };
      if (init.signal.aborted) {
        cleanup();
        return;
      }
      init.signal.addEventListener('abort', cleanup, { once: true });
    });
  };
}

/**
 * Document mode: wraps `client.document(model, pk).onChange(...)`. The initial snapshot
 * (0 or 1 row) arrives as at most one `add` change; every change after that forwards 1:1 as
 * an `add`/`update`/`remove` `SseMessage` matching `streamDocument`'s reducer — no local
 * `hadRow` bookkeeping needed, the op is already on the event.
 */
export function makeSocketDocumentOpener<
  Models extends Record<string, any>,
  N extends keyof Models & string,
>(client: RealtimeClient<Models>, model: N, pk: string): SseOpener {
  return (_url, init) => {
    const doc = client.document(model, pk);

    const emit = (msg: SseMessage) => init.onmessage(msg);

    const unsubscribe = doc.onChange((changes) => {
      try {
        for (const change of changes) {
          if (change.op === 'add' || change.op === 'update') {
            emit({ event: change.op, data: JSON.stringify({ pk: change.pk, row: change.row }) });
          } else {
            emit({ event: 'remove', data: JSON.stringify({ pk: change.pk }) });
          }
        }
      } catch (err) {
        init.onerror?.(err);
      }
    });

    return new Promise<void>((resolve) => {
      const cleanup = () => {
        unsubscribe();
        resolve();
      };
      if (init.signal.aborted) {
        cleanup();
        return;
      }
      init.signal.addEventListener('abort', cleanup, { once: true });
    });
  };
}
