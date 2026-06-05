/**
 * RTK Query helpers for consuming a pg-realtime SSE endpoint.
 *
 * Collapses the repetitive `onCacheEntryAdded` boilerplate — AbortController,
 * awaiting `cacheDataLoaded`, the `data`/`add`/`update`/`remove` reconciliation, and
 * cleanup on `cacheEntryRemoved` — into one call.
 *
 * Browser-safe and dependency-free: you pass your own SSE opener (e.g. an auth-aware
 * `@microsoft/fetch-event-source` wrapper), so this stays decoupled from how your app
 * authenticates. Import only this subpath in a frontend — it pulls no Node code.
 *
 *   getNode: build.query<INode, string>({
 *     query: (id) => ({ url: `/nodes/${id}` }),
 *     onCacheEntryAdded: (id, api) =>
 *       streamDocument({ url: `${BASE}/nodes/${id}/realtime`, open: fetchAuthenticatedEventSource, lifecycle: api }),
 *   }),
 *   getNodes: build.query<INode[], void>({
 *     query: () => ({ url: '/nodes' }),
 *     onCacheEntryAdded: (_arg, api) =>
 *       streamList({ url: `${BASE}/nodes/realtime`, open: fetchAuthenticatedEventSource, lifecycle: api }),
 *   }),
 */

/** A single SSE message — the subset of `@microsoft/fetch-event-source`'s EventSourceMessage used here. */
export interface SseMessage {
  event?: string;
  data: string;
}

/** Opens an SSE stream and resolves when it ends. Pass your auth-aware `fetchEventSource`. */
export type SseOpener = (
  url: string,
  init: {
    signal: AbortSignal;
    openWhenHidden?: boolean;
    onmessage: (ev: SseMessage) => void;
    onerror?: (err: unknown) => void;
  },
) => Promise<void>;

/** The subset of RTK Query's `onCacheEntryAdded` lifecycle API the helpers use. */
export interface CacheLifecycle<T> {
  cacheDataLoaded: Promise<unknown>;
  cacheEntryRemoved: Promise<void>;
  updateCachedData: (recipe: (draft: T) => T | void) => void;
}

interface BaseArgs<T> {
  url: string;
  open: SseOpener;
  lifecycle: CacheLifecycle<T>;
  /** Keep streaming while the tab is hidden. Default true. */
  openWhenHidden?: boolean;
  onError?: (err: unknown) => void;
}

/** Run the cache-entry lifecycle around an SSE consumer (shared by both helpers). */
async function run<T>(args: BaseArgs<T>, onmessage: (ev: SseMessage) => void): Promise<void> {
  const controller = new AbortController();
  let streamPromise: Promise<void> | undefined;
  try {
    await args.lifecycle.cacheDataLoaded;
    streamPromise = args.open(args.url, {
      signal: controller.signal,
      openWhenHidden: args.openWhenHidden ?? true,
      onmessage,
      onerror: args.onError,
    });
    await args.lifecycle.cacheEntryRemoved;
  } finally {
    controller.abort();
    await streamPromise?.catch(() => undefined);
  }
}

/**
 * Document mode: the cache holds a single row. `data` (snapshot, 0–1 rows) and
 * `add`/`update` set it; `remove` leaves the cached value by default (the detail view
 * handles absence) — override via `onRemove`.
 */
export function streamDocument<T>(
  args: BaseArgs<T> & {
    /** Called on a `remove` delta. Default: keep the cached value. */
    onRemove?: (update: CacheLifecycle<T>['updateCachedData']) => void;
  },
): Promise<void> {
  const { updateCachedData } = args.lifecycle;
  return run<T>(args, (ev) => {
    if (!ev.data) return;
    if (ev.event === 'data') {
      const rows = JSON.parse(ev.data) as Array<{ pk: string; row: T }>;
      if (rows[0]) updateCachedData(() => rows[0].row);
    } else if (ev.event === 'add' || ev.event === 'update') {
      const { row } = JSON.parse(ev.data) as { pk: string; row: T };
      updateCachedData(() => row);
    } else if (ev.event === 'remove') {
      args.onRemove?.(updateCachedData);
    }
  });
}

/**
 * List mode: the cache holds an array. An internal `Map<pk,row>` is reconciled by
 * `data`/`add`/`update`/`remove` and projected to the array on each delta — preserving
 * row identity (memoized items don't needlessly re-render) and ordering (snapshot order,
 * with `add` appending). Handles composite primary keys, since it keys by the engine pk.
 */
export function streamList<T>(args: BaseArgs<T[]>): Promise<void> {
  const { updateCachedData } = args.lifecycle;
  const cache = new Map<string, T>();
  const project = () => updateCachedData(() => [...cache.values()]);
  return run<T[]>(args, (ev) => {
    if (!ev.data) return;
    if (ev.event === 'data') {
      const rows = JSON.parse(ev.data) as Array<{ pk: string; row: T }>;
      cache.clear();
      for (const e of rows) cache.set(e.pk, e.row);
      project();
    } else if (ev.event === 'add' || ev.event === 'update') {
      const { pk, row } = JSON.parse(ev.data) as { pk: string; row: T };
      cache.set(pk, row);
      project();
    } else if (ev.event === 'remove') {
      const { pk } = JSON.parse(ev.data) as { pk: string };
      if (cache.delete(pk)) project();
    }
  });
}
