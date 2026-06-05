import {
  fetchEventSource,
  type EventSourceMessage,
  type FetchEventSourceInit,
} from '@microsoft/fetch-event-source';
import { MingoFilter } from '../types';

/**
 * Browser/Node SSE client mirroring the socket.io client's `onQuery`/`onDocument`,
 * built on `@microsoft/fetch-event-source` (fetch-based, not raw EventSource) so it
 * can carry auth headers, survive tab-hidden, and reconnect with backoff — the same
 * production posture as cubix's `fetchAuthenticatedEventSource`.
 *
 * The subscription spec rides in query params (the request is a GET); auth is via
 * cookies (`credentials`) and/or a `headers` factory refreshed per (re)connect. Keeps
 * a `Map<pk,row>` reconciled by the `data`/`add`/`update`/`remove` events.
 */
class FatalSseError extends Error {}
class RetriableSseError extends Error {}

const BASE_RETRY_MS = 1000;
const MAX_RETRY_MS = 30_000;
const JITTER_MS = 500;

export interface SseClientOptions {
  /** SSE endpoint base URL; the subscription spec is appended as query params. */
  baseURL: string;
  /**
   * Headers for each (re)connect — a static record, or an (async) factory so a token
   * can be refreshed per attempt. Applied through a custom fetch so retries pick up
   * fresh values.
   */
  headers?:
    | Record<string, string>
    | (() => Record<string, string> | Promise<Record<string, string>>);
  /** fetch credentials mode for cookie auth. Default 'include'. */
  credentials?: 'include' | 'omit' | 'same-origin';
  /** Keep streaming while the tab is hidden. Default true. */
  openWhenHidden?: boolean;
}

export interface SseSubscription {
  unsubscribe(): void;
}

export class SseClient<Models extends Record<string, any> = Record<string, any>> {
  constructor(private readonly opts: SseClientOptions) {}

  onQuery<N extends keyof Models & string>(
    model: N,
    filter: MingoFilter,
    onChange: (rows: Array<Models[N]>) => void,
    onError?: (e: unknown) => void,
  ): SseSubscription {
    return this.open(
      { model, filter },
      (cache) => onChange([...cache.values()] as Array<Models[N]>),
      onError,
    );
  }

  onDocument<N extends keyof Models & string>(
    model: N,
    pk: string | unknown[],
    onChange: (row: Models[N] | null) => void,
    onError?: (e: unknown) => void,
  ): SseSubscription {
    return this.open(
      { model, pk },
      (cache) => {
        const first = cache.values().next();
        onChange((first.done ? null : (first.value as Models[N])) ?? null);
      },
      onError,
    );
  }

  private open(
    spec: { model: string; filter?: MingoFilter; pk?: string | unknown[] },
    emit: (cache: Map<string, unknown>) => void,
    onError?: (e: unknown) => void,
  ): SseSubscription {
    const cache = new Map<string, unknown>();
    const controller = new AbortController();
    const url = this.buildUrl(spec);
    let retryAttempt = 0;

    const headerSource = this.opts.headers;
    const customFetch: FetchEventSourceInit['fetch'] | undefined = headerSource
      ? async (input, init) => {
          const dynamic =
            typeof headerSource === 'function' ? await headerSource() : headerSource;
          return fetch(input, {
            ...init,
            headers: { ...(init?.headers as Record<string, string>), ...dynamic },
          });
        }
      : undefined;

    void fetchEventSource(url, {
      signal: controller.signal,
      credentials: this.opts.credentials ?? 'include',
      openWhenHidden: this.opts.openWhenHidden ?? true,
      ...(customFetch ? { fetch: customFetch } : {}),
      onopen: async (response) => {
        if (response.ok) {
          retryAttempt = 0;
          return;
        }
        // 4xx is the client's fault — retrying won't help. 5xx / network is transient.
        if (response.status >= 400 && response.status < 500) {
          throw new FatalSseError(`SSE stream rejected with ${response.status}`);
        }
        throw new RetriableSseError(`SSE stream failed with ${response.status}`);
      },
      onmessage: (ev) => this.dispatch(ev, cache, emit),
      onerror: (err) => {
        onError?.(err);
        if (err instanceof FatalSseError) throw err; // stop retrying
        const delay =
          Math.min(MAX_RETRY_MS, BASE_RETRY_MS * 2 ** retryAttempt) +
          Math.floor(Math.random() * JITTER_MS);
        retryAttempt += 1;
        return delay; // retry after delay
      },
      onclose: () => {
        // The server closed the stream — reconnect (the engine is the source of truth).
        throw new RetriableSseError('SSE stream closed');
      },
    }).catch((err: unknown) => {
      if (err instanceof FatalSseError) return;
      if (isAbort(err)) return;
      onError?.(err);
    });

    return { unsubscribe: () => controller.abort() };
  }

  private dispatch(
    ev: EventSourceMessage,
    cache: Map<string, unknown>,
    emit: (cache: Map<string, unknown>) => void,
  ): void {
    if (!ev.data) return;
    switch (ev.event) {
      case 'data': {
        const rows = JSON.parse(ev.data) as Array<{ pk: string; row: unknown }>;
        cache.clear();
        for (const r of rows) cache.set(r.pk, r.row);
        emit(cache);
        break;
      }
      case 'add':
      case 'update': {
        const { pk, row } = JSON.parse(ev.data) as { pk: string; row: unknown };
        cache.set(pk, row);
        emit(cache);
        break;
      }
      case 'remove': {
        const { pk } = JSON.parse(ev.data) as { pk: string };
        cache.delete(pk);
        emit(cache);
        break;
      }
    }
  }

  private buildUrl(spec: { model: string; filter?: MingoFilter; pk?: string | unknown[] }): string {
    const url = new URL(this.opts.baseURL);
    url.searchParams.set('model', spec.model);
    if (spec.filter) url.searchParams.set('filter', JSON.stringify(spec.filter));
    if (spec.pk !== undefined) {
      url.searchParams.set('pk', typeof spec.pk === 'string' ? spec.pk : JSON.stringify(spec.pk));
    }
    return url.toString();
  }
}

function isAbort(err: unknown): boolean {
  return !!err && typeof err === 'object' && (err as { name?: string }).name === 'AbortError';
}
