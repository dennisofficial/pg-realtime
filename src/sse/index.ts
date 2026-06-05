import { RowDelta, Subscription } from '../types';

/**
 * Server-Sent Events transport (server side), framework-agnostic.
 *
 * Each `RowDelta` becomes one SSE event: the event name is the delta kind
 * (`data`/`add`/`update`/`remove`) and the `data:` line is the JSON payload — the
 * same vocabulary as the socket.io transport. For NestJS `@Sse()`, use
 * `sseObservable` from `pg-realtime/nest` instead; this module is for raw
 * `http.ServerResponse`, Express, Fastify, or framework route handlers.
 */

/** The JSON payload carried by an SSE event for a given delta. */
export function ssePayload(delta: RowDelta): unknown {
  switch (delta.kind) {
    case 'data':
      return delta.rows;
    case 'remove':
      return { pk: delta.pk };
    default:
      return { pk: delta.pk, row: delta.row };
  }
}

/** Format a delta as an SSE event block (`event: <kind>\ndata: <json>\n\n`). */
export function formatSse(delta: RowDelta): string {
  return `event: ${delta.kind}\ndata: ${JSON.stringify(ssePayload(delta))}\n\n`;
}

/** Anything we can write SSE text to — `http.ServerResponse`, Express `res`, etc. */
export interface SseSink {
  write(chunk: string): unknown;
}

export interface PipeToSseOptions {
  /** Heartbeat comment interval (ms) to keep idle connections + proxies alive. 0 disables. Default 25000. */
  heartbeatMs?: number;
  /** Called once when the stream stops (write error or `cleanup()`), after `sub.close()`. */
  onClose?: () => void;
}

/**
 * Stream a subscription's deltas to an SSE sink. Attaching the handler starts the
 * snapshot (so the first event is always `data`). Returns a `cleanup` you MUST call
 * on client disconnect — e.g. `res.on('close', cleanup)` — to close the subscription
 * and stop the heartbeat.
 *
 * The caller is responsible for the SSE response headers before calling this:
 *   Content-Type: text/event-stream
 *   Cache-Control: no-cache, no-transform
 *   Connection: keep-alive
 */
export function pipeToSse(
  sub: Subscription,
  sink: SseSink,
  opts: PipeToSseOptions = {},
): () => void {
  let closed = false;
  let timer: ReturnType<typeof setInterval> | undefined;

  const cleanup = (): void => {
    if (closed) return;
    closed = true;
    if (timer) clearInterval(timer);
    sub.close();
    opts.onClose?.();
  };

  sub.on((delta) => {
    if (closed) return;
    try {
      sink.write(formatSse(delta));
    } catch {
      cleanup();
    }
  });

  const heartbeat = opts.heartbeatMs ?? 25_000;
  if (heartbeat > 0) {
    timer = setInterval(() => {
      if (closed) return;
      try {
        sink.write(': ping\n\n');
      } catch {
        cleanup();
      }
    }, heartbeat);
    // Don't keep the process alive just for the heartbeat.
    (timer as { unref?: () => void }).unref?.();
  }

  return cleanup;
}
