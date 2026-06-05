import { Observable } from 'rxjs';
import { ssePayload } from '../sse';
import { Subscription } from '../types';

/** Structural match for NestJS's `MessageEvent` (so we needn't import it just for a type). */
export interface SseMessageEvent {
  data: string | object;
  id?: string;
  type?: string;
  retry?: number;
}

/**
 * Adapt a pg-realtime `Subscription` to an `Observable<MessageEvent>` for a NestJS
 * `@Sse()` handler. `openSubscription` is async, so pass a factory: the subscription
 * opens when the client connects (the Observable is subscribed) and closes when they
 * disconnect (it is unsubscribed). Each delta becomes a NestJS SSE event whose name is
 * the delta kind.
 *
 *   @Sse('servers/realtime')
 *   stream(@GetUser() user: AuthUser, @Query('filter') filter?: string): Observable<MessageEvent> {
 *     return sseObservable(() =>
 *       this.engine.openSubscription({
 *         model: 'servers',
 *         user,
 *         filter: filter ? JSON.parse(filter) : undefined,
 *       }),
 *     );
 *   }
 */
export function sseObservable(open: () => Promise<Subscription>): Observable<SseMessageEvent> {
  return new Observable<SseMessageEvent>((subscriber) => {
    let sub: Subscription | undefined;
    let cancelled = false;

    open().then(
      (opened) => {
        if (cancelled) {
          opened.close();
          return;
        }
        sub = opened;
        opened.on((delta) => {
          subscriber.next({ type: delta.kind, data: ssePayload(delta) as object });
        });
      },
      (err) => subscriber.error(err),
    );

    return () => {
      cancelled = true;
      sub?.close();
    };
  });
}
