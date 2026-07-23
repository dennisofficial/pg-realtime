import type { Server, Socket } from 'socket.io';
import { RealtimeEngine } from '../engine/engine';
import { Logger, MingoFilter, NOOP_LOGGER, Row, RowDelta, Subscription } from '../types';

export interface Principal {
  [key: string]: unknown;
}

export interface AttachMuxOptions<P = unknown> {
  authenticate: (handshake: Socket['handshake']) => P | Promise<P>;
  resolveResource?: (
    name: string,
    spec: QuerySpec,
    principal: P,
  ) => ComposedResource | null | Promise<ComposedResource | null>;
  logger?: Logger;
}

export interface QuerySpec {
  filter?: MingoFilter;
  sort?: Array<[string, 'asc' | 'desc']>;
  limit?: number;
  offset?: number;
  /** Opaque cursor (app-defined shape) for keyset pagination. */
  after?: unknown;
  pk?: string;
}

export interface ComposedResource {
  triggers: Array<{ model: string; filter?: MingoFilter; pk?: string }>;
  load: () => Promise<Array<{ pk: string; row: Row }>>;
}

/** A closable handle — either a plain engine `Subscription` or a composed multi-trigger group. */
interface ClosableSub {
  close(): void;
}

interface SubscribeMessage {
  subId: string;
  model: string;
  filter?: MingoFilter;
  pk?: string;
  sort?: Array<[string, 'asc' | 'desc']>;
  limit?: number;
  offset?: number;
  /** Opaque cursor (app-defined shape) for keyset pagination. */
  after?: unknown;
}

interface UnsubscribeMessage {
  subId: string;
}

/** Server → client envelope, all carried on the single `'rt'` event. */
export type Envelope =
  | { subId: string; op: 'data'; rows: Array<{ pk: string; row: Record<string, unknown> }> }
  | { subId: string; op: 'add'; pk: string; row: Record<string, unknown> }
  | { subId: string; op: 'update'; pk: string; patch: Record<string, unknown> }
  | { subId: string; op: 'remove'; pk: string }
  | { subId: string; op: 'error'; message: string };

export function projectDelta(subId: string, delta: RowDelta): Envelope | null {
  switch (delta.kind) {
    case 'data':
      return { subId, op: 'data', rows: delta.rows };
    case 'add':
      return { subId, op: 'add', pk: delta.pk, row: delta.row };
    case 'remove':
      return { subId, op: 'remove', pk: delta.pk };
    case 'update': {
      if (delta.changedColumns && delta.changedColumns.length === 0) return null;
      const patch = delta.changedColumns ? pick(delta.row, delta.changedColumns) : delta.row;
      return { subId, op: 'update', pk: delta.pk, patch };
    }
  }
}

function pick(row: Record<string, unknown>, keys: string[]): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const k of keys) out[k] = row[k];
  return out;
}

export function attachMux<P = unknown>(
  io: Server,
  engine: RealtimeEngine,
  opts: AttachMuxOptions<P>,
): void {
  const logger = opts.logger ?? NOOP_LOGGER;

  io.on('connection', (socket: Socket) => {
    void handleConnection(socket, engine, opts, logger);
  });
}

async function handleConnection<P>(
  socket: Socket,
  engine: RealtimeEngine,
  opts: AttachMuxOptions<P>,
  logger: Logger,
): Promise<void> {
  let principal: P;
  try {
    principal = await opts.authenticate(socket.handshake);
  } catch (err) {
    logger.warn(`mux: authentication rejected — ${errMsg(err)}`);
    socket.emit('rt', { subId: '', op: 'error', message: errMsg(err) } satisfies Envelope);
    socket.disconnect(true);
    return;
  }

  const subs = new Map<string, ClosableSub>();

  socket.on('subscribe', (msg: SubscribeMessage) => {
    void (async () => {
      const { subId } = msg;
      try {
        const resource = opts.resolveResource
          ? await opts.resolveResource(
              msg.model,
              {
                filter: msg.filter,
                sort: msg.sort,
                limit: msg.limit,
                offset: msg.offset,
                after: msg.after,
                pk: msg.pk,
              },
              principal,
            )
          : null;

        if (resource) {
          const handle = await openComposedResource(subId, resource, engine, principal, socket, logger);
          if (handle) subs.set(subId, handle);
          return;
        }

        const sub = await engine.openSubscription({
          model: msg.model,
          user: principal,
          filter: msg.filter,
          pk: msg.pk,
        });
        subs.set(subId, sub);
        sub.on((delta) => {
          const envelope = projectDelta(subId, delta);
          if (envelope) socket.emit('rt', envelope);
        });
      } catch (err) {
        logger.debug(`mux subscribe failed (subId=${subId}): ${errMsg(err)}`);
        socket.emit('rt', { subId, op: 'error', message: errMsg(err) } satisfies Envelope);
      }
    })();
  });

  socket.on('unsubscribe', (msg: UnsubscribeMessage) => {
    const sub = subs.get(msg.subId);
    if (!sub) return;
    sub.close();
    subs.delete(msg.subId);
  });

  socket.on('disconnect', () => {
    for (const sub of subs.values()) sub.close();
    subs.clear();
  });
}

/**
 * Opens a composed resource: one trigger `Subscription` per `resource.triggers` entry
 * (change signals only — their deltas are ignored) plus a coalesced `load()` re-run that
 * pushes a fresh ordered `data` snapshot on every trigger firing. Mirrors the
 * `SseSnapshotService.snapshotList` running/pending coalescing pattern.
 */
async function openComposedResource<P>(
  subId: string,
  resource: ComposedResource,
  engine: RealtimeEngine,
  principal: P,
  socket: Socket,
  logger: Logger,
): Promise<ClosableSub | null> {
  const triggerSubs: Subscription[] = [];
  let cancelled = false;
  let running = false;
  let pending = false;

  const emit = async (): Promise<void> => {
    if (running) {
      pending = true;
      return;
    }
    running = true;
    try {
      const rows = await resource.load();
      if (!cancelled) socket.emit('rt', { subId, op: 'data', rows } satisfies Envelope);
    } catch (err) {
      logger.debug(`mux composed resource load failed (subId=${subId}): ${errMsg(err)}`);
      if (!cancelled) socket.emit('rt', { subId, op: 'error', message: errMsg(err) } satisfies Envelope);
    } finally {
      running = false;
      if (pending && !cancelled) {
        pending = false;
        void emit();
      }
    }
  };

  try {
    for (const trigger of resource.triggers) {
      const triggerSub = await engine.openSubscription({
        model: trigger.model,
        user: principal,
        filter: trigger.filter,
        pk: trigger.pk,
      });
      if (cancelled) {
        triggerSub.close();
        return null;
      }
      triggerSubs.push(triggerSub);
      triggerSub.on(() => void emit());
    }
  } catch (err) {
    for (const triggerSub of triggerSubs) triggerSub.close();
    logger.debug(`mux composed resource open failed (subId=${subId}): ${errMsg(err)}`);
    socket.emit('rt', { subId, op: 'error', message: errMsg(err) } satisfies Envelope);
    return null;
  }

  void emit();

  return {
    close(): void {
      cancelled = true;
      for (const triggerSub of triggerSubs) triggerSub.close();
    },
  };
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
