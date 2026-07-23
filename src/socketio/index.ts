import type { Server, Socket } from 'socket.io';
import { RealtimeEngine } from '../engine/engine';

export { Decoder, Encoder, superjsonParser } from './parser-superjson';
export type { SuperjsonPacket } from './parser-superjson';
export { attachMux, projectDelta } from './mux';
export type { AttachMuxOptions, ComposedResource, Envelope, Principal, QuerySpec } from './mux';

export interface AttachSocketIOOptions {
  /** Derive the principal passed to the model guard. Default: `handshake.auth.user`. */
  extractUser?: (socket: Socket) => unknown | Promise<unknown>;
}

interface HandshakeAuth {
  model?: string;
  filter?: Record<string, unknown>;
  pk?: string;
  user?: unknown;
}

export function attachSocketIO(
  io: Server,
  engine: RealtimeEngine,
  opts: AttachSocketIOOptions = {},
): void {
  io.on('connection', (socket: Socket) => {
    void handleConnection(socket, engine, opts);
  });
}

async function handleConnection(
  socket: Socket,
  engine: RealtimeEngine,
  opts: AttachSocketIOOptions,
): Promise<void> {
  const auth = (socket.handshake.auth ?? {}) as HandshakeAuth;
  try {
    if (!auth.model) throw new Error('missing handshake auth: model');
    const user = opts.extractUser ? await opts.extractUser(socket) : (auth.user ?? null);
    const sub = await engine.openSubscription({
      model: auth.model,
      user,
      filter: auth.filter,
      pk: auth.pk,
    });

    socket.on('disconnect', () => sub.close());

    sub.on((delta) => {
      switch (delta.kind) {
        case 'data':
          socket.emit('data', delta.rows);
          break;
        case 'add':
          socket.emit('add', { pk: delta.pk, row: delta.row });
          break;
        case 'update':
          socket.emit('update', { pk: delta.pk, row: delta.row });
          break;
        case 'remove':
          socket.emit('remove', { pk: delta.pk });
          break;
      }
    });
  } catch (err) {
    socket.emit('exception', { message: err instanceof Error ? err.message : String(err) });
    socket.disconnect(true);
  }
}
