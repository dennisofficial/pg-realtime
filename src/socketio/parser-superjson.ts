import superjson from 'superjson';

/** Minimal packet shape accepted by socket.io-parser's Encoder/Decoder contract. */
export interface SuperjsonPacket {
  type: number;
  nsp: string;
  data?: unknown;
  id?: number;
}

type Listener = (...args: unknown[]) => void;

class MiniEmitter {
  private readonly listeners = new Map<string, Set<Listener>>();

  on(event: string, listener: Listener): this {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    set.add(listener);
    return this;
  }

  off(event: string, listener: Listener): this {
    this.listeners.get(event)?.delete(listener);
    return this;
  }

  // socket.io's Client.destroy() calls decoder.removeListener('decoded', ...) on EVERY teardown —
  // without this alias, any socket disconnect throws "removeListener is not a function" and crashes.
  removeListener(event: string, listener: Listener): this {
    return this.off(event, listener);
  }

  emit(event: string, ...args: unknown[]): boolean {
    const set = this.listeners.get(event);
    if (!set || set.size === 0) return false;
    for (const listener of [...set]) listener(...args);
    return true;
  }

  removeAllListeners(): this {
    this.listeners.clear();
    return this;
  }
}

export class Encoder {
  encode(packet: SuperjsonPacket): Array<string | Buffer> {
    return [superjson.stringify(packet)];
  }
}

export class Decoder extends MiniEmitter {
  add(chunk: unknown): void {
    if (typeof chunk !== 'string') {
      throw new Error('superjson parser: unsupported binary chunk (expected string)');
    }
    const packet = superjson.parse(chunk) as SuperjsonPacket;
    this.emit('decoded', packet);
  }

  destroy(): void {
    this.removeAllListeners();
  }
}

/** Parser object shape expected by `new Server(http, { parser })` / `io(url, { parser })`. */
export const superjsonParser = { Encoder, Decoder };
