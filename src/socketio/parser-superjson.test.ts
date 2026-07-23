import { Decoder, Encoder, SuperjsonPacket } from './parser-superjson';

describe('superjson socket.io parser', () => {
  it('round-trips Date and BigInt values through encode/decode', () => {
    const encoder = new Encoder();
    const decoder = new Decoder();

    const originalDate = new Date('2026-07-23T12:00:00.000Z');
    const originalBigInt = 9007199254740993n;

    const packet: SuperjsonPacket = {
      type: 2,
      nsp: '/',
      data: ['event', { createdAt: originalDate, count: originalBigInt }],
      id: 1,
    };

    const decoded = new Promise<SuperjsonPacket>((resolve) => {
      decoder.on('decoded', (packet) => resolve(packet as SuperjsonPacket));
    });

    const encoded = encoder.encode(packet);
    expect(encoded).toHaveLength(1);
    expect(typeof encoded[0]).toBe('string');

    decoder.add(encoded[0]);

    return decoded.then((result) => {
      expect(result.type).toBe(packet.type);
      expect(result.nsp).toBe(packet.nsp);
      expect(result.id).toBe(packet.id);

      const [event, payload] = result.data as [string, { createdAt: Date; count: bigint }];
      expect(event).toBe('event');
      expect(payload.createdAt).toBeInstanceOf(Date);
      expect(payload.createdAt.getTime()).toBe(originalDate.getTime());
      expect(typeof payload.count).toBe('bigint');
      expect(payload.count).toBe(originalBigInt);
    });
  });

  // socket.io's Client.destroy() calls decoder.removeListener('decoded', ...) on EVERY teardown —
  // if the Decoder lacks it, any socket disconnect throws and crashes the server process.
  it('exposes removeListener (socket.io teardown) that detaches the listener', () => {
    const decoder = new Decoder();
    const listener = jest.fn();
    decoder.on('decoded', listener);
    expect(typeof decoder.removeListener).toBe('function');
    decoder.removeListener('decoded', listener);

    const [chunk] = new Encoder().encode({ type: 2, nsp: '/', data: [] });
    decoder.add(chunk);
    expect(listener).not.toHaveBeenCalled();
  });

  it('destroy() clears listeners so no further decode events fire', () => {
    const decoder = new Decoder();
    const listener = jest.fn();
    decoder.on('decoded', listener);
    decoder.destroy();

    const encoder = new Encoder();
    const [chunk] = encoder.encode({ type: 2, nsp: '/', data: [] });
    decoder.add(chunk);

    expect(listener).not.toHaveBeenCalled();
  });
});
