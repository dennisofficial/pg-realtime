import { diffColumns } from './diff';

describe('diffColumns', () => {
  it('returns keys with differing primitive values', () => {
    expect(diffColumns({ a: 1, b: 'x' }, { a: 1, b: 'y' })).toEqual(['b']);
  });

  it('returns the union of keys, including added/removed keys', () => {
    expect(diffColumns({ a: 1 }, { a: 1, b: 2 })).toEqual(['b']);
    expect(diffColumns({ a: 1, b: 2 }, { a: 1 })).toEqual(['b']);
  });

  it('treats equal Dates as unchanged, differing Dates as changed', () => {
    const d1 = new Date('2024-01-01T00:00:00Z');
    const d2 = new Date('2024-01-01T00:00:00Z');
    const d3 = new Date('2024-01-02T00:00:00Z');
    expect(diffColumns({ a: d1 }, { a: d2 })).toEqual([]);
    expect(diffColumns({ a: d1 }, { a: d3 })).toEqual(['a']);
  });

  it('byte-compares Buffers/Uint8Arrays', () => {
    expect(diffColumns({ a: Buffer.from([1, 2, 3]) }, { a: Buffer.from([1, 2, 3]) })).toEqual([]);
    expect(diffColumns({ a: Buffer.from([1, 2, 3]) }, { a: Buffer.from([1, 2, 4]) })).toEqual(['a']);
    expect(
      diffColumns({ a: new Uint8Array([1, 2]) }, { a: Buffer.from([1, 2]) }),
    ).toEqual([]);
  });

  it('handles null/undefined correctly', () => {
    expect(diffColumns({ a: null }, { a: null })).toEqual([]);
    expect(diffColumns({ a: undefined }, { a: undefined })).toEqual([]);
    // `undefined` is the unchanged-TOAST placeholder (see toast.ts); it is NOT
    // the same value as a genuine SQL NULL, so this is a change.
    expect(diffColumns({ a: null }, { a: undefined })).toEqual(['a']);
    expect(diffColumns({ a: null }, { a: 1 })).toEqual(['a']);
  });

  it('deep-compares objects and arrays regardless of key order', () => {
    expect(diffColumns({ a: { x: 1, y: 2 } }, { a: { y: 2, x: 1 } })).toEqual([]);
    expect(diffColumns({ a: { x: 1 } }, { a: { x: 2 } })).toEqual(['a']);
    expect(diffColumns({ a: [1, 2, 3] }, { a: [1, 2, 3] })).toEqual([]);
    expect(diffColumns({ a: [1, 2, 3] }, { a: [1, 3, 2] })).toEqual(['a']);
  });

  it('returns no changes for identical objects', () => {
    expect(diffColumns({ a: 1, b: 'x', c: null }, { a: 1, b: 'x', c: null })).toEqual([]);
  });
});
