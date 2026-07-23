import { Row } from '../types';

/**
 * Returns the keys (union of both objects' keys) whose values differ between
 * `a` and `b`. Used to compute `changedColumns` on the MAPPED row shapes (not
 * raw WAL columns) — see matcher.ts — so renamed/derived fields diff correctly.
 */
export function diffColumns(a: Row, b: Row): string[] {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  const changed: string[] = [];
  for (const key of keys) {
    if (!valuesEqual(a[key], b[key])) changed.push(key);
  }
  return changed;
}

function valuesEqual(x: unknown, y: unknown): boolean {
  if (x === y) return true;

  if (x == null || y == null) return x === y;

  if (x instanceof Date || y instanceof Date) {
    if (!(x instanceof Date) || !(y instanceof Date)) return false;
    return x.getTime() === y.getTime();
  }

  if (isBinary(x) || isBinary(y)) {
    if (!isBinary(x) || !isBinary(y)) return false;
    return bytesEqual(x, y);
  }

  if (typeof x === 'object' && typeof y === 'object') {
    return deepEqual(x, y);
  }

  return false;
}

function isBinary(v: unknown): v is Uint8Array {
  return v instanceof Uint8Array; // Buffer extends Uint8Array
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

/** Small recursive deep-equal for plain objects/arrays (order-sensitive for arrays). */
function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a == null || b == null) return a === b;
  if (typeof a !== 'object' || typeof b !== 'object') return false;

  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b)) return false;
    if (a.length !== b.length) return false;
    return a.every((v, i) => valuesEqual(v, b[i]));
  }

  const aKeys = Object.keys(a as Record<string, unknown>);
  const bKeys = Object.keys(b as Record<string, unknown>);
  if (aKeys.length !== bKeys.length) return false;
  return aKeys.every((k) =>
    valuesEqual((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]),
  );
}
