/**
 * Postgres LSNs arrive from `pg-logical-replication` as `XXXX/YYYY` hex strings
 * (high/low 32-bit halves). They MUST be compared numerically — lexical string
 * comparison is wrong (`'10/0' < '9/0'` lexically but `>` numerically).
 *
 * The snapshot↔stream consistency proof (see `snapshot.ts`) hinges on comparing
 * a change's commit LSN against the LSN captured at snapshot time, so this is
 * load-bearing.
 */

export function parseLsn(lsn: string): bigint {
  const slash = lsn.indexOf('/');
  if (slash === -1) {
    // Some code paths use the all-zero sentinel '0/00000000' or a bare number.
    return BigInt(`0x${lsn}`);
  }
  const hi = BigInt(`0x${lsn.slice(0, slash)}`);
  const lo = BigInt(`0x${lsn.slice(slash + 1)}`);
  // BigInt(32), not the `32n` literal — literals need an ES2020 compile *target*, and this file is
  // typechecked from source by ES2017-target consumers (e.g. the web app). The bigint type is fine.
  return (hi << BigInt(32)) | lo;
}

export function lsnGt(a: string, b: string): boolean {
  return parseLsn(a) > parseLsn(b);
}

export function lsnGte(a: string, b: string): boolean {
  return parseLsn(a) >= parseLsn(b);
}

export const ZERO_LSN = '0/00000000';
