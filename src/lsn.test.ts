import { lsnGt, lsnGte, parseLsn } from './lsn';

describe('lsn', () => {
  it('parses XXXX/YYYY into a comparable bigint', () => {
    expect(parseLsn('0/00000000')).toBe(0n);
    expect(parseLsn('0/0000000A')).toBe(10n);
    expect(parseLsn('1/00000000')).toBe(1n << 32n);
  });

  it('compares numerically, not lexically', () => {
    // '10/0' < '9/0' lexically, but 10/0 > 9/0 numerically.
    expect(lsnGt('10/00000000', '9/00000000')).toBe(true);
    expect(lsnGt('9/00000000', '10/00000000')).toBe(false);
  });

  it('orders within the same high half', () => {
    expect(lsnGt('0/000000FF', '0/000000FE')).toBe(true);
    expect(lsnGt('0/000000FE', '0/000000FF')).toBe(false);
  });

  it('lsnGte includes equality', () => {
    expect(lsnGte('0/10', '0/10')).toBe(true);
    expect(lsnGt('0/10', '0/10')).toBe(false);
  });
});
