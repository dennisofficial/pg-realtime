import { isToastIncomplete } from './toast';

describe('isToastIncomplete', () => {
  it('is false when all columns have values (including real NULLs)', () => {
    expect(isToastIncomplete({ id: 'a', status: 'active', blob: 'x' })).toBe(false);
    // A genuine SQL NULL parses to `null`, NOT `undefined` — that is complete.
    expect(isToastIncomplete({ id: 'a', status: null })).toBe(false);
  });

  it('is true when a column came through as the unchanged-TOAST placeholder (undefined)', () => {
    expect(isToastIncomplete({ id: 'a', status: 'active', blob: undefined })).toBe(true);
  });

  it('is false for null/empty rows', () => {
    expect(isToastIncomplete(null)).toBe(false);
    expect(isToastIncomplete({})).toBe(false);
  });
});
