/**
 * Two projects:
 *  - "unit"        — fast, no external services. Colocated `src/**\/*.test.ts`.
 *  - "integration" — requires the scratch Postgres from docker-compose.test.yml
 *                    (and optionally Redis). `test/integration/**\/*.test.ts`.
 *
 * `pnpm test` runs unit only (safe without docker); `pnpm test:integration`
 * runs the docker-backed suite (in-band, since they share one slot/publication).
 */
const base = {
  testEnvironment: 'node',
  transform: { '^.+\\.(t|j)s$': ['@swc/jest'] },
  moduleFileExtensions: ['ts', 'js', 'json'],
  // `superjson` (and its transitive deps `copy-anything`/`is-what`) ship
  // ESM-only (no CJS build) — let swc transform them too, instead of the
  // default "skip all of node_modules" behavior. `.*` before the package
  // name accounts for pnpm's nested `node_modules/.pnpm/<pkg>@.../node_modules/<pkg>` layout.
  transformIgnorePatterns: ['node_modules/(?!.*(superjson|copy-anything|is-what))'],
};

module.exports = {
  projects: [
    {
      ...base,
      displayName: 'unit',
      testMatch: ['<rootDir>/src/**/*.test.ts'],
    },
    {
      ...base,
      displayName: 'integration',
      testMatch: ['<rootDir>/test/integration/**/*.test.ts'],
    },
  ],
};
