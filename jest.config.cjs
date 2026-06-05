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
      testTimeout: 30000,
    },
  ],
};
