# Workspace Package Template

Universal template for PNPM workspace packages.

## Usage

Clone this template into your monorepo:

```bash
pnpx degit dennisofficial/workspace-package-template ./folder
```

Then update:
1. `package.json` - Change `name`, `description`, `author`
2. Start adding your code in `src/index.ts`

## Scripts

- `pnpm build` - Build the package
- `pnpm dev` - Watch mode (rebuilds on file changes)

## Features

- ✅ TypeScript with strict mode
- ✅ Dual CJS/ESM builds via tsup
- ✅ Source maps
- ✅ Tree shaking
- ✅ Auto-detects externals from package.json
- ✅ Watch mode with nodemon
