// Marks the CJS build directory as CommonJS. The package root is "type":
// "module", so without this file Node would parse dist-cjs/*.js as ESM.
import { mkdirSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const dir = fileURLToPath(new URL('../dist-cjs/', import.meta.url));
mkdirSync(dir, { recursive: true });
writeFileSync(`${dir}package.json`, `${JSON.stringify({ type: 'commonjs' }, null, 2)}\n`);
