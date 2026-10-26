// Smoke-tests both published entry points of the dual build: ESM import from
// dist/ and CJS require from dist-cjs/. Run after `npm run build` and
// `npm run build:cjs` (wired as `npm run smoke`).
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

import { mpscChannel as esmMpsc, select as esmSelect, timeout as esmTimeout } from '../dist/index.js';

const require = createRequire(import.meta.url);
const { mpscChannel: cjsMpsc, select: cjsSelect, timeout: cjsTimeout } = require('../dist-cjs/index.js');

const variants = [
    ['esm', esmMpsc, esmSelect, esmTimeout],
    ['cjs', cjsMpsc, cjsSelect, cjsTimeout],
];

for (const [label, mpscChannel, select, timeout] of variants) {
    const [tx, rx] = mpscChannel(1);
    assert.equal(tx.trySend(7), 'ok');
    assert.deepEqual(rx.tryRecv(), { status: 'value', value: 7 });
    assert.deepEqual(await select([Promise.resolve('a'), Promise.resolve('b')]), { index: 0, value: 'a' });
    assert.equal(await timeout(50, Promise.resolve(1)), 1);
    console.log(`smoke ${label}: ok`);
}
