import { createRequire } from 'node:module';
import { readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

// This is a maintainer-only generation step. Installed Go applications need
// neither Node.js nor the Node SDK repository.
const here = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(path.join(here, '../../repro-web/package.json'));
const { build } = require('esbuild');
const result = await build({
  entryPoints: [path.join(here, 'privacy-entry.ts')],
  bundle: true, write: false, format: 'iife', globalName: 'ReproPrivacy',
  target: 'es2018', platform: 'neutral', treeShaking: true, legalComments: 'inline',
  alias: { crypto: path.join(here, 'privacy-crypto.ts') }, metafile: true,
});
const sources = {};
for (const filename of Object.keys(result.metafile.inputs).sort()) {
  sources[path.relative(path.join(here, '..'), path.resolve(filename))] = createHash('sha256').update(await readFile(filename)).digest('hex');
}
await writeFile(path.join(here, '../internal/privacy/engine.js'), result.outputFiles[0].contents);
await writeFile(path.join(here, '../internal/privacy/sources.json'), JSON.stringify(sources, null, 2) + '\n');
