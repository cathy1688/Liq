// 部署前构建：把 src/pipeline.js 与 src/index.js 合并为单文件，并内联 public/index.html
// 用法：node scripts/build.mjs  →  输出 dist/index.js（wrangler.toml 的 main）
// 说明：pipeline.js 无外部 import，仅用命名导出；此处剥离 export 关键字后与 worker 源码拼接，
// worker 顶部对 './pipeline.js' 的 import 行随之移除（名字在同一作用域内可见）。
import { readFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const pipeline = readFileSync(join(root, 'src/pipeline.js'), 'utf8');
const worker = readFileSync(join(root, 'src/index.js'), 'utf8');
const html = readFileSync(join(root, 'public/index.html'), 'utf8');

const pipelineBody = pipeline
  .replace(/^export (async function|function|const|let|class) /gm, '$1 ')
  .replace(/^export \{[^}]*\};?\s*$/gm, '');

const importLine = "import { createPipeline, COINS, TFS } from './pipeline.js';";
if (!worker.includes(importLine)) throw new Error('pipeline import line not found in src/index.js');
const PLACEHOLDER = '"__LIQ_INDEX_HTML__"';
if (!worker.includes(PLACEHOLDER)) throw new Error('INDEX_HTML placeholder not found in src/index.js');

const workerBody = worker
  .replace(importLine, '// [build] pipeline.js 已内联至本文件上方')
  .replace(PLACEHOLDER, () => JSON.stringify(html));

const out = `// 由 scripts/build.mjs 生成，勿手改（源：src/pipeline.js + src/index.js + public/index.html）\n${pipelineBody}\n${workerBody}`;
mkdirSync(join(root, 'dist'), { recursive: true });
writeFileSync(join(root, 'dist/index.js'), out);
console.log(`built dist/index.js (${out.length} bytes)`);
