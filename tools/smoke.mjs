import fs from 'node:fs';
import vm from 'node:vm';

const BASE = process.env.LIQ_BASE || 'https://liq.bittensor.workers.dev';
const dash = await (await fetch(`${BASE}/api/dashboard?coin=BTC`)).json();
const candles = await (await fetch(`${BASE}/api/candles?coin=BTC&tf=1d`)).json();

const ctxStub = () => new Proxy({}, { get: (t, k) => {
  if (k === 'measureText') return () => ({ width: 90 });
  if (k === 'createLinearGradient' || k === 'createPattern') return () => ({ addColorStop() {} });
  if (typeof k === 'string') return t[k] !== undefined ? t[k] : (() => {});
  return undefined;
}, set: () => true });

function makeEl(id) {
  const el = {
    id, style: {}, children: [], dataset: {}, width: 0, height: 0,
    clientWidth: id === 'ladder' ? 264 : 1500, clientHeight: 800,
    classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
    addEventListener() {}, removeEventListener() {}, appendChild() {}, remove() {},
    setAttribute() {}, getAttribute: () => null, querySelector: () => null, querySelectorAll: () => [],
    getContext: () => ctxStub(), getBoundingClientRect: () => ({ left: 0, top: 0, width: 1500, height: 800 }),
    focus() {}, click() {},
  };
  return el;
}
const els = new Map();
const documentStub = {
  getElementById: (id) => { if (!els.has(id)) els.set(id, makeEl(id)); return els.get(id); },
  querySelectorAll: () => [], querySelector: () => null,
  createElement: (tag) => makeEl(tag), addEventListener() {}, body: makeEl('body'), documentElement: makeEl('html'),
};

const sandbox = {
  console, Math, JSON, Date, Promise, Set, Map, Array, Object, Number, String, Boolean, RegExp, Error, Float64Array, isFinite, parseFloat, parseInt,
  document: documentStub,
  localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
  location: { href: 'https://liq.bittensor.workers.dev/', reload() {} },
  navigator: { userAgent: 'harness' },
  devicePixelRatio: 1, innerWidth: 1920, innerHeight: 1000,
  addEventListener() {}, removeEventListener() {},
  setInterval: () => 0, clearInterval() {}, setTimeout: (f) => 0, clearTimeout() {},
  requestAnimationFrame: () => 0,
  fetch: async (url) => {
    const u = String(url);
    let body = {};
    if (u.includes('/api/dashboard')) body = dash;
    else if (u.includes('/api/candles')) body = candles;
    else if (u.includes('hyperliquid')) body = [];
    return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) };
  },
};
sandbox.window = sandbox; sandbox.globalThis = sandbox;
vm.createContext(sandbox);

const html = fs.readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');
let script = html.match(/<script>([\s\S]*)<\/script>/)[1];
script += '\n;globalThis.__test = () => { const out = {}; try { computeScale(); out.scale = !!scale; } catch (e) { out.scaleErr = e.stack.split("\\n").slice(0,3).join(" | "); } try { renderLadder(); out.ladder = "OK"; } catch (e) { out.ladderErr = e.stack.split("\\n").slice(0,4).join(" | "); } try { draw(); out.draw = "OK"; } catch (e) { out.drawErr = e.stack.split("\\n").slice(0,4).join(" | "); } return out; };';
try {
  vm.runInContext(script, sandbox, { filename: 'liq.js' });
  console.log('script eval: OK');
} catch (e) {
  console.log('script eval THREW:', e.stack.split('\n').slice(0, 4).join(' | '));
}
await new Promise((r) => setTimeout(r, 300));
console.log(JSON.stringify(sandbox.__test ? sandbox.__test() : { noTest: true }, null, 1));
