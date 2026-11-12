/**
 * 流动性看板 — Cloudflare Worker（name = liq → liq.bittensor.workers.dev）
 *
 * - fetch：托管前端（构建时由 scripts/build.mjs 把 public/index.html 内联进 INDEX_HTML）
 *     GET /api/dashboard?coin=X   { markets, snapshot }（snapshot 含 candlesTf 摘要）
 *     GET /api/candles?coin=&tf=&before=  K 线窗口数组
 *     GET /api/status             各键更新时间 / 状态机进度
 *     POST /api/step              手动推进一个刷新阶段（排障/播种用，同 UNI 先例的 /api/refresh）
 * - scheduled（cron 每 10 分钟）：推进管线状态机一个阶段（≤40 子请求），快照每小时至少发布一版
 *
 * 存储：单个 KV namespace（binding CACHE）
 *   snap:<COIN> / candles:<COIN>:<tf> / candles:meta / lighter:watchlist / job:state / stage:*
 */

import { createPipeline, COINS, TFS } from './pipeline.js';

// 构建时由 scripts/build.mjs 注入 public/index.html 的完整内容
const INDEX_HTML = "__LIQ_INDEX_HTML__";

const kvAdapter = (env) => ({
  get: (key) => env.CACHE.get(key, 'json'),
  put: (key, value) => env.CACHE.put(key, JSON.stringify(value)),
});

const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
  });

const badCoin = (coin) => !COINS.includes(coin);
const badTf = (tf) => !TFS.includes(tf);

export default {
  async scheduled(event, env, ctx) {
    const pipeline = createPipeline({ fetch, kv: kvAdapter(env) });
    ctx.waitUntil(pipeline.step(40));
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const kv = kvAdapter(env);

    if (url.pathname === '/api/dashboard' && request.method === 'GET') {
      const coin = (url.searchParams.get('coin') || 'BTC').toUpperCase();
      if (badCoin(coin)) return json({ error: 'bad coin' }, 400);
      const snap = await kv.get(`snap:${coin}`);
      const meta = (await kv.get('candles:meta')) || { coins: {} };
      if (!snap) return json({ markets: [], snapshot: null, candlesTf: meta.coins[coin] || {} });
      const { markets, ...rest } = snap;
      return json({ markets: markets || [], snapshot: rest, candlesTf: meta.coins[coin] || {} });
    }

    if (url.pathname === '/api/candles' && request.method === 'GET') {
      const coin = (url.searchParams.get('coin') || 'BTC').toUpperCase();
      const tf = url.searchParams.get('tf') || '1d';
      if (badCoin(coin) || badTf(tf)) return json({ error: 'bad coin/tf' }, 400);
      const stored = (await kv.get(`candles:${coin}:${tf}`)) || { bars: [] };
      const bars = stored.bars || [];
      const before = Number(url.searchParams.get('before')) || Infinity;
      const limit = Math.min(Number(url.searchParams.get('limit')) || 1500, 4000);
      // 取 t < before 的最后 limit 根（升序返回）
      let end = bars.length;
      while (end > 0 && bars[end - 1][0] >= before) end--;
      const slice = bars.slice(Math.max(0, end - limit), end);
      return json({
        coin, tf,
        bars: slice.map((b) => ({ t: b[0], o: b[1], h: b[2], l: b[3], c: b[4], v: b[5] })),
        total: bars.length,
        earliest: bars.length ? bars[0][0] : null,
        latest: bars.length ? bars[bars.length - 1][0] : null,
      });
    }

    if (url.pathname === '/api/status' && request.method === 'GET') {
      const state = await kv.get('job:state');
      const meta = (await kv.get('candles:meta')) || { coins: {} };
      const watchlist = (await kv.get('lighter:watchlist')) || {};
      const snaps = {};
      for (const coin of COINS) {
        const s = await kv.get(`snap:${coin}`);
        snaps[coin] = s ? s.fetchedAt : null;
      }
      return json({
        snaps, candlesUpdatedAt: meta.updatedAt || null, candles: meta.coins || {},
        watchlist: Object.keys(watchlist).length,
        job: state ? {
          stage: state.stage, round: state.round,
          hlCursor: state.hlCursor, ltCursor: state.ltCursor,
          lastPublish: state.lastPublish ? new Date(state.lastPublish).toISOString() : null,
          updatedAt: state.updatedAt,
        } : null,
      });
    }

    if (url.pathname === '/api/step' && request.method === 'POST') {
      const pipeline = createPipeline({ fetch, kv });
      const report = await pipeline.step(40);
      return json({ ok: true, report });
    }

    if (url.pathname.startsWith('/api/')) return json({ error: 'not found' }, 404);

    // 静态前端（构建时已内联进 INDEX_HTML）
    return new Response(INDEX_HTML, {
      headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' },
    });
  },
};
