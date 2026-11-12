// 由 scripts/build.mjs 生成，勿手改（源：src/pipeline.js + src/index.js + public/index.html）
/**
 * 流动性看板 — 数据管线（同构模块，Worker 与本地 Node 共用）
 *
 * 职责：HL 排行榜鲸鱼扫描 + Lighter 成交发现样本仓位 + 合并分簇 + K 线回填/追加。
 * 刷新被拆成 KV 状态机阶段（harvest → hlscan → lighterpoll → finalize → candles），
 * 每阶段 ≤ BUDGET 个外部子请求，靠 cron 每 10 分钟推进；快照每小时至少发布一版。
 *
 * 依赖注入：createPipeline({ fetch, kv, now })
 *   fetch: 标准 fetch
 *   kv:    { get(key)->Promise<any|null>, put(key, value)->Promise<void> }（值均为 JSON 可序列化对象）
 *   now:   ()->ms 时间戳
 *
 * KV 键：
 *   snap:<COIN>          最新已发布快照
 *   candles:<COIN>:<tf>  K 线序列 { bars: [[t,o,h,l,c,v],...] }（t 升序）
 *   candles:meta         K 线元信息 { updatedAt, coins: { COIN: { tf: {count, earliest, latest} } } }
 *   lighter:watchlist    { "<accountIndex>": lastSeenMs }
 *   job:state            状态机状态
 *   stage:addrs          本轮 HL 待扫地址（top300）
 *   stage:hlpos          本轮 HL 已累积仓位
 *   stage:ltpos          本轮 Lighter 已累积仓位
 *   stage:ltstats        本轮 Lighter 统计 { polled, found, rejected }
 */

const COINS = ['BTC', 'ETH', 'HYPE', 'TAO', 'UNI', 'LIT'];
const TFS = ['15m', '1h', '4h', '1d', '1w', '1M'];
const TF_MS = {
  '15m': 900000, '1h': 3600000, '4h': 14400000,
  '1d': 86400000, '1w': 604800000, '1M': 2592000000,
};
const LIGHTER_MARKETS = { ETH: 0, BTC: 1, TAO: 13, HYPE: 24, UNI: 30, LIT: 120 };
const BITFINEX_PAIRS = { BTC: 'tBTCUSD', ETH: 'tETHUSD', UNI: 'tUNIUSD' }; // TAO/HYPE/LIT 无 Bitfinex 对子
const BFX_TF = { '15m': '15m', '1h': '1h', '4h': '4h', '1d': '1D', '1w': '1W', '1M': '1M' };
// 周线口径（2026-11-12 定）：全站周四锚（epoch 周），与 HL 原生 '1w' 及原版看板一致——
// Bitfinex '7D' 是滚动窗、开盘不接上根收盘（画出来是乱的，已弃用）；'1W' 是周一锚，
// 与 HL 尾段拼接会错位。故 1w 一律走 HL 分支（见 candleVisit），种子由日线按周四桶聚合灌入。
const BACKFILL_TARGET_MS = Date.UTC(2022, 10, 11); // 2022-11-11，与现版回填视界一致
const HL_INFO = 'https://api.hyperliquid.xyz/info';
const HL_LEADERBOARD = 'https://stats-data.hyperliquid.xyz/Mainnet/leaderboard';
const LIGHTER_API = 'https://mainnet.zklighter.elliot.ai/api/v1';
const BFX_API = 'https://api-pub.bitfinex.com/v2';
const SENTINEL_ACCOUNT = 10000000; // ≥ 此值的 account_id 为哨兵，过滤
const MIN_POSITION_USD = 10000;     // Lighter 仓位入围门槛
const CLUSTER_MIN_USD = 3000000;    // 簇金额门槛（或每侧 top5 保底）
const CLUSTER_MAX_PER_SIDE = 8;
const STAGE_BUDGET = 40;     // 每阶段外部子请求上限
const PUBLISH_INTERVAL_MS = 55 * 60000; // 快照最长发布间隔

const num = (v) => (v === null || v === undefined || v === '' ? NaN : Number(v));
const isPos = (p) => p && isFinite(p.liqPx) && p.liqPx > 0 && isFinite(p.notional) && p.notional > 0;

/** 分簇：同侧按清算价排序，相邻间隔 ≤ 现价 1% 时链式并簇（纯函数，可单测） */
function clusterPositions(positions, mark) {
  const out = [];
  for (const side of [1, -1]) {
    // 分区口径（据原版 artifact 发布快照反推核实，2026-11-12）：按距现价的 1% 价格格归格，
    // 同侧相邻格合并成带；带边 = 格边 mark×(1+k%)，故每带至少 1% 宽、distPct = 带中心 %。
    // 旧口径（相邻清算价 ≤1% 链式合并、带边取实际清算价）会产生大量零宽单鲸簇，图上只剩发丝线。
    const cells = new Map(); // cellIdx -> { totalUsd, whales }
    for (const p of positions) {
      if (p.side !== side) continue;
      const idx = Math.floor((p.liqPx / mark - 1) * 100);
      let c = cells.get(idx);
      if (!c) { c = { totalUsd: 0, whales: new Set() }; cells.set(idx, c); }
      c.totalUsd += p.notional; c.whales.add(p.id);
    }
    const groups = [];
    for (const idx of [...cells.keys()].sort((a, b) => a - b)) {
      const c = cells.get(idx);
      const g = groups[groups.length - 1];
      if (g && idx === g.hiCell + 1) {
        g.hiCell = idx; g.totalUsd += c.totalUsd;
        for (const w of c.whales) g.whales.add(w);
      } else {
        groups.push({ loCell: idx, hiCell: idx, totalUsd: c.totalUsd, whales: new Set(c.whales) });
      }
    }
    // 门槛：≥$3M，或该侧金额 top5 保底；每侧最多 8 簇（按金额取大）
    const bySize = [...groups].sort((a, b) => b.totalUsd - a.totalUsd);
    const top5 = new Set(bySize.slice(0, 5));
    let kept = groups.filter((g) => g.totalUsd >= CLUSTER_MIN_USD || top5.has(g));
    kept = [...kept].sort((a, b) => b.totalUsd - a.totalUsd).slice(0, CLUSTER_MAX_PER_SIDE);
    for (const g of kept) {
      out.push({
        side: side === 1 ? 'long' : 'short',
        lowPx: mark * (1 + g.loCell / 100), highPx: mark * (1 + (g.hiCell + 1) / 100),
        totalUsd: g.totalUsd,
        whaleCount: g.whales.size,
        distPct: (g.loCell + g.hiCell + 1) / 2,
      });
    }
  }
  // 同侧按距现价由近及远
  out.sort((a, b) => (a.side === b.side ? Math.abs(a.distPct) - Math.abs(b.distPct) : a.side === 'long' ? -1 : 1));
  return out;
}

/** 阶梯 buckets：−50% ~ +60%，步长 2.5%（纯函数） */
function buildBuckets(positions, mark) {
  const buckets = [];
  for (let lo = -50; lo < 60; lo += 2.5) {
    const hi = lo + 2.5;
    const pLo = mark * (1 + lo / 100), pHi = mark * (1 + hi / 100);
    let longUsd = 0, shortUsd = 0;
    for (const p of positions) {
      if (p.liqPx >= pLo && p.liqPx < pHi) {
        if (p.side === 1) longUsd += p.notional; else shortUsd += p.notional;
      }
    }
    if (longUsd > 0 || shortUsd > 0) {
      buckets.push({ price: mark * (1 + (lo + hi) / 200), longUsd, shortUsd });
    }
  }
  return buckets;
}

/** K 线合并：同 t 以 incoming 为准（HL 近段优先），返回升序数组 */
function mergeBars(existing, incoming) {
  const map = new Map();
  for (const b of existing || []) map.set(b[0], b);
  for (const b of incoming || []) map.set(b[0], b);
  return [...map.values()].sort((a, b) => a[0] - b[0]);
}

function createPipeline(deps) {
  const fetchFn = deps.fetch;
  const kv = deps.kv;
  const now = deps.now || (() => Date.now());
  let budget = { used: 0, limit: STAGE_BUDGET };

  async function api(url, opts) {
    if (budget.used >= budget.limit) throw new Error('__budget__');
    budget.used++;
    const r = await fetchFn(url, opts);
    if (!r.ok) throw new Error(`upstream ${r.status} ${url.split('?')[0]}`);
    return r.json();
  }
  const hlPost = (body) => api(HL_INFO, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
  });

  // ---------- 基础数据 ----------
  async function fetchMeta() {
    const [meta, ctxs] = await hlPost({ type: 'metaAndAssetCtxs' });
    const markets = [], marks = {};
    meta.universe.forEach((u, i) => {
      if (!COINS.includes(u.name)) return;
      const c = ctxs[i] || {};
      const markPx = num(c.markPx);
      marks[u.name] = markPx;
      markets.push({
        coin: u.name,
        market: {
          markPx, prevDayPx: num(c.prevDayPx),
          changePct: markPx && num(c.prevDayPx) ? (markPx / num(c.prevDayPx) - 1) * 100 : 0,
          dayVolumeUsd: num(c.dayNtlVlm), openInterestUsd: num(c.openInterest) * markPx,
          fundingRate: num(c.funding),
        },
      });
    });
    markets.sort((a, b) => COINS.indexOf(a.coin) - COINS.indexOf(b.coin));
    return { markets, marks };
  }

  async function fetchLeaderboardAddrs() {
    const j = await api(HL_LEADERBOARD);
    const rows = Array.isArray(j) ? j : j.leaderboardRows || [];
    return rows.slice(0, 300).map((r) => String(r.ethAddress || r.address || '').toLowerCase()).filter(Boolean);
  }

  async function fetchHlPositions(address) {
    const j = await hlPost({ type: 'clearinghouseState', user: address });
    const out = [];
    for (const ap of j.assetPositions || []) {
      const p = ap.position || {};
      if (!COINS.includes(p.coin)) continue;
      const liqPx = num(p.liquidationPx);
      const szi = num(p.szi);
      const notional = Math.abs(num(p.positionValue));
      const rec = {
        src: 'hl', id: address, coin: p.coin, side: szi >= 0 ? 1 : -1,
        size: szi, entryPx: num(p.entryPx), liqPx,
        notional, lev: p.leverage ? num(p.leverage.value) : NaN,
      };
      if (isPos(rec)) out.push(rec);
    }
    return out;
  }

  async function fetchLighterTrades(marketId) {
    const j = await api(`${LIGHTER_API}/recentTrades?market_id=${marketId}&limit=100`);
    return Array.isArray(j) ? j : j.trades || [];
  }

  async function fetchLighterPositions(accountId, marks) {
    const j = await api(`${LIGHTER_API}/account?by=index&value=${accountId}`);
    const acc = (j.accounts && j.accounts[0]) || j;
    const found = [], accepted = [];
    let rejected = 0;
    for (const p of acc.positions || []) {
      const coin = Object.keys(LIGHTER_MARKETS).find((c) => LIGHTER_MARKETS[c] === p.market_id);
      if (!coin) continue;
      const notional = Math.abs(num(p.position_value));
      if (!(notional >= MIN_POSITION_USD)) continue;
      found.push(p);
      const liqPx = num(p.liquidation_price);
      const mark = marks[coin];
      const side = num(p.sign) >= 0 ? 1 : -1;
      // 清算价合理性过滤：全仓账户的 0 / 哨兵乱值在此被拒，只收逐仓量级可信的仓位
      let ok = false;
      if (mark > 0 && liqPx > 0) {
        ok = side === 1 ? (liqPx >= 0.25 * mark && liqPx < mark) : (liqPx > mark && liqPx <= 3 * mark);
      }
      const rec = {
        src: 'lt', id: `lighter:${accountId}`, coin, side,
        size: num(p.position), entryPx: num(p.avg_entry_price), liqPx, notional, lev: NaN,
      };
      if (ok && isPos(rec)) accepted.push(rec); else rejected++;
    }
    return { found: found.length, accepted, rejected };
  }

  // ---------- 发布 ----------
  async function publish(state, reason) {
    const hlPos = (await kv.get('stage:hlpos')) || [];
    const ltPos = (await kv.get('stage:ltpos')) || [];
    const ltStats = (await kv.get('stage:ltstats')) || { polled: 0, found: 0, rejected: 0 };
    const watchlist = (await kv.get('lighter:watchlist')) || {};
    const marks = state.marks || {};
    const fetchedAt = new Date(now()).toISOString();
    const all = [...hlPos, ...ltPos];
    for (const coin of COINS) {
      const pos = all.filter((p) => p.coin === coin);
      const mark = marks[coin] || 0;
      const longs = pos.filter((p) => p.side === 1), shorts = pos.filter((p) => p.side === -1);
      const snap = {
        coin, fetchedAt, markPx: mark,
        totals: {
          longLiqUsd: longs.reduce((s, p) => s + p.notional, 0),
          shortLiqUsd: shorts.reduce((s, p) => s + p.notional, 0),
          positionCount: pos.length,
          whaleCount: new Set(pos.map((p) => p.id)).size,
        },
        clusters: mark > 0 ? clusterPositions(pos, mark) : [],
        buckets: mark > 0 ? buildBuckets(pos, mark) : [],
        sources: {
          hl: { scannedAccounts: state.hlCursor || 0, positionCount: hlPos.filter((p) => p.coin === coin).length },
          lighter: {
            watchlist: Object.keys(watchlist).length, polled: ltStats.polled || 0,
            found: ltStats.found || 0,
            accepted: ltPos.filter((p) => p.coin === coin).length,
            rejected: ltStats.rejected || 0, ok: true,
          },
        },
        markets: state.markets || [],
        publishReason: reason,
      };
      await kv.put(`snap:${coin}`, snap);
    }
    state.lastPublish = now();
    return fetchedAt;
  }

  // ---------- K 线 ----------
  async function hlCandles(coin, tf, startMs, endMs) {
    const j = await hlPost({
      type: 'candleSnapshot',
      req: { coin, interval: tf, startTime: startMs, endTime: endMs },
    });
    return (j || []).map((c) => [c.t, num(c.o), num(c.h), num(c.l), num(c.c), num(c.v)]);
  }

  // Bitfinex 大周期单页跨度过大时返回 500，分页根数按周期收窄（2026-11-12 实测可用值）
  const BFX_PAGE_BARS = { '15m': 1000, '1h': 1000, '4h': 1000, '1d': 400, '1w': 200, '1M': 120 };
  async function bfxPage(coin, tf, endMs) {
    const pageBars = BFX_PAGE_BARS[tf] || 1000;
    const span = TF_MS[tf] * pageBars;
    const url = `${BFX_API}/candles/trade:${BFX_TF[tf]}:${BITFINEX_PAIRS[coin]}/hist?start=${endMs - span}&end=${endMs}&limit=${pageBars}&sort=-1`;
    const j = await api(url);
    // Bitfinex 行格式 [mts, open, close, high, low, volume]，注意 close 在 high 之前
    return (j || []).map((r) => [r[0], r[1], r[3], r[4], r[2], r[5]]).sort((a, b) => a[0] - b[0]);
  }

  async function candleVisit(state, coin, tf) {
    const key = `candles:${coin}:${tf}`;
    const meta = state.candles[`${coin}:${tf}`] || (state.candles[`${coin}:${tf}`] = { done: false });
    const stored = (await kv.get(key)) || { bars: [] };
    const tfMs = TF_MS[tf];
    if (!meta.done) {
      if (BITFINEX_PAIRS[coin] && tf !== '1w') {
        // Bitfinex 分页回填：每访最多 4 页（4000 根）
        // 注意：end 取未来时刻会让 Bitfinex 对 1D/7D/1M 返回 10020 time_interval invalid，故用 now()
        let cursor = meta.cursorEnd || now();
        for (let i = 0; i < 4; i++) {
          const page = await bfxPage(coin, tf, cursor);
          if (!page.length) { meta.done = true; meta.source = 'bitfinex'; break; }
          stored.bars = mergeBars(stored.bars, page);
          cursor = page[0][0];
          meta.cursorEnd = cursor;
          await kv.put(key, stored); // 每页落盘：中途失败（如 429）不丢进度、不留洞，下轮续页
          // 短页可能只是无成交缺口，不作完成判据；只认空页或抵达回填目标
          if (cursor <= BACKFILL_TARGET_MS) { meta.done = true; meta.source = 'bitfinex'; break; }
          if (i < 3) await new Promise((r) => setTimeout(r, 120)); // 对 Bitfinex 限流客气一点
        }
      } else {
        // 无 Bitfinex 对子：HL 一次取可得全历史（上限约 5000 根），如实记最早日期
        const bars = await hlCandles(coin, tf, 0, now());
        stored.bars = mergeBars(stored.bars, bars);
        meta.done = true; meta.source = 'hl';
      }
      if (meta.done && stored.bars.length) meta.earliest = stored.bars[0][0];
    } else {
      // 追加/修未收盘根：只取尾段，HL 覆盖同 t
      const lastT = stored.bars.length ? stored.bars[stored.bars.length - 1][0] : 0;
      const bars = await hlCandles(coin, tf, Math.max(0, lastT - 3 * tfMs), now());
      stored.bars = mergeBars(stored.bars, bars);
    }
    await kv.put(key, stored);
    return { count: stored.bars.length, earliest: stored.bars.length ? stored.bars[0][0] : null, latest: stored.bars.length ? stored.bars[stored.bars.length - 1][0] : null };
  }

  async function saveCandleMeta(state, perPair) {
    const meta = (await kv.get('candles:meta')) || { coins: {} };
    meta.updatedAt = new Date(now()).toISOString();
    for (const [k, v] of Object.entries(perPair)) {
      const [coin, tf] = k.split(':');
      meta.coins[coin] = meta.coins[coin] || {};
      meta.coins[coin][tf] = v;
    }
    await kv.put('candles:meta', meta);
  }

  // ---------- 状态机 ----------
  async function loadState() {
    return (await kv.get('job:state')) || {
      stage: 'harvest', round: 1, hlCursor: 0, ltCursor: 0, ltOrder: [],
      candleIdx: 0, rotationDone: false, lastPublish: 0,
      marks: {}, markets: [], candles: {}, updatedAt: null,
    };
  }

  /**
   * 推进一个阶段（≤ budgetLimit 个外部子请求）。
   * 任意阶段边界上，若距上次发布 ≥55 分钟且已有累积仓位，先补发一版快照（不清空累积，覆盖度随轮次生长）。
   */
  async function step(budgetLimit = STAGE_BUDGET) {
    budget = { used: 0, limit: budgetLimit };
    const state = await loadState();
    const report = { stage: state.stage, round: state.round, subrequests: 0, actions: [] };
    try {
      // 小时发布保底
      if (state.lastPublish && now() - state.lastPublish >= PUBLISH_INTERVAL_MS &&
          (((await kv.get('stage:hlpos')) || []).length || ((await kv.get('stage:ltpos')) || []).length)) {
        report.actions.push(`publish:hourly@${await publish(state, 'hourly')}`);
      }

      if (state.stage === 'harvest') {
        const { markets, marks } = await fetchMeta();
        state.markets = markets; state.marks = marks;
        const wl = (await kv.get('lighter:watchlist')) || {};
        if (!state.ltCooldownUntil || now() >= state.ltCooldownUntil) {
          try {
            for (const coin of COINS) {
              const trades = await fetchLighterTrades(LIGHTER_MARKETS[coin]);
              for (const t of trades) {
                for (const id of [t.ask_account_id, t.bid_account_id]) {
                  const n = num(id);
                  if (isFinite(n) && n > 0 && n < SENTINEL_ACCOUNT) wl[String(Math.trunc(n))] = now();
                }
              }
            }
            await kv.put('lighter:watchlist', wl);
          } catch (e) {
            // Lighter 限流/故障不得堵死整条管线：名单沿用存量，冷却 30 分钟后再试，本阶段照常推进
            state.ltCooldownUntil = now() + 30 * 60 * 1000;
            report.actions.push(`harvest:lighter-deferred:${String(e && e.message || e).slice(0, 80)}`);
          }
        } else {
          report.actions.push('harvest:lighter-cooldown');
        }
        report.actions.push(`harvest:watchlist=${Object.keys(wl).length}`);
        state.stage = 'hlscan'; state.hlCursor = 0;
        await kv.put('stage:hlpos', []);
      } else if (state.stage === 'hlscan') {
        let addrs = await kv.get('stage:addrs');
        if (!addrs || state.addrsRound !== state.round) {
          addrs = await fetchLeaderboardAddrs();
          await kv.put('stage:addrs', addrs);
          state.addrsRound = state.round;
          report.actions.push(`hlscan:leaderboard=${addrs.length}`);
        }
        addrs = addrs || [];
        const pos = (await kv.get('stage:hlpos')) || [];
        while (state.hlCursor < addrs.length && budget.used < budget.limit) {
          try { pos.push(...await fetchHlPositions(addrs[state.hlCursor])); } catch (e) { if (e.message === '__budget__') throw e; }
          state.hlCursor++;
        }
        await kv.put('stage:hlpos', pos);
        report.actions.push(`hlscan:${state.hlCursor}/${addrs.length},pos=${pos.length}`);
        if (state.hlCursor >= addrs.length) {
          // 进入 Lighter 轮询：名单按 lastSeen 倒序，截断 300
          const wl = (await kv.get('lighter:watchlist')) || {};
          state.ltOrder = Object.entries(wl).sort((a, b) => b[1] - a[1]).slice(0, 300).map((e) => e[0]);
          state.ltCursor = 0;
          await kv.put('stage:ltpos', []);
          await kv.put('stage:ltstats', { polled: 0, found: 0, rejected: 0 });
          state.stage = 'lighterpoll';
        }
      } else if (state.stage === 'lighterpoll') {
        const pos = (await kv.get('stage:ltpos')) || [];
        const stats = (await kv.get('stage:ltstats')) || { polled: 0, found: 0, rejected: 0 };
        while (state.ltCursor < state.ltOrder.length && budget.used < budget.limit) {
          try {
            const r = await fetchLighterPositions(state.ltOrder[state.ltCursor], state.marks || {});
            pos.push(...r.accepted); stats.found += r.found; stats.rejected += r.rejected;
          } catch (e) { if (e.message === '__budget__') throw e; }
          state.ltCursor++; stats.polled++;
        }
        await kv.put('stage:ltpos', pos);
        await kv.put('stage:ltstats', stats);
        report.actions.push(`lighterpoll:${state.ltCursor}/${state.ltOrder.length},accepted=${pos.length}`);
        if (state.ltCursor >= state.ltOrder.length) state.stage = 'finalize';
      } else if (state.stage === 'finalize') {
        report.actions.push(`publish:finalize@${await publish(state, 'finalize')}`);
        state.stage = 'candles'; state.candleIdx = 0; state.rotationDone = false;
      } else if (state.stage === 'candles') {
        const pairs = [];
        for (const coin of COINS) for (const tf of TFS) pairs.push([coin, tf]);
        const perPair = {};
        let visited = 0;
        while (visited < pairs.length && budget.used < budget.limit) {
          const idx = (state.candleIdx + visited) % pairs.length;
          const [coin, tf] = pairs[idx];
          try { perPair[`${coin}:${tf}`] = await candleVisit(state, coin, tf); }
          catch (e) { if (e.message === '__budget__') break; perPair[`${coin}:${tf}`] = { error: String(e.message || e) }; }
          visited++;
        }
        state.candleIdx = (state.candleIdx + visited) % pairs.length;
        if (state.candleIdx === 0 || visited >= pairs.length) state.rotationDone = true;
        await saveCandleMeta(state, perPair);
        report.actions.push(`candles:visited=${visited},idx=${state.candleIdx}`);
        if (state.rotationDone) {
          // 一轮结束：清空累积，开新一轮
          await kv.put('stage:hlpos', []);
          await kv.put('stage:ltpos', []);
          await kv.put('stage:ltstats', { polled: 0, found: 0, rejected: 0 });
          state.round++; state.stage = 'harvest'; state.hlCursor = 0; state.ltCursor = 0;
          report.actions.push(`round->${state.round}`);
        }
      }
    } catch (e) {
      if (e.message !== '__budget__') report.error = String(e && e.message || e);
    }
    report.subrequests = budget.used;
    state.updatedAt = new Date(now()).toISOString();
    await kv.put('job:state', state);
    return report;
  }

  /** 本地/手动：连续推进直到完成一整轮（round 变化）或达到 maxSteps */
  async function runFullRound(maxSteps = 200, budgetPerStep = 100000) {
    const reports = [];
    const start = await loadState();
    for (let i = 0; i < maxSteps; i++) {
      const r = await step(budgetPerStep);
      reports.push(r);
      const s = await loadState();
      if (s.round !== start.round) break;
    }
    return reports;
  }

  return { step, runFullRound, publish, loadState, _internal: { fetchMeta, clusterPositions, buildBuckets } };
}

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

// [build] pipeline.js 已内联至本文件上方

// 构建时由 scripts/build.mjs 注入 public/index.html 的完整内容
const INDEX_HTML = "<!DOCTYPE html>\n<html lang=\"zh-CN\">\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<title>流动性看板 · 清算簇</title>\n<style>\n  :root{\n    --ink:#1F2D38; --gray:#8A9BA3; --axis-gray:#A3B4BC; --border:#E3EAF0; --grid:#E7EEF3;\n    --up:#50B383; --down:#F6465D; --long:#00B16B; --short:#F6465D; --selected:#00A870;\n  }\n  *{box-sizing:border-box; margin:0; padding:0;}\n  body{background:#F7F8FA; color:var(--ink);\n    font-family:Inter,\"SF Pro Display\",\"Segoe UI\",Roboto,\"Helvetica Neue\",Arial,\"PingFang SC\",\"Microsoft YaHei\",sans-serif;\n    font-variant-numeric:tabular-nums; -webkit-font-smoothing:antialiased;}\n  .wrap{max-width:1800px; margin:0 auto; background:#fff; border-left:1px solid var(--border); border-right:1px solid var(--border);\n    padding:16px 20px 18px; min-height:100vh;}\n  @media (max-width:760px){ .wrap{padding:10px 8px 14px;} }\n  /* ---- 头部：币对 + 大价格 + 横排 24h 统计（对图1） ---- */\n  .head1{display:flex; align-items:center; gap:16px; flex-wrap:wrap;}\n  .pair-name{font-size:17px; font-weight:700; white-space:nowrap;}\n  .big-price{font-size:29px; font-weight:700; line-height:1; white-space:nowrap;}\n  .head-stats{display:flex; align-items:center; gap:20px; flex-wrap:wrap; margin-left:auto;}\n  .hstat{font-size:13px; color:var(--ink); font-weight:600; white-space:nowrap;}\n  .hstat span{color:var(--gray); font-weight:400; margin-right:5px;}\n  /* ---- 币种页签：星标 pill + 涨跌（对图1） ---- */\n  .tabs{display:flex; gap:8px; flex-wrap:wrap; margin-top:14px;}\n  .tabs button{border:1px solid var(--border); background:#fff; color:var(--ink); border-radius:999px;\n    padding:6px 14px; font-size:13px; font-weight:600; cursor:pointer; font-family:inherit; display:flex; gap:6px; align-items:center;}\n  .tabs button .star{color:#F5B800; font-size:12px;}\n  .tabs button .pct{font-size:12px; font-weight:600;}\n  .tabs button.on{background:var(--selected); border-color:var(--selected); color:#fff;}\n  .tabs button.on .star{color:#fff;}\n  .tabs button.on .pct{color:#fff !important;}\n  /* ---- 时间戳 ---- */\n  .stamps{display:flex; gap:18px; margin-top:10px; font-size:12px; color:var(--gray); flex-wrap:wrap;}\n  /* ---- 多空总量：标签在条外（对图1） ---- */\n  .totals-row{display:flex; align-items:center; gap:12px; margin-top:12px;}\n  .totals-lab{font-size:13px; font-weight:700; white-space:nowrap;}\n  .totals-lab.long{color:var(--long);} .totals-lab.short{color:var(--short);}\n  .totals{flex:1; display:flex; height:15px; border-radius:4px; overflow:hidden; background:#F0F3F5;}\n  .totals .seg{height:100%; transition:width .3s;}\n  .totals .seg.long{background:var(--long);} .totals .seg.short{background:var(--short);}\n  .totals-note{margin-top:7px; font-size:12px; color:var(--gray);}\n  /* ---- 读数行（图在工具栏上方，对图1） ---- */\n  .readout{margin-top:12px; font-size:12.5px; color:var(--gray); line-height:1.9; min-height:44px;}\n  .readout b{color:var(--ink); font-weight:600;}\n  .readout .dim{color:var(--axis-gray);}\n  /* ---- 工具栏 ---- */\n  .toolbar{display:flex; align-items:center; gap:10px; margin-top:6px; flex-wrap:wrap;}\n  .seg-ctl{display:flex; align-items:center; border:1px solid var(--border); border-radius:8px; overflow:hidden;}\n  .seg-ctl button{border:0; background:#fff; padding:5px 12px; font-size:13px; cursor:pointer; color:var(--ink); font-family:inherit;}\n  .seg-ctl button.on{background:var(--selected); color:#fff; font-weight:600;}\n  .seg-ctl .grp{padding:5px 4px 5px 10px; font-size:13px; color:var(--gray);}\n  .zoom-btn{width:28px; height:28px; border:1px solid var(--border); background:#fff; border-radius:8px;\n    font-size:15px; cursor:pointer; color:var(--ink); font-family:inherit; line-height:1;}\n  .legend-note{font-size:12px; color:var(--gray); display:flex; align-items:center; gap:6px;}\n  .legend-note i{width:13px; height:10px; border-radius:2px; display:inline-block;}\n  .spacer{flex:1;}\n  /* ---- 图表 + 阶梯 ---- */\n  .chart-flex{display:flex; margin-top:8px; border:1px solid var(--border); border-radius:10px; overflow:hidden;}\n  .chart-wrap{position:relative; flex:1; min-width:0;}\n  canvas{display:block; width:100%; cursor:grab; touch-action:none; outline:none;}\n  canvas.dragging{cursor:grabbing;}\n  .ladder{width:264px; flex:none; border-left:1px solid var(--border); position:relative; background:#FCFDFE; overflow:hidden;}\n  .ladder h4{font-size:12px; color:var(--gray); font-weight:600; padding:8px 10px 0; display:flex; justify-content:space-between;}\n  .ladder .bar{position:absolute; left:6px; height:18px; border-radius:4px; display:flex; align-items:center;\n    padding:0 7px; font-size:11px; font-weight:600; color:#fff; overflow:hidden; white-space:nowrap;}\n  .ladder .tick{position:absolute; right:5px; font-size:10px; color:var(--axis-gray); white-space:nowrap; transform:translateY(-50%);}\n  .home-btn{position:absolute; right:108px; bottom:34px; z-index:5; border:1px solid var(--border); background:#fff;\n    border-radius:8px; padding:6px 12px; font-size:12px; font-weight:600; cursor:pointer; box-shadow:0 2px 8px rgba(31,45,56,.12); font-family:inherit;}\n  .hover-card{position:absolute; z-index:6; width:204px; padding:7px 10px; background:rgba(255,255,255,.97);\n    border:1px solid var(--short); border-radius:8px; box-shadow:0 4px 16px rgba(31,45,56,.16);\n    pointer-events:none; line-height:1.5; display:none;}\n  .hover-card .r{text-align:center; white-space:nowrap;}\n  .hover-card .lab{color:#8A9BA3; font-size:12px; font-weight:400;}\n  /* ---- 下方两列簇表 ---- */\n  .lists{display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-top:16px;}\n  @media (max-width:760px){ .lists{grid-template-columns:1fr;} .ladder{display:none;} }\n  .list-box{border:1px solid var(--border); border-radius:10px; overflow:hidden;}\n  .list-box h3{font-size:14px; padding:10px 14px; border-bottom:1px solid var(--border);}\n  .list-box table{width:100%; border-collapse:collapse; font-size:13px;}\n  .list-box td,.list-box th{padding:7px 14px; border-bottom:1px solid #F0F3F5; text-align:right;}\n  .list-box td:first-child,.list-box th:first-child{text-align:left;}\n  .list-box th{color:var(--gray); font-weight:500; font-size:12px;}\n  .list-box .empty{padding:16px; color:var(--gray); font-size:13px; text-align:center;}\n  .dot{display:inline-block; width:9px; height:9px; border-radius:2px; margin-right:7px; vertical-align:1px;}\n  footer{margin-top:16px; font-size:12px; color:var(--gray); line-height:1.7; border-top:1px solid var(--border); padding-top:10px;}\n</style>\n</head>\n<body>\n<div class=\"wrap\">\n  <div class=\"head1\">\n    <div class=\"pair-name\" id=\"pairName\">BTC / USDC</div>\n    <div class=\"big-price\" id=\"bigPrice\">—</div>\n    <div class=\"head-stats\">\n      <div class=\"hstat\"><span>24h 涨跌</span><b id=\"hsChg\">—</b></div>\n      <div class=\"hstat\"><span>24h 高</span><b id=\"hsHigh\">—</b></div>\n      <div class=\"hstat\"><span>24h 低</span><b id=\"hsLow\">—</b></div>\n      <div class=\"hstat\"><span>24h 成交额</span><b id=\"hsVol\">—</b></div>\n      <div class=\"hstat\"><span>资金费率 1h</span><b id=\"hsFund\">—</b></div>\n      <div class=\"hstat\"><span>未平仓量 OI</span><b id=\"hsOi\">—</b></div>\n    </div>\n  </div>\n  <div class=\"tabs\" id=\"coinTabs\"></div>\n  <div class=\"stamps\">\n    <span id=\"stampLive\">行情更新于 —</span>\n    <span id=\"stampSnap\">清算快照于 —</span>\n  </div>\n  <div class=\"totals-row\">\n    <div class=\"totals-lab long\" id=\"totLong\">多头清算 —</div>\n    <div class=\"totals\" id=\"totalsBar\"></div>\n    <div class=\"totals-lab short\" id=\"totShort\">空头清算 —</div>\n  </div>\n  <div class=\"totals-note\">上方空头清算密集 = 向上挤压的燃料；下方多头清算贴现价 = 下跌磁铁，容易连环踩踏。</div>\n  <div class=\"readout\" id=\"readout\"></div>\n  <div class=\"toolbar\">\n    <div class=\"seg-ctl\" id=\"tfCtl\"></div>\n    <div class=\"spacer\"></div>\n    <div class=\"seg-ctl\" id=\"modeCtl\">\n      <span class=\"grp\">清算带</span>\n      <button data-mode=\"near\">近端 ±30%</button><button data-mode=\"all\" class=\"on\">全部</button><button data-mode=\"off\">关</button>\n    </div>\n    <button class=\"zoom-btn\" id=\"zoomIn\">+</button>\n    <button class=\"zoom-btn\" id=\"zoomOut\">−</button>\n    <div class=\"legend-note\"><i style=\"background:var(--long);opacity:.4\"></i><i style=\"background:var(--short);opacity:.4\"></i>颜色深浅 = 清算金额</div>\n  </div>\n  <div class=\"chart-flex\">\n    <div class=\"chart-wrap\" id=\"chartWrap\">\n      <canvas id=\"chart\" tabindex=\"0\"></canvas>\n      <button class=\"home-btn\" id=\"homeBtn\" style=\"display:none\">回到最新</button>\n      <div class=\"hover-card\" id=\"hoverCard\"></div>\n    </div>\n    <div class=\"ladder\" id=\"ladder\"><h4><span>清算全景</span><span>-50% / +60%</span></h4></div>\n  </div>\n  <div class=\"lists\">\n    <div class=\"list-box\"><h3 id=\"longHead\">多头清算簇</h3><div id=\"longList\"></div></div>\n    <div class=\"list-box\"><h3 id=\"shortHead\">空头清算簇</h3><div id=\"shortList\"></div></div>\n  </div>\n  <footer>\n    数据来源：清算簇由 Hyperliquid 排行榜前 300 地址与 Lighter 成交发现活跃账户的真实仓位清算价合并生成；Lighter 为样本覆盖、逐轮累积，非全量。\n    币安、OKX、Bybit 等中心化交易所不公开逐仓清算价，无法生成前瞻清算簇，未接入。\n    K 线：Bitfinex 长历史拼接 Hyperliquid 近段（TAO / HYPE / LIT 为 Hyperliquid 可得历史，起点如实标注）。\n  </footer>\n</div>\n<script>\n'use strict';\n/* ================= 常量（与现版看板定版一致） ================= */\nconst COINS = ['BTC','ETH','HYPE','TAO','UNI','LIT'];\nconst TF_LIST = [['15m','15分'],['1h','1时'],['4h','4时'],['1d','日'],['1w','周'],['1M','月']];\nconst COL = { up:'#50B383', down:'#F6465D', long:'#00B16B', short:'#F6465D', shortHover:'#F91F3C',\n  grid:'#E7EEF3', ink:'#1F2D38', axisGray:'#A3B4BC',\n  ema5:'#FF8A3D', ema30:'#F6465D', ema90:'#F5B800', ema200:'#34C759',\n  volMa5:'#26a69a', volMa10:'#E8A33D', dif:'#1F9D63', dea:'#E8930C' };\nconst AXIS_W = 96, AXIS_GAP = 22;\nconst FONT = 'Inter,\"SF Pro Display\",\"Segoe UI\",Roboto,\"Helvetica Neue\",Arial,sans-serif';\n\n/* ================= 状态 ================= */\nconst S = {\n  coin: 'BTC', tf: '1d', bars: [], snap: null, markets: [], candlesTf: {},\n  endX: 0, count: 160, homeCount: 160,\n  hoverI: -1, hoverCluster: null, mouse: null,\n  mode: 'all',\n  mainH: 520, volH: 90, macdH: 110, timeH: 20,\n  liveMark: 0, liveAt: 0, dayHi: 0, dayLo: 0, loadingOlder: false,\n};\nconst cache = new Map(); // `${coin}:${tf}` -> bars\nconst $ = (id) => document.getElementById(id);\nconst canvas = $('chart'), ctx = canvas.getContext('2d');\n\n/* ================= 格式化 ================= */\nconst fmtPrice = (v) => {\n  if (!isFinite(v) || v <= 0) return '—';\n  const d = v >= 1000 ? 0 : v >= 100 ? 1 : v >= 1 ? 2 : 4;\n  return v.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });\n};\nconst fmtM = (v) => `$${(v / 1e6).toFixed(1)}M`;\nconst fmtUsd = (v) => v >= 1e9 ? `$${(v / 1e9).toFixed(2)}B` : v >= 1e6 ? `$${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `$${(v / 1e3).toFixed(1)}K` : `$${v.toFixed(0)}`;\nconst fmtPct = (v, digits = 1) => `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;\nconst fmtVolPlain = (v) => isFinite(v) ? String(Number(v.toFixed(1))) : '—';           // 读数行用量：币本位原值，如 5976.7\nconst fmtVolAxis = (v) => v >= 1e6 ? `${(v / 1e6).toFixed(2)}M` : v >= 1e3 ? `${(v / 1e3).toFixed(2)}k` : `${v.toFixed(0)}`; // 量轴：102.20k 口径\nconst fmtAxisNum = (v) => v.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }); // MACD 轴：4,146.9 口径\nconst pad2 = (n) => String(n).padStart(2, '0');\nconst fmtClock = (ms) => { const d = new Date(ms); return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`; };\nconst fmtSnapTime = (ms) => { const d = new Date(ms); return `${d.getMonth() + 1}/${d.getDate()} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`; };\nconst barTime = (t) => {\n  const d = new Date(t);\n  return S.tf === '1d' || S.tf === '1w' || S.tf === '1M'\n    ? `${d.getMonth() + 1}/${d.getDate()}`\n    : d.toLocaleString('zh-CN', { hour12: false, month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' });\n};\n\n/* ================= 指标 ================= */\nfunction emaSeries(vals, n) {\n  const out = new Array(vals.length).fill(NaN), k = 2 / (n + 1);\n  let e = NaN;\n  for (let i = 0; i < vals.length; i++) {\n    e = isNaN(e) ? vals[i] : vals[i] * k + e * (1 - k);\n    out[i] = i >= n - 1 ? e : NaN;\n  }\n  return out;\n}\nlet IND = null;\nfunction computeIndicators() {\n  const b = S.bars, closes = b.map((x) => x.c), vols = b.map((x) => x.v);\n  const sma = (arr, n) => arr.map((_, i) => i >= n - 1 ? arr.slice(i - n + 1, i + 1).reduce((s, v) => s + v, 0) / n : NaN);\n  const e12 = emaSeries(closes, 12), e26 = emaSeries(closes, 26);\n  const dif = closes.map((_, i) => isNaN(e12[i]) || isNaN(e26[i]) ? NaN : e12[i] - e26[i]);\n  const deaIn = dif.map((v) => isNaN(v) ? 0 : v);\n  const deaRaw = emaSeries(deaIn, 9);\n  const dea = dif.map((v, i) => isNaN(v) ? NaN : deaRaw[i]);\n  IND = {\n    ema5: emaSeries(closes, 5), ema30: emaSeries(closes, 30),\n    ema90: emaSeries(closes, 90), ema200: emaSeries(closes, 200),\n    volMa5: sma(vols, 5), volMa10: sma(vols, 10),\n    dif, dea, hist: dif.map((v, i) => isNaN(v) || isNaN(dea[i]) ? NaN : 2 * (v - dea[i])),\n  };\n}\n\n/* ================= 数据 ================= */\nasync function getJSON(u, opts) { const r = await fetch(u, opts); if (!r.ok) throw new Error(u); return r.json(); }\nasync function loadDashboard() {\n  const d = await getJSON(`/api/dashboard?coin=${S.coin}`);\n  S.markets = d.markets || []; S.snap = d.snapshot; S.candlesTf = d.candlesTf || {};\n  renderHeader(); renderLists(); renderLadder();\n}\nlet loadGen = 0; // 切换竞态代号：慢返回的旧周期数据不许覆盖新周期\nasync function loadCandles(before) {\n  const key = `${S.coin}:${S.tf}`;\n  if (!before && cache.has(key)) { S.bars = cache.get(key); computeIndicators(); return; } // 缓存命中也必须重算指标（旧版漏算 → 切回时 EMA/MACD 读旧序列）\n  const gen = before ? loadGen : ++loadGen;\n  const barsRef = S.bars;\n  const q = before ? `&before=${before}&limit=1500` : '&limit=1500';\n  const d = await getJSON(`/api/candles?coin=${S.coin}&tf=${S.tf}${q}`);\n  if (gen !== loadGen || (before && S.bars !== barsRef)) return; // 期间已切换，丢弃\n  const rows = (d.bars || []).filter((b) => b.o > 0 && b.h > 0 && b.l > 0 && b.c > 0).map((b) => ({ ...b }));\n  if (before) {\n    const have = new Set(S.bars.map((b) => b.t));\n    const add = rows.filter((b) => !have.has(b.t));\n    if (add.length) { S.bars = [...add, ...S.bars]; S.endX += add.length; }\n  } else {\n    S.bars = rows;\n  }\n  cache.set(key, S.bars);\n  computeIndicators();\n}\nconst HL_POST = (body) => getJSON('https://api.hyperliquid.xyz/info', {\n  method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),\n});\nasync function hlLive() {\n  try {\n    const [meta, ctxs] = await HL_POST({ type: 'metaAndAssetCtxs' });\n    S.markets = meta.universe.map((u, i) => {\n      if (!COINS.includes(u.name)) return null;\n      const c = ctxs[i] || {}, markPx = Number(c.markPx);\n      return { coin: u.name, market: { markPx, prevDayPx: Number(c.prevDayPx),\n        changePct: markPx && Number(c.prevDayPx) ? (markPx / Number(c.prevDayPx) - 1) * 100 : 0,\n        dayVolumeUsd: Number(c.dayNtlVlm), openInterestUsd: Number(c.openInterest) * markPx, fundingRate: Number(c.funding) } };\n    }).filter(Boolean).sort((a, b) => COINS.indexOf(a.coin) - COINS.indexOf(b.coin));\n    const mine = S.markets.find((m) => m.coin === S.coin);\n    if (mine) S.liveMark = mine.market.markPx;\n    S.liveAt = Date.now();\n    renderHeader();\n  } catch { /* 行情层失败时保留上次显示 */ }\n  try {\n    // 24h 高/低：HL 行情接口不直接给，用近 24 根 1h K 线现算\n    const hrs = await HL_POST({ type: 'candleSnapshot', req: { coin: S.coin, interval: '1h', startTime: Date.now() - 24 * 3600e3, endTime: Date.now() } });\n    if (Array.isArray(hrs) && hrs.length) {\n      S.dayHi = Math.max(...hrs.map((c) => +c.h));\n      S.dayLo = Math.min(...hrs.map((c) => +c.l));\n      renderHeader();\n    }\n  } catch { /* 同上 */ }\n  try {\n    const last = S.bars[S.bars.length - 1];\n    if (last) {\n      const reqCoin = S.coin, reqTf = S.tf;\n      const j = await HL_POST({ type: 'candleSnapshot', req: { coin: reqCoin, interval: reqTf, startTime: last.t, endTime: Date.now() } });\n      if (reqCoin !== S.coin || reqTf !== S.tf) return; // 请求期间已切换：这批 K 线不属于当前序列，丢弃防串档\n      const wasHome = isHome();\n      for (const c of j || []) {\n        // 只更新价、不动量：HL 的量是永续口径（万级 BTC/日），与存储的 Bitfinex 现货量（千级）\n        // 不是一个尺度，混用会把量窗格的标尺冲爆（图2 巨型量柱的根源）。量以存储序列为准、随快照刷新。\n        if (!(+c.o > 0 && +c.h > 0 && +c.l > 0 && +c.c > 0)) continue;\n        const nb = { t: c.t, o: +c.o, h: +c.h, l: +c.l, c: +c.c, v: NaN };\n        const tail = S.bars[S.bars.length - 1];\n        if (tail && tail.t === nb.t) { nb.v = tail.v; S.bars[S.bars.length - 1] = nb; }\n        else if (!tail || nb.t > tail.t) { nb.v = tail ? tail.v : +c.v; S.bars.push(nb); }\n      }\n      if (wasHome) goHome(false);\n      computeIndicators(); draw();\n    }\n  } catch { /* 同上 */ }\n}\n\n/* ================= 头部 / 页签 / 总量 / 列表 ================= */\nfunction marketOf(coin) { return (S.markets.find((x) => x.coin === coin) || {}).market || {}; }\nfunction renderHeader() {\n  const m = marketOf(S.coin);\n  const px = S.liveMark || m.markPx || (S.snap && S.snap.markPx) || 0;\n  $('pairName').textContent = `${S.coin} / USDC`;\n  const bp = $('bigPrice');\n  bp.textContent = px ? fmtPrice(px) : '—';\n  bp.style.color = isFinite(m.changePct) ? (m.changePct >= 0 ? COL.up : COL.down) : COL.ink;\n  const hc = $('hsChg');\n  if (isFinite(m.changePct)) { hc.textContent = fmtPct(m.changePct, 2); hc.style.color = m.changePct >= 0 ? COL.up : COL.down; }\n  else hc.textContent = '—';\n  $('hsHigh').textContent = S.dayHi ? fmtPrice(S.dayHi) : '—';\n  $('hsLow').textContent = S.dayLo ? fmtPrice(S.dayLo) : '—';\n  $('hsVol').textContent = m.dayVolumeUsd ? fmtUsd(m.dayVolumeUsd) : '—';\n  $('hsFund').textContent = isFinite(m.fundingRate) ? `${(m.fundingRate * 100).toFixed(4)}%` : '—';\n  $('hsOi').textContent = m.openInterestUsd ? fmtUsd(m.openInterestUsd) : '—';\n  // 页签涨跌\n  document.querySelectorAll('#coinTabs button').forEach((b) => {\n    const mm = marketOf(b.dataset.coin), pct = b.querySelector('.pct');\n    b.classList.toggle('on', b.dataset.coin === S.coin);\n    if (pct) {\n      pct.textContent = isFinite(mm.changePct) ? fmtPct(mm.changePct, 1) : '';\n      pct.style.color = isFinite(mm.changePct) ? (mm.changePct >= 0 ? COL.up : COL.down) : '';\n    }\n  });\n  // 多空总量（标签在条外）\n  const bar = $('totalsBar');\n  if (S.snap && (S.snap.totals.longLiqUsd + S.snap.totals.shortLiqUsd) > 0) {\n    const L = S.snap.totals.longLiqUsd, Sh = S.snap.totals.shortLiqUsd, tot = L + Sh;\n    $('totLong').textContent = `多头清算 ${fmtUsd(L)}`;\n    $('totShort').textContent = `空头清算 ${fmtUsd(Sh)}`;\n    bar.innerHTML = `<div class=\"seg long\" style=\"width:${(L / tot * 100).toFixed(2)}%\"></div>` +\n      `<div class=\"seg short\" style=\"width:${(Sh / tot * 100).toFixed(2)}%\"></div>`;\n  } else {\n    $('totLong').textContent = '多头清算 —'; $('totShort').textContent = '空头清算 —'; bar.innerHTML = '';\n  }\n  $('stampLive').textContent = `行情更新于 ${S.liveAt ? fmtClock(S.liveAt) : '—'}（价格 / K 线 · 每 10 秒）`;\n  $('stampSnap').textContent = `清算快照于 ${S.snap ? fmtSnapTime(Date.parse(S.snap.fetchedAt)) : '—（积累中）'}（每小时自动重扫 · 打开/切回页面时自动校验）`;\n}\nfunction clusterRows(side) {\n  if (!S.snap) return [];\n  return S.snap.clusters.filter((c) => c.side === side);\n}\nfunction renderLists() {\n  const mk = (side, headEl, listEl, color) => {\n    const rows = clusterRows(side);\n    const sum = rows.reduce((s, c) => s + c.totalUsd, 0);\n    $(headEl).innerHTML = `${side === 'long' ? '多头' : '空头'}清算簇（${fmtM(sum)}）`;\n    if (!rows.length) { $(listEl).innerHTML = '<div class=\"empty\">暂无成簇数据</div>'; return; }\n    $(listEl).innerHTML = '<table><tr><th>价位</th><th>金额</th><th>鲸鱼</th><th>距现价</th></tr>' +\n      rows.map((c) => `<tr><td><span class=\"dot\" style=\"background:${color}\"></span>${fmtPrice(c.lowPx)} – ${fmtPrice(c.highPx)}</td>` +\n        `<td>${fmtM(c.totalUsd)}</td><td>${c.whaleCount}</td><td>${fmtPct(c.distPct)}</td></tr>`).join('') + '</table>';\n  };\n  mk('long', 'longHead', 'longList', COL.long);\n  mk('short', 'shortHead', 'shortList', COL.short);\n}\nconst blockAlpha = (amt, maxAmt) => Math.min(0.54, 0.30 + 0.24 * Math.sqrt(maxAmt > 0 ? amt / maxAmt : 0));\n/* 全景阶梯：统一细条（18px）按距现价定位、近簇自动错行不重叠；右侧 % 刻度带价（对图1） */\nfunction renderLadder() {\n  const el = $('ladder');\n  el.querySelectorAll('.bar,.tick').forEach((n) => n.remove());\n  const H = S.mainH + S.volH + S.macdH + S.timeH + 8;\n  el.style.height = H + 'px';\n  if (!S.snap || !S.snap.markPx) return;\n  const mark = S.snap.markPx, HEAD = 30;\n  const yOf = (pct) => HEAD + (60 - pct) / 110 * (H - HEAD - 8);\n  for (const pct of [60, 40, 20, 0, -20, -40]) {\n    const t = document.createElement('div');\n    t.className = 'tick';\n    t.style.top = yOf(pct) + 'px';\n    t.textContent = `${pct > 0 ? '+' : ''}${pct}% · ${fmtPrice(mark * (1 + pct / 100))}`;\n    el.appendChild(t);\n  }\n  const rows = S.snap.clusters\n    .filter((c) => c.distPct >= -50 && c.distPct <= 60)\n    .sort((a, b) => b.distPct - a.distPct);\n  const maxAmt = Math.max(1, ...rows.map((c) => c.totalUsd));\n  // 条宽：金额 √ 标尺打底，但不得窄于标签文本宽（11px 粗体实测），上限给右侧 % 刻度让位\n  const meas = document.createElement('canvas').getContext('2d');\n  meas.font = `600 11px ${FONT}`;\n  let lastBottom = -Infinity;\n  for (const c of rows) {\n    let top = yOf(c.distPct) - 9;\n    if (top < lastBottom + 2) top = lastBottom + 2;   // 错行防重叠\n    lastBottom = top + 18;\n    const label = `${c.side === 'short' ? '空' : '多'} ${fmtM(c.totalUsd)} · ${fmtPct(c.distPct)}`;\n    const div = document.createElement('div');\n    div.className = 'bar';\n    const base = c.side === 'long' ? '0,177,107' : '246,70,93';\n    const a = 0.55 + 0.30 * Math.sqrt(c.totalUsd / maxAmt);\n    div.style.background = `rgba(${base},${a.toFixed(2)})`;\n    div.style.top = top + 'px';\n    const amtW = 34 + 138 * Math.sqrt(c.totalUsd / maxAmt);\n    div.style.width = `${Math.min(178, Math.max(amtW, meas.measureText(label).width + 16)).toFixed(0)}px`;\n    div.textContent = label;\n    el.appendChild(div);\n  }\n}\n\n/* ================= 读数行（两行，对图1） ================= */\nfunction updateReadout(i) {\n  const el = $('readout');\n  if (!S.bars.length) { el.innerHTML = ''; return; }\n  const idx = i >= 0 ? i : S.bars.length - 1;\n  const b = S.bars[idx], prev = S.bars[idx - 1];\n  const chg = prev ? (b.c / prev.c - 1) * 100 : 0;\n  const amp = (b.h - b.l) / b.l * 100;\n  const ema = (arr, col, name) =>\n    `<span style=\"color:${col}\">${name}：</span><b>${arr && isFinite(arr[idx]) ? fmtPrice(arr[idx]) : '—'}</b>`;\n  el.innerHTML =\n    `<div>${barTime(b.t)}&ensp;开 <b>${fmtPrice(b.o)}</b>&ensp;高 <b>${fmtPrice(b.h)}</b>&ensp;低 <b>${fmtPrice(b.l)}</b>&ensp;收 <b>${fmtPrice(b.c)}</b>&ensp;` +\n    `涨幅 <b style=\"color:${chg >= 0 ? COL.up : COL.down}\">${fmtPct(chg, 2)}</b>&ensp;振幅 <b>${amp.toFixed(2)}%</b>&ensp;量 <b>${fmtVolPlain(b.v)}</b>` +\n    `${idx === S.bars.length - 1 ? '&ensp;<span class=\"dim\">（最新一根）</span>' : ''}</div>` +\n    `<div>${ema(IND && IND.ema5, COL.ema5, 'EMA(5)')} ${ema(IND && IND.ema30, COL.ema30, 'EMA(30)')} ` +\n    `${ema(IND && IND.ema90, COL.ema90, 'EMA(90)')} ${ema(IND && IND.ema200, COL.ema200, 'EMA(200)')}` +\n    `&ensp;<span class=\"dim\">均线按当前周期根数计；历史不足 200 根时 EMA(200) 如实缺失</span></div>`;\n}\n\n/* ================= 图表几何与绘制 ================= */\nlet W = 0, H = 0, plotW = 0;\nconst DIV = 4;\nconst paneY = () => ({\n  main: 0, vol: S.mainH + DIV, macd: S.mainH + DIV + S.volH + DIV,\n  time: S.mainH + DIV + S.volH + DIV + S.macdH,\n});\nfunction resize() {\n  const wrap = $('chartWrap');\n  W = wrap.clientWidth; H = S.mainH + S.volH + S.macdH + S.timeH + DIV * 2;\n  plotW = Math.max(200, W - AXIS_W);\n  const dpr = window.devicePixelRatio || 1;\n  canvas.width = Math.round(W * dpr); canvas.height = Math.round(H * dpr);\n  canvas.style.height = H + 'px';\n  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);\n  S.homeCount = Math.min(180, Math.max(110, Math.round(plotW / 9)));\n  renderLadder();\n}\nconst startX = () => S.endX - S.count;\nconst xFor = (i) => (i - startX()) * (plotW / S.count) + (plotW / S.count) / 2;\nconst idxFloatForX = (px) => startX() + px / (plotW / S.count);\nlet scale = null;\nfunction computeScale() {\n  const lo = Math.max(0, Math.floor(startX())), hi = Math.min(S.bars.length, Math.ceil(S.endX));\n  let mn = Infinity, mx = -Infinity;\n  for (let i = lo; i < hi; i++) { mn = Math.min(mn, S.bars[i].l); mx = Math.max(mx, S.bars[i].h); }\n  if (!isFinite(mn)) {\n    const c = S.liveMark || (S.snap && S.snap.markPx) || (S.bars.length ? S.bars[S.bars.length - 1].c : 100);\n    mn = c * 0.92; mx = c * 1.08;\n  }\n  // 留白在对数空间里加：线性留白在宽幅序列（月线 850→126,000）上会把 mn 垫成负数，\n  // log10 被钳到 1e-9 后轴面爆炸（2026-11-12 周/月线事故的根）\n  const la0 = Math.log10(Math.max(mn, 1e-9)), lb0 = Math.log10(mx);\n  const lpad = (lb0 - la0) * 0.04 || 0.02;\n  const a = la0 - lpad, b = lb0 + lpad;\n  mn = Math.pow(10, a); mx = Math.pow(10, b);\n  scale = { y: (p) => (b - Math.log10(Math.max(p, 1e-9))) / (b - a) * S.mainH,\n            inv: (y) => Math.pow(10, b - y / S.mainH * (b - a)), mn, mx, la: a, lb: b };\n}\nfunction displayedClusters() {\n  if (!S.snap || S.mode === 'off') return [];\n  if (S.mode === 'near') return S.snap.clusters.filter((c) => Math.abs(c.distPct) <= 30);\n  return S.snap.clusters;\n}\nconst rgba = (hex, a) => {\n  const n = parseInt(hex.slice(1), 16);\n  return `rgba(${(n >> 16) & 255},${(n >> 8) & 255},${n & 255},${a})`;\n};\nfunction drawLine(series, color, vScale) {\n  ctx.strokeStyle = color; ctx.lineWidth = 1; ctx.globalAlpha = 0.9; ctx.beginPath();\n  let started = false;\n  const lo = Math.max(0, Math.floor(startX())), hi = Math.min(S.bars.length, Math.ceil(S.endX));\n  for (let i = lo; i < hi; i++) {\n    const v = series[i];\n    if (!isFinite(v)) { started = false; continue; }\n    const x = xFor(i), y = vScale(v);\n    if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);\n  }\n  ctx.stroke(); ctx.globalAlpha = 1;\n}\nfunction draw() {\n  if (!W) return;\n  const py = paneY();\n  ctx.clearRect(0, 0, W, H);\n  ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, W, H);\n  computeScale();\n  const bw = plotW / S.count;\n  const lo = Math.max(0, Math.floor(startX())), hi = Math.min(S.bars.length, Math.ceil(S.endX));\n  /* --- 网格：对数等距约 9 线（对图1），最底层 --- */\n  ctx.strokeStyle = COL.grid; ctx.lineWidth = 1;\n  const NL = 9;\n  ctx.beginPath();\n  for (let k = 0; k < NL; k++) {\n    const y = Math.round(S.mainH * k / (NL - 1)) + 0.5;\n    ctx.moveTo(0, y); ctx.lineTo(plotW, y);\n  }\n  ctx.stroke();\n  ctx.font = `400 13px ${FONT}`; ctx.fillStyle = COL.ink; ctx.textBaseline = 'middle'; ctx.textAlign = 'left';\n  for (let k = 0; k < NL; k++) {\n    const frac = k / (NL - 1);\n    const p = Math.pow(10, scale.lb - frac * (scale.lb - scale.la));\n    const ly = Math.min(Math.max(S.mainH * frac, 8), S.mainH - 8); // 首/末标签钳进画布，防顶缘裁切、与量轴标签留距\n    ctx.fillText(fmtPrice(p), plotW + AXIS_GAP, ly);\n  }\n  // 时间竖线（与时间轴刻度对齐，贯穿三窗格）\n  const tickBars = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000].find((n) => n * bw >= 110) || 2000;\n  ctx.beginPath();\n  for (let i = Math.ceil(startX() / tickBars) * tickBars; i < S.endX; i += tickBars) {\n    if (i < 0 || i >= S.bars.length) continue;\n    const x = Math.round(xFor(i) - bw / 2) + 0.5;\n    ctx.moveTo(x, 0); ctx.lineTo(x, py.time);\n  }\n  ctx.stroke();\n  ctx.font = `400 11px ${FONT}`; ctx.fillStyle = COL.axisGray; ctx.textAlign = 'center';\n  for (let i = Math.ceil(startX() / tickBars) * tickBars; i < S.endX; i += tickBars) {\n    if (i < 0 || i >= S.bars.length) continue;\n    const d = new Date(S.bars[i].t);\n    const label = (S.tf === '1d' || S.tf === '1w' || S.tf === '1M')\n      ? `${d.getFullYear()}-${pad2(d.getMonth() + 1)}${S.tf === '1M' ? '' : '-' + pad2(d.getDate())}`\n      : `${d.getMonth() + 1}-${d.getDate()} ${pad2(d.getHours())}:00`;\n    ctx.fillText(label, Math.min(Math.max(xFor(i), 30), plotW - 30), py.time + S.timeH / 2);\n  }\n  ctx.textAlign = 'left';\n  /* --- 清算色块（平填、真实价位 ∩ 可见区） --- */\n  const clusters = displayedClusters();\n  const maxAmt = Math.max(1, ...clusters.map((c) => c.totalUsd));\n  for (const c of clusters) {\n    const hov = S.hoverCluster === c;\n    const color = hov && c.side === 'short' ? COL.shortHover : c.side === 'long' ? COL.long : COL.short;\n    const alpha = hov ? (c.side === 'short' ? Math.min(0.64, blockAlpha(c.totalUsd, maxAmt) + 0.10) : 0.6)\n                      : blockAlpha(c.totalUsd, maxAmt);\n    ctx.fillStyle = rgba(color, alpha);\n    const yTop = scale.y(c.highPx), yBot = scale.y(c.lowPx);\n    if (yBot < 0) ctx.fillRect(0, 0, plotW, 3);                    // 整簇在可见区上方 → 顶部边缘标记\n    else if (yTop > S.mainH) ctx.fillRect(0, S.mainH - 3, plotW, 3); // 整簇在下方 → 底部边缘标记\n    else ctx.fillRect(0, Math.max(0, yTop), plotW, Math.min(S.mainH, yBot) - Math.max(0, yTop));\n  }\n  /* --- K 线 --- */\n  for (let i = lo; i < hi; i++) {\n    const b = S.bars[i], x = xFor(i), up = b.c >= b.o;\n    ctx.strokeStyle = ctx.fillStyle = up ? COL.up : COL.down;\n    ctx.beginPath(); ctx.moveTo(x, scale.y(b.h)); ctx.lineTo(x, scale.y(b.l)); ctx.stroke();\n    const bwBody = Math.max(1, bw * 0.65);\n    if (bw >= 1.2) ctx.fillRect(x - bwBody / 2, scale.y(Math.max(b.o, b.c)), bwBody, Math.max(1, Math.abs(scale.y(b.o) - scale.y(b.c))));\n    else ctx.fillRect(x - 0.5, scale.y(Math.max(b.o, b.c)), 1, Math.max(1, Math.abs(scale.y(b.o) - scale.y(b.c))));\n  }\n  /* --- 均线 --- */\n  if (IND) {\n    drawLine(IND.ema5, COL.ema5, (v) => scale.y(v));\n    drawLine(IND.ema30, COL.ema30, (v) => scale.y(v));\n    drawLine(IND.ema90, COL.ema90, (v) => scale.y(v));\n    drawLine(IND.ema200, COL.ema200, (v) => scale.y(v));\n  }\n  /* --- 成交量窗格：标尺只认可见窗内原值（币本位），柱高钳制在窗内 --- */\n  let vRaw = 0;\n  for (let i = lo; i < hi; i++) vRaw = Math.max(vRaw, S.bars[i].v);\n  const vMax = vRaw * 1.08 || 1;\n  const vY = (v) => py.vol + S.volH - Math.min(1, v / vMax) * S.volH;\n  ctx.strokeStyle = COL.grid; ctx.beginPath(); ctx.moveTo(0, py.vol + 0.5); ctx.lineTo(plotW, py.vol + 0.5); ctx.stroke();\n  for (let i = lo; i < hi; i++) {\n    const b = S.bars[i];\n    ctx.fillStyle = b.c >= b.o ? COL.up : COL.down;\n    const hgt = Math.max(0, Math.min(S.volH, (py.vol + S.volH) - vY(b.v)));\n    ctx.fillRect(xFor(i) - Math.max(0.5, bw * 0.32), py.vol + S.volH - hgt, Math.max(1, bw * 0.65), hgt);\n  }\n  if (IND) { drawLine(IND.volMa5, COL.volMa5, vY); drawLine(IND.volMa10, COL.volMa10, vY); }\n  ctx.font = `400 11px ${FONT}`; ctx.fillStyle = COL.axisGray;\n  ctx.fillText(fmtVolAxis(vRaw), plotW + AXIS_GAP, py.vol + 7);\n  ctx.fillText(fmtVolAxis(vRaw / 2), plotW + AXIS_GAP, py.vol + S.volH / 2);\n  ctx.fillText('0', plotW + AXIS_GAP, py.vol + S.volH - 4);\n  /* --- MACD 窗格：数字轴（对图1） --- */\n  let mRaw = 0;\n  if (IND) for (let i = lo; i < hi; i++) {\n    for (const v of [IND.dif[i], IND.dea[i], IND.hist[i]]) if (isFinite(v)) mRaw = Math.max(mRaw, Math.abs(v));\n  }\n  const mMax = mRaw * 1.1 || 1;\n  const mY = (v) => py.macd + S.macdH / 2 - v / mMax * (S.macdH / 2);\n  ctx.strokeStyle = COL.grid; ctx.beginPath();\n  ctx.moveTo(0, py.macd + 0.5); ctx.lineTo(plotW, py.macd + 0.5);\n  ctx.moveTo(0, Math.round(mY(0)) + 0.5); ctx.lineTo(plotW, Math.round(mY(0)) + 0.5); ctx.stroke();\n  if (IND) {\n    for (let i = lo; i < hi; i++) {\n      const v = IND.hist[i];\n      if (!isFinite(v)) continue;\n      const prev = i > 0 ? IND.hist[i - 1] : NaN;\n      const expanding = !isFinite(prev) || Math.abs(v) >= Math.abs(prev);\n      const x = xFor(i) - Math.max(0.5, bw * 0.32), w = Math.max(1, bw * 0.65);\n      const y1 = mY(Math.max(0, v)), y2 = mY(Math.min(0, v));\n      ctx.strokeStyle = ctx.fillStyle = v >= 0 ? COL.up : COL.down;\n      if (expanding) ctx.fillRect(x, y1, w, Math.max(1, y2 - y1));\n      else ctx.strokeRect(x + 0.5, y1 + 0.5, Math.max(1, w - 1), Math.max(1, y2 - y1 - 1));\n    }\n    drawLine(IND.dif, COL.dif, mY); drawLine(IND.dea, COL.dea, mY);\n  }\n  ctx.fillStyle = COL.axisGray; ctx.font = `400 11px ${FONT}`;\n  if (mRaw > 0) {\n    ctx.fillText(fmtAxisNum(mRaw), plotW + AXIS_GAP, mY(mRaw));\n    ctx.fillText(fmtAxisNum(mRaw / 2), plotW + AXIS_GAP, mY(mRaw / 2));\n    ctx.fillText('0', plotW + AXIS_GAP, mY(0));\n    ctx.fillText(fmtAxisNum(-mRaw / 2), plotW + AXIS_GAP, mY(-mRaw / 2));\n    ctx.fillText(fmtAxisNum(-mRaw), plotW + AXIS_GAP, mY(-mRaw));\n  }\n  /* --- 窗格分隔线 --- */\n  ctx.fillStyle = COL.grid;\n  ctx.fillRect(0, S.mainH, W, 1); ctx.fillRect(0, py.macd - DIV + 3, W, 1); ctx.fillRect(0, py.time, W, 1);\n  /* --- 现价虚线与标签 --- */\n  const lastBar = S.bars[S.bars.length - 1];\n  const livePx = S.liveMark || (lastBar ? lastBar.c : 0);\n  if (livePx > 0) {\n    const y = scale.y(livePx);\n    if (y >= 0 && y <= S.mainH) {\n      const upDay = lastBar ? livePx >= lastBar.o : true;\n      ctx.strokeStyle = upDay ? COL.up : COL.down; ctx.setLineDash([4, 4]);\n      ctx.beginPath(); ctx.moveTo(0, Math.round(y) + 0.5); ctx.lineTo(plotW, Math.round(y) + 0.5); ctx.stroke();\n      ctx.setLineDash([]);\n      ctx.fillStyle = upDay ? COL.up : COL.down;\n      const th = 20;\n      ctx.beginPath(); ctx.roundRect(plotW + 2, y - th / 2, AXIS_W - 6, th, 4); ctx.fill();\n      ctx.fillStyle = '#fff'; ctx.font = `500 14px ${FONT}`; ctx.textAlign = 'center';\n      ctx.fillText(fmtPrice(livePx), plotW + AXIS_W / 2, y);\n      ctx.textAlign = 'left';\n    }\n  }\n  /* --- 十字线 --- */\n  if (S.mouse && S.mouse.inside && !S.mouse.dragging) {\n    ctx.strokeStyle = 'rgba(31,45,56,.35)'; ctx.setLineDash([4, 4]);\n    ctx.beginPath();\n    ctx.moveTo(Math.round(S.mouse.x) + 0.5, 0); ctx.lineTo(Math.round(S.mouse.x) + 0.5, py.time);\n    if (S.mouse.y < S.mainH) { ctx.moveTo(0, Math.round(S.mouse.y) + 0.5); ctx.lineTo(plotW, Math.round(S.mouse.y) + 0.5); }\n    ctx.stroke(); ctx.setLineDash([]);\n  }\n  updateHomeBtn();\n}\n\n/* ================= 悬停卡（定版规格） ================= */\nfunction showCard(c, mx, my) {\n  const card = $('hoverCard');\n  const color = c.side === 'long' ? COL.long : COL.short;\n  card.style.borderColor = color;\n  card.innerHTML =\n    `<div class=\"r\" style=\"font-size:15px;font-weight:700;color:${color}\">${c.side === 'long' ? '多头清算簇' : '空头清算簇'}</div>` +\n    `<div class=\"r\"><span class=\"lab\">距现价 </span><span style=\"font-size:13px;font-weight:600;color:${color}\">${fmtPct(c.distPct)}</span></div>` +\n    `<div class=\"r\"><span class=\"lab\">金额 </span><span style=\"font-size:15px;font-weight:700;color:${color}\">${fmtM(c.totalUsd)}</span>` +\n    `<span style=\"font-size:13px;font-weight:600;color:${color}\"> · ${c.whaleCount} 鲸</span></div>` +\n    `<div class=\"r\"><span class=\"lab\">价位 </span><span style=\"font-size:13px;font-weight:600;color:${color}\">${fmtPrice(c.lowPx)} – ${fmtPrice(c.highPx)}</span></div>`;\n  card.style.display = 'block';\n  const wrapW = $('chartWrap').clientWidth;\n  let left = mx + 16, top = my - 20;\n  if (left + 214 > wrapW) left = mx - 220;\n  top = Math.max(4, Math.min(top, S.mainH - 110));\n  card.style.left = left + 'px'; card.style.top = top + 'px';\n}\nfunction hideCard() { $('hoverCard').style.display = 'none'; }\n\n/* ================= 视图 ================= */\nfunction isHome() {\n  return Math.abs(S.endX - (S.bars.length + 6)) < 0.01 && Math.abs(S.count - S.homeCount) < 0.01;\n}\nfunction goHome(redraw = true) {\n  S.count = S.homeCount;\n  S.endX = S.bars.length + 6; // home：末根 + 固定 6 个空槽\n  if (redraw) draw();\n}\nfunction clampView() {\n  S.count = Math.min(Math.max(S.count, 20), Math.max(200, S.bars.length + 60));\n  const minStart = -S.count, maxStart = S.bars.length; // 两侧各约一屏自由过冲\n  let s = S.endX - S.count;\n  s = Math.min(Math.max(s, minStart), maxStart);\n  S.endX = s + S.count;\n}\nfunction updateHomeBtn() { $('homeBtn').style.display = isHome() ? 'none' : 'block'; }\nfunction zoomBy(factor) {\n  const anchor = startX() + 0.5 * S.count;\n  S.count *= factor;\n  clampView();\n  S.endX = anchor + 0.5 * S.count;\n  clampView(); maybePrefetch(); draw();\n}\nasync function maybePrefetch() {\n  if (S.loadingOlder || !S.bars.length) return;\n  if (startX() > S.count * 2) return;\n  const earliest = S.bars[0].t;\n  const meta = (S.candlesTf || {})[S.tf];\n  if (meta && meta.earliest && earliest <= meta.earliest) return;\n  S.loadingOlder = true;\n  try { await loadCandles(earliest); } catch { /* 忽略，下次再试 */ }\n  S.loadingOlder = false;\n  draw();\n}\n\n/* ================= 交互 ================= */\nlet drag = null;\ncanvas.addEventListener('pointerdown', (e) => {\n  canvas.setPointerCapture(e.pointerId);\n  drag = { x: e.offsetX, y: e.offsetY, endX: S.endX, volH: S.volH, macdH: S.macdH, moved: false,\n           divider: dividerAt(e.offsetY) };\n  canvas.classList.add('dragging');\n});\ncanvas.addEventListener('pointermove', (e) => {\n  if (drag) {\n    const dx = e.offsetX - drag.x, dy = e.offsetY - drag.y;\n    if (Math.abs(dx) + Math.abs(dy) > 3) drag.moved = true;\n    if (drag.divider === 1) { S.volH = Math.min(160, Math.max(50, drag.volH + dy)); resize(); }\n    else if (drag.divider === 2) { S.macdH = Math.min(180, Math.max(60, drag.macdH + dy)); resize(); renderLadder(); }\n    else if (drag.moved) {\n      S.endX = drag.endX - dx / (plotW / S.count); // 抓图跟手：往右拉看以前（内容随手右移）、往左拉看后面；旧版符号反了，与原版当年修过的同一个坑\n      clampView(); maybePrefetch();\n    }\n    S.mouse = { x: e.offsetX, y: e.offsetY, inside: true, dragging: true };\n    draw(); return;\n  }\n  S.mouse = { x: e.offsetX, y: e.offsetY, inside: true, dragging: false };\n  const i = Math.floor(idxFloatForX(e.offsetX));\n  S.hoverI = i >= 0 && i < S.bars.length ? i : -1;\n  updateReadout(S.hoverI);\n  S.hoverCluster = null;\n  if (e.offsetY < S.mainH && scale && S.snap) {\n    const price = scale.inv(e.offsetY);\n    const clusters = displayedClusters();\n    S.hoverCluster = clusters.find((c) => price >= c.lowPx && price <= c.highPx) || null;\n    if (S.hoverCluster) showCard(S.hoverCluster, e.offsetX, e.offsetY);\n    else hideCard();\n  } else hideCard();\n  draw();\n});\nconst endDrag = () => { drag = null; canvas.classList.remove('dragging'); if (S.mouse) S.mouse.dragging = false; draw(); };\ncanvas.addEventListener('pointerup', endDrag);\ncanvas.addEventListener('pointercancel', endDrag);\ncanvas.addEventListener('pointerleave', () => { S.mouse = null; S.hoverCluster = null; hideCard(); updateReadout(-1); draw(); });\nfunction dividerAt(y) {\n  const py = paneY();\n  if (Math.abs(y - (S.mainH + DIV / 2)) <= 5) return 1;\n  if (Math.abs(y - (py.macd - DIV / 2)) <= 5) return 2;\n  return 0;\n}\ncanvas.addEventListener('wheel', (e) => {\n  e.preventDefault();\n  const f = (e.offsetX) / plotW, anchor = startX() + f * S.count;\n  const factor = e.deltaY < 0 ? 0.88 : 1.14;\n  S.count = S.count * factor;\n  clampView();\n  S.endX = anchor + (1 - f) * S.count;\n  clampView(); maybePrefetch(); draw();\n}, { passive: false });\ncanvas.addEventListener('dblclick', () => goHome());\ncanvas.addEventListener('keydown', (e) => {\n  const stepBars = e.shiftKey ? 10 : 1;\n  if (e.key === 'ArrowRight') { S.endX += stepBars; clampView(); draw(); }\n  else if (e.key === 'ArrowLeft') { S.endX -= stepBars; clampView(); maybePrefetch(); draw(); }\n  else if (e.key === 'Home') { e.preventDefault(); goHome(); }\n});\n$('homeBtn').addEventListener('click', () => goHome());\n$('zoomIn').addEventListener('click', () => zoomBy(0.8));\n$('zoomOut').addEventListener('click', () => zoomBy(1.25));\n\n/* ================= 控件 ================= */\nconst tabsEl = $('coinTabs');\nfor (const c of COINS) {\n  const b = document.createElement('button');\n  b.innerHTML = `<span class=\"star\">★</span>${c}<span class=\"pct\"></span>`;\n  b.dataset.coin = c;\n  b.addEventListener('click', async () => { if (S.coin !== c) { S.coin = c; S.liveMark = 0; S.dayHi = 0; S.dayLo = 0; await switchCoin(); } });\n  tabsEl.appendChild(b);\n}\nconst tfEl = $('tfCtl');\nfor (const [tf, label] of TF_LIST) {\n  const b = document.createElement('button');\n  b.textContent = label; b.dataset.tf = tf;\n  if (tf === S.tf) b.classList.add('on');\n  b.addEventListener('click', async () => {\n    if (S.tf === tf) return;\n    S.tf = tf;\n    tfEl.querySelectorAll('button').forEach((x) => x.classList.toggle('on', x.dataset.tf === tf));\n    await loadCandles(); goHome(); updateReadout(-1);\n  });\n  tfEl.appendChild(b);\n}\n$('modeCtl').addEventListener('click', (e) => {\n  const b = e.target.closest('button'); if (!b) return;\n  S.mode = b.dataset.mode;\n  $('modeCtl').querySelectorAll('button').forEach((x) => x.classList.toggle('on', x === b));\n  draw();\n});\nasync function switchCoin() {\n  await loadDashboard();\n  await loadCandles();\n  goHome(); updateReadout(-1);\n}\n\n/* ================= 新鲜度检查：打开时 / 回前台 / 15 分钟兜底 ================= */\nlet lastSnapAt = '';\nasync function checkFresh() {\n  try {\n    const st = await getJSON('/api/status');\n    const t = st.snaps && st.snaps[S.coin];\n    if (t && lastSnapAt && t !== lastSnapAt) { lastSnapAt = t; await loadDashboard(); draw(); }\n    else if (t) lastSnapAt = t;\n  } catch { /* 忽略 */ }\n}\ndocument.addEventListener('visibilitychange', () => { if (!document.hidden) checkFresh(); });\n\n/* ================= 启动 ================= */\n(async function init() {\n  resize();\n  window.addEventListener('resize', () => { resize(); draw(); });\n  await loadDashboard();\n  await loadCandles();\n  goHome(); updateReadout(-1);\n  if (S.snap) lastSnapAt = S.snap.fetchedAt;\n  await hlLive(); draw();\n  setInterval(hlLive, 10000);          // 行情层 10 秒轮询（浏览器直连 HL）\n  setInterval(checkFresh, 15 * 60000); // 快照新鲜度 15 分钟兜底\n})();\n</script>\n</body>\n</html>\n";

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
