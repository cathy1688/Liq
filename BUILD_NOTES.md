# BUILD_NOTES — 流动性看板 Cloudflare 原生复刻版

构建日期：2026-11-12。目标：把 Muse artifact「流动性看板」（slug `liu-dong-xing-kan-ban`）的现行规格
1:1 复刻为 Cloudflare 原生项目，经 GitHub 同步部署到 Workers，域名 `liq.bittensor.workers.dev`。

- 推送目标仓库：**`cathy1688/Liq`（公有）**（本目录即仓库根；本次未执行 git push / 未部署，按任务禁区止步于本地成品）
- Worker 名：`liq`（域名靠它：`liq.bittensor.workers.dev`）
- KV namespace：**已创建**，id `d3eadb32b80d4738980f3702c2968acf`，已直接写入 `wrangler.toml`（binding `CACHE`），无占位符待换
- `wrangler.toml` 全文无占位符残留（已 grep 核验）；`src/index.js` 中的 `"__LIQ_INDEX_HTML__"` 是构建注入点
  （UNI 先例同款机制），`dist/index.js` 中已替换为真实 HTML（grep 计数 0）

## 文件图

| 文件 | 作用 |
|---|---|
| `wrangler.toml` | name=liq、account_id、compatibility_date=2026-11-12、crons `*/10 * * * *`、KV binding CACHE（真实 id） |
| `package.json` | scripts：build / deploy（build+wrangler deploy）/ dev / verify；devDependency wrangler ^3 |
| `src/pipeline.js` | 同构管线模块（Worker 与 Node 共用）：HL/Lighter/Bitfinex 客户端、分簇/buckets 纯函数、KV 状态机 step()、runFullRound() |
| `src/index.js` | Worker 入口：`fetch`（前端 + /api/*）+ `scheduled`（推进一阶段，预算 40 子请求） |
| `public/index.html` | 单文件前端（原生 JS + 单 canvas，无框架）；构建时内联进 Worker |
| `scripts/build.mjs` | 构建：剥离 pipeline 的 export → 与 worker 拼接 → 注入 HTML → `dist/index.js`（wrangler main） |
| `tools/local-verify.mjs` | 本地验证：文件 KV 适配器 + 真实公开 API 实跑一整轮并打印各币实数与 K 线元信息 |
| `dist/index.js` | 构建产物（60,985 字节，node --check 通过）；部署前由 build 重新生成 |

## KV 键表（单个 namespace，binding CACHE）

| 键 | 内容 |
|---|---|
| `snap:<COIN>` | 已发布快照：coin/fetchedAt/markPx/totals/clusters/buckets/sources/markets |
| `candles:<COIN>:<tf>` | `{ bars: [[t,o,h,l,c,v],...] }` 升序全量序列（追加式、无上限） |
| `candles:meta` | `{ updatedAt, coins: { COIN: { tf: {count, earliest, latest} } } }` |
| `lighter:watchlist` | `{ "<accountIndex>": lastSeenMs }`，成交流采集、跨轮持久累积 |
| `job:state` | 状态机：stage/round/游标/marks/markets/回填进度/lastPublish |
| `stage:addrs` | 本轮 HL top300 地址（按 round 失效重拉） |
| `stage:hlpos` / `stage:ltpos` / `stage:ltstats` | 本轮累积仓位与 Lighter 统计（轮末清空） |

## Cron 阶段表（`*/10 * * * *`，每 tick 推进一个阶段、≤40 外部子请求）

| 阶段 | 做什么 | 子请求/ tick |
|---|---|---|
| harvest | HL metaAndAssetCtxs（marks+markets）+ 6 个 Lighter 市场 recentTrades 采 account_id 入 watchlist（过滤 ≥10,000,000 哨兵） | 7 |
| hlscan | 首 tick 拉 leaderboard 取 top300（字段 `ethAddress`）；之后每 tick 40 个 clearinghouseState，只收六币且 liquidationPx 非空的仓位 | ≤40 |
| lighterpoll | watchlist 按 lastSeen 倒序取 ≤300，每 tick 40 个 account；只留六币、\|position_value\|≥$10,000，再过清算价合理性过滤（多头 0.25×mark ≤ liq < mark；空头 mark < liq ≤ 3×mark；liq=0 拒） | ≤40 |
| finalize | 合并 HL+Lighter 仓位 → totals/分簇/buckets → 写六币 `snap:*` | 1（meta 已在 harvest 取） |
| candles | 36 个 (coin,tf) 轮转：未回填的走 Bitfinex 分页（每访 ≤4 页、每页落盘）或 HL 一次取全（TAO/HYPE/LIT）；已回填的用 HL 尾段追加/修未收盘根 | ≤40 |

发布保底：任意阶段边界上，距上次发布 ≥55 分钟且已有累积仓位 → 先补发一版（不清空累积，
sources 里的 scannedAccounts/polled 如实反映覆盖度），保证快照对外每小时至少一版。
分簇规则：按距现价的 1% 价格格归格、同侧相邻格合并成带（带边 = 格边 mark×(1+k%)，
每带 ≥1% 宽、distPct = 带中心 %；口径据原版发布快照反推核实，2026-11-12）；保留「簇额 ≥$3M 或该侧金额 top5」，
每侧 ≤8 簇；totals = 全部仓位名义额分侧合计（未成簇小仓位也计入，与现版一致）；
buckets = −50%~+60%、步长 2.5% 的多空金额网格（阶梯数据源）。

## Cloudflare Git 同步部署（建议设置）

仓库 `cathy1688/Liq` 连接到 Workers 后（Workers & Pages → Create/Connect Git）：

- Root directory：仓库根 `/`
- Build command：`npm run build`
- Deploy command：`npx wrangler deploy`
- 变量/绑定：无需在控制台另配，`wrangler.toml` 已含 account_id、KV id、cron
- 首次部署后可用 `POST /api/step` 连打若干次加速第一轮（每次推进一阶段），或等 cron 自然推进

## 验证实数（2026-11-12 本地实跑，Node + 文件 KV + 真实公开 API）

语法：`node --check` 对 src/pipeline.js、src/index.js、dist/index.js、前端内嵌 script（提取后）全部通过。
Worker 层冒烟（fake KV 调 dist 的 fetch handler）：/api/status、/（HTML 36,143 字节）、/api/candles、
/api/dashboard 均 200 形状正确；`POST /api/step` 实跑 harvest 成功（watchlist=167）。

整轮实跑（store=/tmp/liq-verify-store，212.6 秒，纯顺序请求）：
harvest 7 子请求（watchlist 178）→ hlscan 301 子请求（top300 全扫、六币 HL 仓位共 214 笔）→
lighterpoll 178 轮询（found 130 / accepted 42 / rejected 88）→ finalize 发布 → candles 63 子请求。

| 币 | 仓位（HL+Lighter） | 簇 | 多头总量 | 空头总量 |
|---|---|---|---|---|
| BTC | 88（72+16） | 16 | $237.8M | $599.2M |
| ETH | 55（42+13） | 14 | $98.4M | $273.8M |
| HYPE | 60（51+9） | 15 | $80.4M | $188.2M |
| TAO | 17（16+1） | 5 | $0.0M | $6.7M |
| UNI | 17（17+0） | 5 | $0.0M | $3.3M |
| LIT | 19（16+3） | 7 | $1.6M | $10.8M |

与现版 artifact 对照（artifact 2026-11-12 ~15:34 CST 合并快照，BTC：75 笔（HL 59 + Lighter 16）、
15 簇、多 $224.78M / 空 $601.58M；TAO：10 笔、5 簇、空 $6.66M；UNI 空 $3.18M）：
本轮 BTC 空头总量 −0.4%、多头 +5.8%，TAO 空头 $6.7M vs $6.66M、UNI 空头 $3.3M vs $3.18M，
量级与结构一致；差异来自约 1.3 小时的时间漂移（top300 构成与仓位实时变化）及 watchlist
累积程度不同（现版已累积多日，本轮为全新 178 个）。BTC 的 Lighter 接受数两边同为 16 笔。

K 线回填抽查（修复后单独跑 candles 阶段）：BTC 日线 1,597 根（从 2022-06-30）、周线 399 根、
月线 118 根；TAO 日线 998 根（从 2024-02-19，与现版**逐根同数同起点**）；HYPE 日线 708 根
（从 2024-12-05，现版 707，多 1 根为当日新根）；UNI 日线 1,597 根、周线 322 根（从 2020-09-17）、
月线 75 根；LIT 日线 326 根（从 2025-12-22）。15m/1h/4h 首访各回填约 4,000 根，后续轮次继续
向 2022-11-11 推进（小周期全程约 14 万根，按 4 页/访节奏需多轮收敛，属预期行为）。

构建期踩坑与修正（均已实测）：
1. HL leaderboard 行字段是 `ethAddress` 不是 `address`（首轮曾因此扫到 0 地址，已修）。
2. Bitfinex 对 1D/7D/1M 单页跨度过大返回 500：分页根数按周期收窄（1d 400 / 1w 200 / 1M 120；
   15m/1h/4h 保持 1000），实测可用。
3. Bitfinex 回填 end 取未来时刻触发 `10020 time_interval: invalid`：游标改用 now()。
4. 短页 ≠ 数据尽头（无成交缺口）：完成判据改为「空页或抵达 2022-11-11」；且每页落盘，
   中途 429 不丢进度、不留洞。

## 与现版规格的偏差清单（目标零偏差，以下为如实申报）

1. **分簇口径（2026-11-12 已修正对齐）**：原实现用「相邻清算价间隔 ≤ 现价 1%」链式并簇、带边取
   实际清算价，会产生大量零宽单鲸簇（图上只剩发丝线，用户斥「难看」）。后据原版 artifact
   2026-11-12 17:05 发布快照的 clusters 实数反推核实：原版为 1% 价格格分区（带边恰为 mark×(1+k%)
   整数百分比、distPct 为带中心），已改为同口径实现（见 pipeline.js clusterPositions 注释）；
   ≥$3M / top5 / ≤8 的过滤规则不变。
2. **buckets 生成方式**：本实现按任务规格用固定 −50%~+60% / 2.5% 网格；现版 artifact 的
   buckets 疑为自适应分箱且覆盖窗外远端仓位。前端阶梯实际由 clusters 渲染（与现版视觉一致），
   buckets 仅作 API 数据面，故视觉零偏差、数据面口径不同。
3. **MACD 柱倍数**：用国内看盘惯例 2×(DIF−DEA)；现版内部倍数不可见，若现版为 1× 则柱高差一倍，
   DIF/DEA 线本身无差异。
4. **日线回填深度略超**：BTC/UNI 日线 1,597 根（2022-06-30 起）vs 现版 1,461 根（2022-11-11 起）
   ——分页边界过冲造成的超集，不是缺失；前端默认视图与现版一致。
5. **dashboard 的 markets 新鲜度**：快照内 markets 为发布时间的 HL 行情（现版 getDashboard 为
   请求时现拉）；前端头部现价/统计由浏览器 10 秒直连 HL 轮询覆盖，显示口径与现版一致，
   仅 /api/dashboard 的原始消费者会看到发布时刻的 markets。
6. **sources.lighter 的 found/rejected**：为整轮全局计数（现版快照内亦为全局口径），accepted
   为分币计数；轮询统计维度与现版一致到这个粒度。
7. **首轮收敛时间**：受「每阶段 ≤40 子请求」架构约束，第一版完整快照约在部署后 ~3 小时内
   出现（55 分钟保底会先发布部分覆盖版）；现版 artifact 的刷新为单次全量执行，无此冷启动过程。
   可用 `POST /api/step` 手动连打加速。
8. **ETH 日/周线在本地验证时点未完成**：测试突发流量触发 Bitfinex 429，管线已改为每页落盘 +
   页间 120ms，部署后下轮自动续完；这是验证时点状态，不是口径偏差。
9. HYPE 月线首根为 2024-11-13（HL 原生月线时间戳如此，现版日线口径从 2024-12-05 起）；
   数据为 HL 原样，未做人为裁剪。

其余规格（白色主题、1800 容器、页签顺序、30px 大价格、双时间戳、无「仓位来源」行、页脚注记、
色块平填与透明度函数、悬停色与悬停卡四行、EMA/蜡烛/MACD/量线全部色值、网格 #E7EEF3、价格轴
Inter 13px/400/#1F2D38 与 96px 轴宽、拖动方向与自由过冲、home 末根+6 空槽、滚轮光标锚缩放、
默认可见根数公式、对数轴、双列簇列表表头口径、阶梯标签居中且无现价标记、无手动刷新按钮、
快照新鲜度三路检查）均按父摘要现行定版实现，前端代码内有逐项对应常量可查。
