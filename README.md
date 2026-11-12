# 流动性看板（liq）

Cloudflare Worker + KV + Cron 的清算簇看板：Hyperliquid 排行榜前 300 地址的鲸鱼仓位与
Lighter 成交发现活跃账户的样本仓位，按真实清算价合并成分簇图谱。币种：BTC / ETH / HYPE / TAO / UNI / LIT。

- 线上域名：`liq.bittensor.workers.dev`（Worker 名 `liq`）
- 仓库：`cathy1688/Liq`（公有），Cloudflare Git 同步部署
- 工程与口径细节见 [BUILD_NOTES.md](./BUILD_NOTES.md)

## 结构

| 路径 | 说明 |
|---|---|
| `src/pipeline.js` | 同构数据管线：HL 扫描 / Lighter 名单与轮询 / 分簇 / K 线回填（状态机分阶段） |
| `src/index.js` | Worker 入口：前端托管 + `/api/*` + scheduled 推进一阶段 |
| `public/index.html` | 单文件前端（原生 JS + canvas，无框架无构建依赖） |
| `scripts/build.mjs` | 构建：合并 pipeline+worker 并内联 HTML → `dist/index.js` |
| `tools/local-verify.mjs` | 本地验证：文件 KV 适配器实跑一整轮管线并打印实数 |
| `wrangler.toml` | name=liq、KV binding CACHE（已建）、cron `*/10 * * * *` |

## 常用命令

```bash
npm run build     # 生成 dist/index.js
npm run deploy    # build + wrangler deploy
npm run verify    # 本地实跑一轮（需要能直连 HL / Lighter / Bitfinex 公开 API）
```

## API

- `GET /api/dashboard?coin=BTC` → `{ markets, snapshot, candlesTf }`
- `GET /api/candles?coin=BTC&tf=1d&before=<ms>&limit=1500` → K 线窗口
- `GET /api/status` → 各快照/K 线更新时间与状态机进度
- `POST /api/step` → 手动推进一个刷新阶段（排障/播种）

## 数据口径（一句话版）

刷新拆成状态机阶段（harvest → hlscan → lighterpoll → finalize → candles），cron 每 10 分钟推进
一个阶段、每阶段 ≤40 个外部子请求；快照每小时至少发布一版。Lighter 只收清算价通过合理性
过滤的仓位（全仓账户的 0/哨兵值剔除），属样本覆盖而非全量，页面页脚有标注。
