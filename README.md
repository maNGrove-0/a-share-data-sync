# A-Share Data Sync
基于 Tushare 的 A 股日线本地化同步工具。项目目标是把常用回测数据稳定落到本地 SQLite，并支持长期可维护的增量更新。

## Overview
本项目提供一套单机可用的数据同步流程：
- 自动拉取 A 股股票池并缓存到本地。
- 将日线行情写入 SQLite，按 `(symbol, trade_date)` 做 UPSERT。
- 默认“日增量 + 周全量回补”，兼顾速度和复权数据准确性。

适合个人研究、策略开发和本地回测场景。

## Features
- Tushare-only 方案，依赖简单，部署成本低。
- 增量同步，重复执行不会产生重复数据。
- 复权模式下支持回补窗口，降低历史复权值过期风险。
- 股票池刷新失败时自动回退本地缓存，提高任务稳健性。
- Token 支持 CLI / 环境变量 / 文件三种来源。

## Project Structure
```text
.
├── sync_a_share_to_sqlite.py   # 主脚本
├── data/
│   ├── a_share.db              # 本地数据库（运行后生成）
│   └── ts_token.txt            # Tushare token（建议本地文件）
└── README.md
```

## Data Model
脚本启动时会自动初始化以下表结构。

### `symbols`
股票基础信息和本地股票池缓存。

| 列名 | 类型 | 说明 |
| --- | --- | --- |
| `symbol` | `TEXT` | 6 位股票代码，主键 |
| `name` | `TEXT` | 股票名称 |
| `updated_at` | `TEXT` | 该记录最近更新时间 |

### `daily_quotes`
A 股日线主表，主键为 `(symbol, trade_date)`。

| 列名 | 类型 | 说明 |
| --- | --- | --- |
| `symbol` | `TEXT` | 股票代码（如 `000001`） |
| `trade_date` | `TEXT` | 交易日期（`YYYY-MM-DD`） |
| `open` | `REAL` | 开盘价 |
| `high` | `REAL` | 最高价 |
| `low` | `REAL` | 最低价 |
| `close` | `REAL` | 收盘价 |
| `volume` | `REAL` | 成交量（Tushare `vol`） |
| `amount` | `REAL` | 成交额（Tushare `amount`） |
| `amplitude` | `REAL` | 振幅（约 `(high-low)/pre_close*100`） |
| `pct_chg` | `REAL` | 涨跌幅（%） |
| `chg` | `REAL` | 涨跌额 |
| `turnover` | `REAL` | 换手率（当前为 `NULL`） |
| `source` | `TEXT` | 数据来源（当前固定 `tushare`） |
| `updated_at` | `TEXT` | 该行最近写入/更新时间 |

## Sync Strategy
默认策略为：

1. Daily incremental  
   日常运行按增量更新，默认日常回补天数为 `0`（纯增量）。

2. Weekly full refresh  
   每周触发一次近似全量回补（默认周日，回补天数 `99999`）。

3. Symbol universe refresh  
   默认先从 Tushare 刷新股票池，失败时回退本地缓存。

说明：
- 以上策略为脚本内固定配置，不通过 CLI 参数暴露。
- 如需调整，请修改 `sync_a_share_to_sqlite.py` 顶部常量：`ENABLE_WEEKLY_FULL`、`DAILY_ADJUST_BACKFILL_DAYS`、`WEEKLY_FULL_WEEKDAY`、`WEEKLY_FULL_BACKFILL_DAYS`。

## Token Resolution
Token 读取优先级：
1. `--ts-token`
2. `TS_TOKEN` 环境变量
3. `--ts-token-file`（默认 `data/ts_token.txt`）

说明：
- 仅在前两者都不存在时读取 token 文件。
- 建议将 token 文件加入 `.gitignore`（当前仓库已配置）。

## Quick Start
### 1) Install
```bash
pip install tushare pandas
```

### 2) Configure token
```bash
mkdir -p data
echo "your_tushare_token" > data/ts_token.txt
```

### 3) Run sync
```bash
python /Users/zhao/Quant/股票数据/sync_a_share_to_sqlite.py \
  --db /Users/zhao/Quant/股票数据/data/a_share.db \
  --start-date 20100101
```

## Common Commands
仅同步指定股票：
```bash
python /Users/zhao/Quant/股票数据/sync_a_share_to_sqlite.py \
  --symbols 000001,600519
```

仅使用本地股票池缓存：
```bash
python /Users/zhao/Quant/股票数据/sync_a_share_to_sqlite.py \
  --skip-symbol-refresh
```

## Key Arguments
- `--db`：SQLite 文件路径。
- `--start-date` / `--end-date`：同步区间。
- `--adjust {,qfq,hfq}`：复权模式，默认 `qfq`。
- `--sleep`：每只股票请求间隔（秒）。
- `--max-retries`：单股票失败重试次数。
- `--retry-backoff`：重试退避基数（秒）。
- `--symbols` / `--symbols-file`：手动指定股票池。
- `--skip-symbol-refresh`：只用本地股票池缓存。
- `--ts-token` / `--ts-token-file`：Tushare token 配置。

## Log Format
示例：
```text
[57/5484] 000155: 2010-01-01 -> 2026-02-25, rows=3376
```

含义：
- `57/5484`：进度（当前/总数）
- `000155`：股票代码
- `2010-01-01 -> 2026-02-25`：本次拉取区间
- `rows=3376`：本次写入/更新行数
