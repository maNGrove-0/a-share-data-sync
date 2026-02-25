# A-Share Data Sync
使用 Tushare 将 A 股日线数据同步到本地 SQLite，支持长期运行的增量更新。

## 1. 这套数据库格式是不是业内常用
结论：设计思路是常见的，存储引擎偏个人/研究场景常用。

常见部分：
- 一行代表一只股票一个交易日（`symbol + trade_date`）。
- OHLCV 等字段放在同一张日线表。
- 用主键冲突更新（UPSERT）做增量同步。

偏个人研究部分：
- 使用 SQLite 作为本地单机库，部署简单、维护成本低。
- 对于个人量化研究/回测，这个方案是实用且稳定的。
- 机构生产常见会扩展到 Parquet + DuckDB/Spark、ClickHouse、Timescale/kdb+ 等。

## 2. 数据库结构
脚本启动时会自动建表。

### `symbols`
用途：股票基础信息与本地股票池缓存。

字段：
- `symbol`：6 位股票代码，主键。
- `name`：股票名称。
- `updated_at`：该条基础信息最近更新时间。

### `daily_quotes`
用途：日线行情主表。

主键：
- `(symbol, trade_date)`。

字段：
- `symbol`：股票代码（6 位），如 `000001`。
- `trade_date`：交易日期（`YYYY-MM-DD`）。
- `open`：开盘价。
- `high`：最高价。
- `low`：最低价。
- `close`：收盘价。
- `volume`：成交量（来自 Tushare `vol`）。
- `amount`：成交额（来自 Tushare `amount`）。
- `amplitude`：振幅（百分比），计算方式约为 `(high - low) / pre_close * 100`。
- `pct_chg`：涨跌幅（百分比）。
- `chg`：涨跌额。
- `turnover`：换手率（当前脚本未填，默认 `NULL`）。
- `source`：数据来源（当前固定为 `tushare`）。
- `updated_at`：该行最近一次写入/更新时间。

## 3. 当前同步策略（已实现）
默认策略是“每天增量 + 每周自动全量回补”。

### 每天增量
- 默认 `--adjust-backfill-days 0`，即从本地最新交易日之后继续拉。

### 每周自动全量
- 默认每周日触发（`--weekly-full-weekday 6`）。
- 触发后使用 `--weekly-full-backfill-days 99999` 近似全量回补。
- 这样可以覆盖复权因子变化导致的历史价格更新。

### 股票池刷新策略
- 默认每次优先从 Tushare 拉最新股票池。
- 拉取失败才回退到本地缓存（`symbols`/`daily_quotes`）。
- 可显式 `--skip-symbol-refresh` 仅使用本地缓存。

## 4. Token 优先级与安全
Token 读取优先级：
1. `--ts-token`
2. 环境变量 `TS_TOKEN`
3. `--ts-token-file`（默认 `data/ts_token.txt`）

说明：
- 只有前两者都不存在时才读取 token 文件。
- `.gitignore` 已忽略 `data/ts_token.txt`，避免误提交密钥。

## 5. 安装与运行
## 5.1 安装依赖
```bash
pip install tushare pandas
```

## 5.2 准备 token（推荐文件方式）
```bash
mkdir -p data
echo "你的_tushare_token" > data/ts_token.txt
```

## 5.3 运行同步
```bash
python /Users/zhao/Quant/股票数据/sync_a_share_to_sqlite.py \
  --db /Users/zhao/Quant/股票数据/data/a_share.db \
  --start-date 20100101
```

## 5.4 常用命令
仅同步指定股票：
```bash
python /Users/zhao/Quant/股票数据/sync_a_share_to_sqlite.py \
  --symbols 000001,600519
```

仅用本地股票池缓存（不请求 Tushare 股票池）：
```bash
python /Users/zhao/Quant/股票数据/sync_a_share_to_sqlite.py \
  --skip-symbol-refresh
```

关闭每周自动全量：
```bash
python /Users/zhao/Quant/股票数据/sync_a_share_to_sqlite.py \
  --disable-weekly-full
```

## 6. 关键参数
- `--db`：SQLite 文件路径。
- `--start-date/--end-date`：同步范围。
- `--adjust {,qfq,hfq}`：复权模式，默认 `qfq`。
- `--sleep`：每只股票请求间隔秒数。
- `--max-retries`：单只股票失败重试次数。
- `--retry-backoff`：重试退避基数（秒）。
- `--adjust-backfill-days`：日常增量时的回补天数（默认 0）。
- `--weekly-full-weekday`：每周全量触发日（0=周一…6=周日）。
- `--weekly-full-backfill-days`：每周全量回补天数（默认 99999）。
- `--disable-weekly-full`：关闭每周自动全量。
- `--symbols/--symbols-file`：手动指定股票池。
- `--skip-symbol-refresh`：只用本地股票池缓存。
- `--ts-token/--ts-token-file`：Tushare token 配置。

## 7. 日志怎么看
示例：
```text
[57/5484] 000155: 2010-01-01 -> 2026-02-25, rows=3376
```

含义：
- `[57/5484]`：当前第 57 只，总共 5484 只。
- `000155`：股票代码。
- `2010-01-01 -> 2026-02-25`：本次拉取区间。
- `rows=3376`：本次写入/更新的行数。
