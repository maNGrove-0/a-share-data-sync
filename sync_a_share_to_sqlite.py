#!/usr/bin/env python3
"""
用 Tushare 将 A 股日线增量同步到本地 SQLite。

优先级：--ts-token > 环境变量 TS_TOKEN > 脚本固定文件（默认 data/ts_token.txt）
默认策略：每天纯增量；到每周指定日自动触发一次全量回补。
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import hashlib
import json
import os
import random
import sqlite3
import sys
import time
import warnings
from pathlib import Path
from typing import Callable

pd = None
ts = None

DB_SCHEMA = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS symbols (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_quotes (
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL,
    amplitude REAL,
    pct_chg REAL,
    chg REAL,
    turnover REAL,
    source TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_quotes_trade_date ON daily_quotes (trade_date);
"""

# 固定配置（统一在这里管理，不通过 CLI 暴露）。
# 1) 路径配置：token 文件位置固定为项目内 data/ts_token.txt。
PROJECT_ROOT = Path(__file__).resolve().parent
TOKEN_FILE_PATH = PROJECT_ROOT / "data/ts_token.txt"

# 2) 请求配置：控制请求间隔、重试次数、退避基数。
REQUEST_SLEEP_SECONDS = 0.8
REQUEST_MAX_RETRIES = 4
REQUEST_RETRY_BACKOFF_SECONDS = 1.0

# 3) 同步策略配置：
#    - 日常按增量执行（回补天数通常设为 0）。
#    - 到每周指定日时，在 CLI 中询问是否执行近似全量回补，用于修正复权历史。
#    - 复权模式固定在脚本内配置，默认 qfq。
ADJUST_MODE = "qfq"
DAILY_ADJUST_BACKFILL_DAYS = 0
WEEKLY_FULL_WEEKDAY = 6
WEEKLY_FULL_BACKFILL_DAYS = 99999


def parse_date(value: str) -> dt.date:
    """解析日期字符串，支持 `YYYYMMDD` 和 `YYYY-MM-DD`。"""
    value = value.strip()
    if "-" in value:
        return dt.date.fromisoformat(value)
    return dt.datetime.strptime(value, "%Y%m%d").date()


def now_ts() -> str:
    """生成当前本地时间戳（秒级）。"""
    return dt.datetime.now().replace(microsecond=0).isoformat(sep=" ")


def read_secret_first_line(file_path: Path) -> str:
    """读取第一行非空、非注释内容。"""
    if not file_path.exists():
        return ""
    text = file_path.read_text(encoding="utf-8")
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        return line
    return ""


def require_dependencies() -> None:
    """加载 pandas/tushare 依赖并屏蔽不影响结果的已知警告。"""
    global pd, ts

    try:
        import pandas as _pd  # type: ignore

        pd = _pd
    except Exception as exc:
        print(
            "Missing dependency: pandas\nInstall with: pip install pandas",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    try:
        import tushare as _ts  # type: ignore

        ts = _ts
    except Exception as exc:
        print(
            "Missing dependency: tushare\nInstall with: pip install tushare",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    # tushare 内部对 pandas 的旧式调用会触发 FutureWarning，不影响结果。
    warnings.filterwarnings(
        "ignore",
        message=r"Series\.fillna with 'method' is deprecated.*",
        category=FutureWarning,
    )


def create_connection(db_path: Path) -> sqlite3.Connection:
    """创建 SQLite 连接并初始化库表结构。"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.executescript(DB_SCHEMA)
    conn.commit()
    return conn


def normalize_symbol(raw: str) -> str:
    """将输入清洗为纯数字股票代码。"""
    return "".join(ch for ch in raw.strip() if ch.isdigit())


def to_ts_symbol(symbol: str) -> str:
    """将本地股票代码转换为 Tushare 的 `ts_code` 格式。"""
    s = symbol.strip().upper()
    if s.endswith(".SH") or s.endswith(".SZ") or s.endswith(".BJ"):
        return s
    code = normalize_symbol(s)
    if len(code) != 6:
        raise ValueError(f"invalid stock code for Tushare: {symbol}")
    # 北交所 92xxxx 需要优先判定为 BJ，避免被 9 前缀误判为 SH。
    if code.startswith("92"):
        return f"{code}.BJ"
    if code.startswith(("5", "6", "9")):
        return f"{code}.SH"
    if code.startswith(("4", "8")):
        return f"{code}.BJ"
    return f"{code}.SZ"


def from_ts_symbol(symbol: str) -> str:
    """将 Tushare 的 `ts_code` 转回本地 6 位代码。"""
    s = symbol.strip().upper()
    if "." in s:
        s = s.split(".", 1)[0]
    return normalize_symbol(s)


def parse_symbols_args(args: argparse.Namespace) -> list[str]:
    """解析命令行股票参数，输出去重后的 6 位代码列表。"""
    symbols: list[str] = []
    if args.symbols:
        symbols.extend(args.symbols.split(","))
    if args.symbols_file:
        p = Path(args.symbols_file)
        if not p.exists():
            raise FileNotFoundError(f"symbols file not found: {p}")
        text = p.read_text(encoding="utf-8")
        symbols.extend(text.replace("\n", ",").split(","))

    out = []
    for s in symbols:
        code = normalize_symbol(s)
        if len(code) == 6:
            out.append(code)
    return sorted(set(out))


def ts_auth(token: str):
    """使用 token 登录 Tushare 并返回 pro 客户端。"""
    try:
        ts.set_token(token)
        return ts.pro_api(token)
    except Exception as exc:
        raise RuntimeError(f"Tushare auth failed: {exc}") from exc


def ts_pro_bar_compat(
    pro,
    ts_code: str,
    adj: str | None,
    start_date: str,
    end_date: str,
):
    """兼容不同 Tushare 版本的 pro_bar 参数签名。"""
    kwargs = {
        "ts_code": ts_code,
        "adj": adj,
        "start_date": start_date,
        "end_date": end_date,
        "freq": "D",
    }
    try:
        return ts.pro_bar(pro_api=pro, **kwargs)
    except TypeError as exc:
        if "unexpected keyword argument 'pro_api'" in str(exc):
            return ts.pro_bar(**kwargs)
        raise


def get_symbol_pool_from_ts(pro) -> list[tuple[str, str]]:
    """拉取在市 A 股股票池（list_status=L）。"""
    df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name")
    if df is None or df.empty:
        raise RuntimeError("Tushare stock_basic returned empty data")

    out: list[tuple[str, str]] = []
    for _, row in df.iterrows():
        code = from_ts_symbol(str(row.get("ts_code", "")))
        if len(code) != 6:
            continue
        name = str(row.get("name", "")).strip()
        out.append((code, name))

    dedup = {s: n for s, n in out}
    return sorted(dedup.items(), key=lambda x: x[0])


def get_symbol_pool_from_local_cache(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """从本地缓存读取股票池，优先 symbols 表，回退 daily_quotes。"""
    rows = conn.execute(
        "SELECT symbol, COALESCE(name, '') AS name FROM symbols ORDER BY symbol"
    ).fetchall()
    if rows:
        return [(str(r[0]), str(r[1])) for r in rows if len(str(r[0])) == 6]

    rows = conn.execute("SELECT DISTINCT symbol FROM daily_quotes ORDER BY symbol").fetchall()
    return [(str(r[0]), "") for r in rows if len(str(r[0])) == 6]


def upsert_symbols(conn: sqlite3.Connection, symbols: list[tuple[str, str]]) -> None:
    """将股票池 UPSERT 到 symbols 表。"""
    if not symbols:
        return
    ts_now = now_ts()
    conn.executemany(
        """
        INSERT INTO symbols (symbol, name, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            name = excluded.name,
            updated_at = excluded.updated_at
        """,
        [(s, n, ts_now) for s, n in symbols],
    )
    conn.commit()


def get_last_trade_date(conn: sqlite3.Connection, symbol: str) -> dt.date | None:
    """读取指定股票在本地库中的最新交易日。"""
    row = conn.execute(
        "SELECT MAX(trade_date) FROM daily_quotes WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    if not row or not row[0]:
        return None
    return dt.date.fromisoformat(row[0])


def compute_fetch_start_date(
    global_start: dt.date,
    last_trade_date: dt.date | None,
    adjust: str,
    adjust_backfill_days: int,
) -> dt.date:
    """计算本次拉取起点；复权模式下回补一段历史以覆盖复权变更。"""
    if last_trade_date is None:
        return global_start

    incremental_start = last_trade_date + dt.timedelta(days=1)
    if adjust and adjust_backfill_days > 0:
        backfill_start = last_trade_date - dt.timedelta(days=adjust_backfill_days)
        return max(global_start, min(incremental_start, backfill_start))
    return max(global_start, incremental_start)


def fetch_symbol_daily_ts(
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    adjust: str,
    max_retries: int,
    retry_backoff: float,
    pro,
):
    """拉取单只股票日线；失败时按指数退避重试。"""
    err: Exception | None = None
    ts_code = to_ts_symbol(symbol)
    adj = adjust if adjust else None

    for attempt in range(1, max_retries + 1):
        try:
            df = ts_pro_bar_compat(
                pro=pro,
                ts_code=ts_code,
                adj=adj,
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
            )
            return df if df is not None else pd.DataFrame()
        except Exception as exc:
            err = exc
            # 参数签名错误属于不可重试问题。
            if isinstance(exc, TypeError):
                break
            if attempt == max_retries:
                break
            wait_s = retry_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.2)
            print(f"{symbol}: 第 {attempt} 次失败，{wait_s:.1f}s 后重试", file=sys.stderr)
            time.sleep(wait_s)

    raise RuntimeError(f"Fetch failed for {symbol}: {err}") from err


def normalize_daily_df_ts(df, symbol: str):
    """把 Tushare 原始日线数据标准化为 `daily_quotes` 表字段。"""
    if df is None or df.empty:
        return pd.DataFrame()

    pre_close = pd.to_numeric(df.get("pre_close"), errors="coerce")
    out = pd.DataFrame(index=df.index)
    out["symbol"] = symbol
    out["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.strftime(
        "%Y-%m-%d"
    )
    out["open"] = pd.to_numeric(df.get("open"), errors="coerce")
    out["high"] = pd.to_numeric(df.get("high"), errors="coerce")
    out["low"] = pd.to_numeric(df.get("low"), errors="coerce")
    out["close"] = pd.to_numeric(df.get("close"), errors="coerce")
    out["volume"] = pd.to_numeric(df.get("vol"), errors="coerce")
    out["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
    out["amplitude"] = ((out["high"] - out["low"]) / pre_close) * 100
    out["pct_chg"] = pd.to_numeric(df.get("pct_chg"), errors="coerce")
    out["chg"] = pd.to_numeric(df.get("change"), errors="coerce")
    out["turnover"] = None
    out["source"] = "tushare"
    out["updated_at"] = now_ts()

    out = out.dropna(subset=["trade_date"])
    out = out.drop_duplicates(subset=["trade_date"], keep="last")
    out = out.sort_values("trade_date")
    return out


def upsert_daily_quotes(conn: sqlite3.Connection, data) -> int:
    """将标准化后的日线数据 UPSERT 到数据库，返回写入行数。"""
    if data.empty:
        return 0
    tuples = list(data.itertuples(index=False, name=None))
    conn.executemany(
        """
        INSERT INTO daily_quotes (
            symbol, trade_date, open, high, low, close,
            volume, amount, amplitude, pct_chg, chg, turnover, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, trade_date) DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            amount = excluded.amount,
            amplitude = excluded.amplitude,
            pct_chg = excluded.pct_chg,
            chg = excluded.chg,
            turnover = excluded.turnover,
            source = excluded.source,
            updated_at = excluded.updated_at
        """,
        tuples,
    )
    conn.commit()
    return len(tuples)


def resolve_ts_token(cli_token: str) -> tuple[str, Path]:
    """按优先级解析 token：CLI > 环境变量 > 固定文件。"""
    token_file = TOKEN_FILE_PATH.expanduser().resolve()
    cli_token = (cli_token or "").strip()
    if cli_token:
        return cli_token, token_file

    env_token = (os.getenv("TS_TOKEN", "") or "").strip()
    if env_token:
        return env_token, token_file

    try:
        file_token = read_secret_first_line(token_file).strip()
    except Exception as exc:
        raise RuntimeError(f"读取 token 文件失败: {token_file}: {exc}") from exc

    if file_token:
        print(f"Tushare token loaded from file: {token_file}")
        return file_token, token_file

    raise RuntimeError(
        "需要 Tushare token。可通过 --ts-token、TS_TOKEN 或 token 文件提供。"
    )


def validate_sync_policy() -> None:
    """校验脚本内常量配置，避免误配置导致异常行为。"""
    if REQUEST_SLEEP_SECONDS < 0:
        raise ValueError("REQUEST_SLEEP_SECONDS must be >= 0")
    if REQUEST_MAX_RETRIES < 1:
        raise ValueError("REQUEST_MAX_RETRIES must be >= 1")
    if REQUEST_RETRY_BACKOFF_SECONDS < 0:
        raise ValueError("REQUEST_RETRY_BACKOFF_SECONDS must be >= 0")
    if ADJUST_MODE not in {"", "qfq", "hfq"}:
        raise ValueError("ADJUST_MODE must be one of '', 'qfq', 'hfq'")
    if DAILY_ADJUST_BACKFILL_DAYS < 0:
        raise ValueError("DAILY_ADJUST_BACKFILL_DAYS must be >= 0")
    if WEEKLY_FULL_BACKFILL_DAYS < 0:
        raise ValueError("WEEKLY_FULL_BACKFILL_DAYS must be >= 0")
    if not (0 <= WEEKLY_FULL_WEEKDAY <= 6):
        raise ValueError("WEEKLY_FULL_WEEKDAY must be in [0, 6]")


def ask_weekly_full_refresh(today: dt.date) -> bool:
    """在每周触发日询问是否执行全量回补；默认不执行。"""
    if today.weekday() != WEEKLY_FULL_WEEKDAY:
        return False

    prompt = (
        f"今天是每周全量触发日（weekday={today.weekday()}）。"
        "是否执行本次全量回补？[y/N]: "
    )

    if not sys.stdin.isatty():
        print("检测到非交互终端，默认不执行每周全量回补。")
        return False

    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        print("未读取到输入，默认不执行每周全量回补。")
        return False

    return answer in {"y", "yes", "1", "true", "是"}


def decide_weekly_full_refresh(today: dt.date, mode: str) -> bool:
    """根据运行模式决定周触发日是否执行全量回补。"""
    if today.weekday() != WEEKLY_FULL_WEEKDAY:
        return False
    mode = (mode or "auto").strip().lower()
    if mode == "yes":
        return True
    if mode == "no":
        return False
    return ask_weekly_full_refresh(today)


def has_potential_trading_day(start_date: dt.date, end_date: dt.date) -> bool:
    """轻量交易日判断：区间内存在工作日才认为有潜在交易日。"""
    d = start_date
    while d <= end_date:
        if d.weekday() < 5:
            return True
        d += dt.timedelta(days=1)
    return False


def get_open_trade_day_ordinals_from_ts(
    pro,
    start_date: dt.date,
    end_date: dt.date,
) -> list[int]:
    """从 Tushare trade_cal 拉取开市日并转为有序 ordinal 列表。"""
    params = {
        "exchange": "",
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date": end_date.strftime("%Y%m%d"),
        "fields": "cal_date,is_open",
    }
    try:
        df = pro.trade_cal(**params)
    except TypeError as exc:
        # 兼容不支持 fields 参数的旧接口。
        if "unexpected keyword argument 'fields'" not in str(exc):
            raise
        params.pop("fields", None)
        df = pro.trade_cal(**params)

    if df is None or df.empty:
        return []

    open_days: set[int] = set()
    for _, row in df.iterrows():
        is_open_raw = row.get("is_open", 0)
        try:
            is_open = int(is_open_raw)
        except Exception:
            try:
                is_open = int(float(str(is_open_raw).strip() or "0"))
            except Exception:
                is_open = 0
        if is_open != 1:
            continue

        cal_date_raw = str(row.get("cal_date", "")).strip()
        if not cal_date_raw:
            continue
        try:
            trade_day = parse_date(cal_date_raw)
        except Exception:
            continue
        open_days.add(trade_day.toordinal())

    return sorted(open_days)


def build_trading_day_checker(
    pro,
    start_date: dt.date,
    end_date: dt.date,
) -> tuple[Callable[[dt.date, dt.date], bool], str]:
    """构建“区间是否存在潜在交易日”检查器。优先 trade_cal，失败回退工作日规则。"""
    try:
        open_day_ordinals = get_open_trade_day_ordinals_from_ts(pro, start_date, end_date)
        print(
            "Trading-day pre-check source: tushare trade_cal "
            f"({len(open_day_ordinals)} open days in range)"
        )

        def has_open_day_via_calendar(range_start: dt.date, range_end: dt.date) -> bool:
            if range_start > range_end:
                return False
            left = bisect.bisect_left(open_day_ordinals, range_start.toordinal())
            return left < len(open_day_ordinals) and open_day_ordinals[left] <= range_end.toordinal()

        return has_open_day_via_calendar, "trade_cal"
    except Exception as exc:
        print(
            f"Trading-day pre-check: tushare trade_cal unavailable, fallback to weekday rule. error: {exc}",
            file=sys.stderr,
        )
    return has_potential_trading_day, "weekday"


def write_failed_symbols(file_path: Path, symbols: list[str]) -> None:
    """将失败股票代码落盘（每行一个 6 位代码）。"""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    unique_codes = sorted({normalize_symbol(s) for s in symbols if len(normalize_symbol(s)) == 6})
    file_path.write_text("\n".join(unique_codes) + ("\n" if unique_codes else ""), encoding="utf-8")


def build_symbol_identity(symbol_pairs: list[tuple[str, str]]) -> dict[str, str | int]:
    """生成股票池身份摘要，用于校验 checkpoint 是否可安全续跑。"""
    symbols = [s for s, _ in symbol_pairs]
    joined = "\n".join(symbols)
    return {
        "symbol_hash": hashlib.sha256(joined.encode("utf-8")).hexdigest(),
        "symbol_count": len(symbols),
        "symbol_first": symbols[0] if symbols else "",
        "symbol_last": symbols[-1] if symbols else "",
    }


def parse_next_index_safely(value: object) -> int | None:
    """安全解析 checkpoint 里的 next_index。"""
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 1 else None


def load_checkpoint(file_path: Path) -> dict | None:
    """读取 checkpoint 文件。不存在或损坏时返回 None。"""
    if not file_path.exists():
        return None
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_checkpoint(
    file_path: Path,
    *,
    db_path: Path,
    start_date: dt.date,
    end_date: dt.date,
    next_index: int,
    total_symbols: int,
    last_symbol: str,
    symbol_identity: dict[str, str | int],
) -> None:
    """保存 checkpoint，记录下次应从第几个 symbol 开始。"""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "updated_at": now_ts(),
        "db": str(db_path),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "next_index": next_index,
        "total_symbols": total_symbols,
        "last_symbol": last_symbol,
        **symbol_identity,
    }
    file_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def clear_checkpoint(file_path: Path) -> None:
    """清理 checkpoint 文件（任务跑完整后调用）。"""
    try:
        if file_path.exists():
            file_path.unlink()
    except Exception:
        pass


def checkpoint_symbol_identity_matches(
    checkpoint: dict,
    current_identity: dict[str, str | int],
) -> bool:
    """校验 checkpoint 的股票池身份是否与当前运行一致。"""
    ck_hash = str(checkpoint.get("symbol_hash", "")).strip()
    ck_count = checkpoint.get("symbol_count")
    ck_first = str(checkpoint.get("symbol_first", "")).strip()
    ck_last = str(checkpoint.get("symbol_last", "")).strip()

    has_identity = any([ck_hash, ck_count is not None, ck_first, ck_last])
    if not has_identity:
        # 兼容历史 checkpoint（无 symbol identity 字段）。
        return True

    try:
        ck_count_int = int(ck_count)
    except (TypeError, ValueError):
        return False

    return (
        ck_hash == current_identity["symbol_hash"]
        and ck_count_int == current_identity["symbol_count"]
        and ck_first == current_identity["symbol_first"]
        and ck_last == current_identity["symbol_last"]
    )


def is_tushare_daily_quota_error(exc: Exception) -> bool:
    """判断是否命中 Tushare 日调用限额。"""
    msg = str(exc)
    return "最多访问该接口10000次" in msg or "每天最多访问该接口10000次" in msg


def build_cli() -> argparse.ArgumentParser:
    """构建命令行参数解析器。"""
    parser = argparse.ArgumentParser(description="Sync A-share daily data from Tushare.")
    parser.add_argument("--db", default="data/a_share.db", help="SQLite DB path.")
    parser.add_argument("--start-date", default="20100101", help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument(
        "--end-date",
        default=dt.date.today().strftime("%Y%m%d"),
        help="YYYYMMDD or YYYY-MM-DD",
    )
    parser.add_argument("--symbols", default="", help="e.g. 000001,600519")
    parser.add_argument("--symbols-file", default="", help="comma/newline separated")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--skip-symbol-refresh",
        action="store_true",
        help="仅使用本地 symbols/daily_quotes 缓存，不请求 Tushare 股票池",
    )
    parser.add_argument(
        "--weekly-full",
        choices=["auto", "yes", "no"],
        default="auto",
        help="每周全量模式：auto=触发日交互确认，yes=触发日自动执行，no=永不执行",
    )
    parser.add_argument(
        "--save-failed-file",
        default="",
        help="将本次失败股票代码写入文件（去重后每行一个 6 位代码）",
    )
    parser.add_argument(
        "--retry-failed-file",
        default="",
        help="从失败列表文件读取股票代码并仅同步这些代码（优先级最高）",
    )
    parser.add_argument(
        "--checkpoint-file",
        default="data/sync_checkpoint.json",
        help="checkpoint 文件路径；用于断点续跑",
    )
    parser.add_argument(
        "--resume-from-checkpoint",
        action="store_true",
        help="从 checkpoint 指定进度继续执行",
    )
    parser.add_argument("--ts-token", default="", help="Priority: cli > env > fixed file")
    return parser


def main() -> int:
    """执行主流程：初始化 -> 获取股票池 -> 增量同步 -> 输出结果。"""
    # 步骤 1：解析参数并加载依赖。
    args = build_cli().parse_args()
    require_dependencies()

    # 步骤 2：解析时间区间并校验固定配置。
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if start_date > end_date:
        raise ValueError("start-date must be <= end-date")
    validate_sync_policy()

    # 步骤 3：计算运行模式（每日增量 / 每周触发日询问全量）。
    today = dt.date.today()
    # 仅复权模式(qfq/hfq)才需要回补；不复权模式下跳过每周全量询问。
    is_weekly_full_day = bool(ADJUST_MODE) and decide_weekly_full_refresh(today, args.weekly_full)
    effective_backfill_days = (
        WEEKLY_FULL_BACKFILL_DAYS if is_weekly_full_day else DAILY_ADJUST_BACKFILL_DAYS
    )

    # 步骤 4：初始化数据库连接与表结构。
    db_path = Path(args.db).expanduser().resolve()
    conn = create_connection(db_path)

    # 步骤 5：解析 token 并完成 Tushare 鉴权。
    token, _ = resolve_ts_token(args.ts_token)
    pro = ts_auth(token)
    print("Tushare auth success.")
    has_trading_day, trading_day_check_source = build_trading_day_checker(
        pro=pro,
        start_date=start_date,
        end_date=end_date,
    )

    # 步骤 6：确定股票池（失败列表重试 / 手动指定 / 远端刷新 / 本地缓存回退）。
    failed_retry_symbols = []
    if args.retry_failed_file:
        retry_path = Path(args.retry_failed_file).expanduser().resolve()
        if not retry_path.exists():
            raise FileNotFoundError(f"retry-failed file not found: {retry_path}")
        text = retry_path.read_text(encoding="utf-8")
        for s in text.replace("\n", ",").split(","):
            code = normalize_symbol(s)
            if len(code) == 6:
                failed_retry_symbols.append(code)
        failed_retry_symbols = sorted(set(failed_retry_symbols))

    if args.retry_failed_file:
        symbol_pairs = [(s, "") for s in failed_retry_symbols]
        if symbol_pairs:
            print(f"Using retry-failed symbol list: {len(symbol_pairs)}")
        else:
            print(
                f"Retry-failed file provided but yielded 0 valid symbols: {Path(args.retry_failed_file).expanduser().resolve()}"
            )
    else:
        manual_symbols = parse_symbols_args(args)
        if manual_symbols:
            symbol_pairs = [(s, "") for s in manual_symbols]
        else:
            local_pairs = get_symbol_pool_from_local_cache(conn)
            if args.skip_symbol_refresh:
                if not local_pairs:
                    raise RuntimeError(
                        "指定了 --skip-symbol-refresh，但本地无股票池缓存。"
                    )
                symbol_pairs = local_pairs
                print(f"Using local symbol cache: {len(symbol_pairs)}")
            else:
                try:
                    symbol_pairs = get_symbol_pool_from_ts(pro)
                    upsert_symbols(conn, symbol_pairs)
                    print(f"Symbol pool refreshed from Tushare: {len(symbol_pairs)}")
                except Exception as exc:
                    if not local_pairs:
                        raise RuntimeError(
                            f"无法从 Tushare 获取股票池，且本地无缓存。错误: {exc}"
                        ) from exc
                    symbol_pairs = local_pairs
                    print(
                        f"Tushare 股票池获取失败，已使用本地缓存 {len(symbol_pairs)}。错误: {exc}",
                        file=sys.stderr,
                    )

    # 步骤 7：应用数量限制并输出运行摘要。
    if args.limit and args.limit > 0:
        symbol_pairs = symbol_pairs[: args.limit]

    total_symbols = len(symbol_pairs)
    symbol_identity = build_symbol_identity(symbol_pairs)
    print(f"DB: {db_path}")
    print("Source: tushare")
    print(f"Symbols to sync: {total_symbols}")
    print(f"Date range: {start_date} -> {end_date}")
    print(f"Adjust: {ADJUST_MODE or '(none)'}")
    print(f"Trading-day pre-check: {trading_day_check_source}")
    if ADJUST_MODE:
        if is_weekly_full_day:
            print(
                "Mode: weekly full refresh "
                f"(weekday={today.weekday()}, backfill_days={effective_backfill_days})"
            )
        else:
            print(
                "Mode: daily incremental "
                f"(weekday={today.weekday()}, backfill_days={effective_backfill_days})"
            )

    inserted_total = 0
    skipped = 0
    empty_return = 0
    failed = 0
    failed_symbols: list[str] = []
    quota_hit = False

    checkpoint_path = Path(args.checkpoint_file).expanduser().resolve()
    start_index = 1
    if args.resume_from_checkpoint:
        checkpoint = load_checkpoint(checkpoint_path)
        if checkpoint:
            ck_db = str(checkpoint.get("db", "")).strip()
            ck_start = str(checkpoint.get("start_date", "")).strip()
            ck_end = str(checkpoint.get("end_date", "")).strip()
            ck_next = parse_next_index_safely(checkpoint.get("next_index"))
            context_match = (
                ck_db == str(db_path)
                and ck_start == start_date.isoformat()
                and ck_end == end_date.isoformat()
            )
            identity_match = checkpoint_symbol_identity_matches(checkpoint, symbol_identity)
            if context_match and identity_match:
                if ck_next is None:
                    print(
                        "Checkpoint exists but next_index is invalid; ignore resume. "
                        f"file={checkpoint_path}",
                        file=sys.stderr,
                    )
                elif ck_next > total_symbols:
                    clear_checkpoint(checkpoint_path)
                    print(
                        "Checkpoint next_index exceeds current symbol count; "
                        f"cleared stale checkpoint and restart from beginning. file={checkpoint_path}",
                        file=sys.stderr,
                    )
                else:
                    start_index = max(1, ck_next)
                    if start_index > 1:
                        print(
                            f"Resume from checkpoint: next_index={start_index}/{total_symbols} "
                            f"(file={checkpoint_path})"
                        )
            else:
                print(
                    "Checkpoint exists but does not match current run context; ignore resume. "
                    f"file={checkpoint_path}",
                    file=sys.stderr,
                )

    # 步骤 8：逐只股票执行“起始日计算 -> 拉取 -> 标准化 -> 入库”。
    for idx in range(start_index, total_symbols + 1):
        symbol, _ = symbol_pairs[idx - 1]

        # 8.1 根据本地最后交易日与回补策略，计算本次拉取起点。
        last_dt = get_last_trade_date(conn, symbol)
        local_start = compute_fetch_start_date(
            global_start=start_date,
            last_trade_date=last_dt,
            adjust=ADJUST_MODE,
            adjust_backfill_days=effective_backfill_days,
        )

        # 8.2 若本地已覆盖到目标区间，直接跳过该股票。
        if local_start > end_date:
            skipped += 1
            print(f"[{idx}/{total_symbols}] {symbol}: up-to-date, skip")
            save_checkpoint(
                checkpoint_path,
                db_path=db_path,
                start_date=start_date,
                end_date=end_date,
                next_index=idx + 1,
                total_symbols=total_symbols,
                last_symbol=symbol,
                symbol_identity=symbol_identity,
            )
            continue

        if not has_trading_day(local_start, end_date):
            skipped += 1
            print(f"[{idx}/{total_symbols}] {symbol}: no potential trading day in range, skip")
            save_checkpoint(
                checkpoint_path,
                db_path=db_path,
                start_date=start_date,
                end_date=end_date,
                next_index=idx + 1,
                total_symbols=total_symbols,
                last_symbol=symbol,
                symbol_identity=symbol_identity,
            )
            continue

        try:
            # 8.3 拉取并标准化数据，再 UPSERT 入库。
            raw_df = fetch_symbol_daily_ts(
                symbol=symbol,
                start_date=local_start,
                end_date=end_date,
                adjust=ADJUST_MODE,
                max_retries=REQUEST_MAX_RETRIES,
                retry_backoff=REQUEST_RETRY_BACKOFF_SECONDS,
                pro=pro,
            )
            norm_df = normalize_daily_df_ts(raw_df, symbol=symbol)
            rows = upsert_daily_quotes(conn, norm_df)
            inserted_total += rows
            if rows == 0:
                empty_return += 1
            print(f"[{idx}/{total_symbols}] {symbol}: {local_start} -> {end_date}, rows={rows}")
            save_checkpoint(
                checkpoint_path,
                db_path=db_path,
                start_date=start_date,
                end_date=end_date,
                next_index=idx + 1,
                total_symbols=total_symbols,
                last_symbol=symbol,
                symbol_identity=symbol_identity,
            )
        except Exception as exc:
            failed += 1
            failed_symbols.append(symbol)
            print(f"[{idx}/{total_symbols}] {symbol}: FAILED: {exc}", file=sys.stderr)

            if is_tushare_daily_quota_error(exc):
                quota_hit = True
                # 配额命中时，保留当前 symbol 作为下次起点。
                save_checkpoint(
                    checkpoint_path,
                    db_path=db_path,
                    start_date=start_date,
                    end_date=end_date,
                    next_index=idx,
                    total_symbols=total_symbols,
                    last_symbol=symbol,
                    symbol_identity=symbol_identity,
                )
                # 将剩余未执行 symbol 合并进失败列表，便于次日快速重跑。
                remaining_symbols = [s for s, _ in symbol_pairs[idx - 1 :]]
                failed_symbols.extend(remaining_symbols)
                print(
                    f"Tushare daily quota hit, stop early at {idx}/{total_symbols}. "
                    "Use --resume-from-checkpoint or --retry-failed-file next run.",
                    file=sys.stderr,
                )
                break

            save_checkpoint(
                checkpoint_path,
                db_path=db_path,
                start_date=start_date,
                end_date=end_date,
                next_index=idx + 1,
                total_symbols=total_symbols,
                last_symbol=symbol,
                symbol_identity=symbol_identity,
            )

        if REQUEST_SLEEP_SECONDS > 0:
            time.sleep(REQUEST_SLEEP_SECONDS)

    # 步骤 9：输出汇总并返回进程码。
    print(
        "Done. "
        f"inserted_rows={inserted_total}, "
        f"skipped={skipped}, "
        f"empty_return={empty_return}, "
        f"failed={failed}, "
        f"quota_hit={quota_hit}, "
        f"symbols={total_symbols}"
    )

    if args.save_failed_file and failed_symbols:
        out = Path(args.save_failed_file).expanduser().resolve()
        write_failed_symbols(out, failed_symbols)
        saved_count = len(
            {
                normalize_symbol(s)
                for s in failed_symbols
                if len(normalize_symbol(s)) == 6
            }
        )
        print(f"Failed symbols saved: {out} ({saved_count})")

    finished_all = not quota_hit and start_index <= total_symbols
    if finished_all:
        clear_checkpoint(checkpoint_path)

    conn.close()
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
