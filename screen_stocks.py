#!/usr/bin/env python3
"""
基于本地 SQLite 的多条件嵌套规则选股脚本。

规则 JSON 支持两类节点：

1) 逻辑节点（可递归嵌套）
{
  "logic": "and" | "or",
  "rules": [ ...子规则... ]
}

2) 条件节点
{
  "field": "pct_chg",
  "op": ">" | ">=" | "<" | "<=" | "=" | "!=" | "in" | "not in" | "between" | "is null" | "is not null",
  "value": ...  # 具体类型取决于 op
}
"""

from __future__ import print_function

import argparse
import datetime as dt
import json
import sqlite3
import sys
from typing import Any, Dict, List, Sequence, Tuple

FIELD_SQL_MAP = {
    "symbol": "q.symbol",
    "trade_date": "q.trade_date",
    "open": "q.open",
    "high": "q.high",
    "low": "q.low",
    "close": "q.close",
    "volume": "q.volume",
    "amount": "q.amount",
    "amplitude": "q.amplitude",
    "pct_chg": "q.pct_chg",
    "chg": "q.chg",
    "turnover": "q.turnover",
    "source": "q.source",
    "name": "s.name",
}

OUTPUT_COLUMNS = [
    "symbol",
    "name",
    "trade_date",
    "close",
    "pct_chg",
    "volume",
    "amount",
]


class RuleValidationError(ValueError):
    """规则解析与校验错误。"""


def parse_date(value: str) -> dt.date:
    """解析 YYYYMMDD 或 YYYY-MM-DD。"""
    text = value.strip()
    if "-" in text:
        return dt.date.fromisoformat(text)
    return dt.datetime.strptime(text, "%Y%m%d").date()


def normalize_operator(raw: Any) -> str:
    """将各种写法统一映射到 SQL 运算符。"""
    if raw is None:
        raise RuleValidationError("missing operator: use 'op' or 'operator'")

    key = str(raw).strip().lower()
    mapping = {
        "=": "=",
        "==": "=",
        "eq": "=",
        "!=": "!=",
        "<>": "!=",
        "ne": "!=",
        ">": ">",
        "gt": ">",
        ">=": ">=",
        "gte": ">=",
        "<": "<",
        "lt": "<",
        "<=": "<=",
        "lte": "<=",
        "in": "IN",
        "not in": "NOT IN",
        "not_in": "NOT IN",
        "between": "BETWEEN",
        "is null": "IS NULL",
        "is_null": "IS NULL",
        "is not null": "IS NOT NULL",
        "is_not_null": "IS NOT NULL",
    }
    if key not in mapping:
        raise RuleValidationError("unsupported operator: {0}".format(raw))
    return mapping[key]


def build_leaf_rule_sql(
    rule: Dict[str, Any], field_sql_map: Dict[str, str]
) -> Tuple[str, List[Any]]:
    """构建叶子条件 SQL。"""
    field = rule.get("field")
    if not isinstance(field, str) or not field.strip():
        raise RuleValidationError("leaf rule requires non-empty string field")

    field_name = field.strip()
    if field_name not in field_sql_map:
        supported = ", ".join(sorted(field_sql_map.keys()))
        raise RuleValidationError(
            "unsupported field: {0}. supported fields: {1}".format(field_name, supported)
        )

    operator = normalize_operator(rule.get("op", rule.get("operator")))
    column = field_sql_map[field_name]

    if operator in ("IS NULL", "IS NOT NULL"):
        return "{0} {1}".format(column, operator), []

    value = rule.get("value")
    if operator in ("IN", "NOT IN"):
        if not isinstance(value, (list, tuple)) or len(value) == 0:
            raise RuleValidationError(
                "operator {0} requires non-empty list value".format(operator)
            )
        placeholders = ", ".join(["?"] * len(value))
        return "{0} {1} ({2})".format(column, operator, placeholders), list(value)

    if operator == "BETWEEN":
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise RuleValidationError("operator BETWEEN requires [min, max]")
        return "{0} BETWEEN ? AND ?".format(column), [value[0], value[1]]

    return "{0} {1} ?".format(column, operator), [value]


def build_rule_sql(
    rule: Dict[str, Any], field_sql_map: Dict[str, str]
) -> Tuple[str, List[Any]]:
    """递归构建规则 SQL，支持 AND/OR 嵌套。"""
    if not isinstance(rule, dict):
        raise RuleValidationError("rule node must be object/dict")

    if "logic" in rule:
        logic_raw = str(rule.get("logic", "")).strip().lower()
        if logic_raw not in ("and", "or"):
            raise RuleValidationError("logic must be 'and' or 'or'")

        children = rule.get("rules")
        if not isinstance(children, list) or len(children) == 0:
            raise RuleValidationError("logic node requires non-empty rules list")

        joiner = " AND " if logic_raw == "and" else " OR "
        sql_parts = []
        params = []
        for child in children:
            child_sql, child_params = build_rule_sql(child, field_sql_map)
            sql_parts.append(child_sql)
            params.extend(child_params)
        return "({0})".format(joiner.join(sql_parts)), params

    return build_leaf_rule_sql(rule, field_sql_map)


def load_rule_file(path: str) -> Dict[str, Any]:
    """加载并校验规则文件。"""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise RuleValidationError("rule file root must be JSON object")

    if "rule" in data:
        nested = data.get("rule")
        if not isinstance(nested, dict):
            raise RuleValidationError("'rule' key must be a JSON object")
        return nested
    return data


def connect_db(db_path: str) -> sqlite3.Connection:
    """创建 SQLite 连接。"""
    return sqlite3.connect(db_path)


def resolve_trade_date(conn: sqlite3.Connection, trade_date_arg: str) -> str:
    """解析目标交易日；未指定时使用库中最大交易日。"""
    if trade_date_arg.strip():
        return parse_date(trade_date_arg).isoformat()

    row = conn.execute("SELECT MAX(trade_date) FROM daily_quotes").fetchone()
    if not row or not row[0]:
        raise RuntimeError("daily_quotes is empty; cannot infer latest trade date")
    return str(row[0])


def run_screener(
    conn: sqlite3.Connection,
    trade_date: str,
    rule_sql: str,
    rule_params: Sequence[Any],
    sort_by: str,
    sort_order: str,
    limit: int,
) -> List[Tuple[Any, ...]]:
    """执行选股查询。"""
    if sort_by not in FIELD_SQL_MAP:
        raise RuleValidationError(
            "unsupported sort field: {0}. supported: {1}".format(
                sort_by, ", ".join(sorted(FIELD_SQL_MAP.keys()))
            )
        )

    order = sort_order.strip().lower()
    if order not in ("asc", "desc"):
        raise RuleValidationError("sort-order must be asc or desc")

    select_sql = """
        SELECT
            q.symbol,
            COALESCE(s.name, '') AS name,
            q.trade_date,
            q.close,
            q.pct_chg,
            q.volume,
            q.amount
        FROM daily_quotes q
        LEFT JOIN symbols s ON s.symbol = q.symbol
        WHERE q.trade_date = ?
          AND {rule_sql}
        ORDER BY {sort_expr} {order}, q.symbol ASC
        LIMIT ?
    """.format(
        rule_sql=rule_sql,
        sort_expr=FIELD_SQL_MAP[sort_by],
        order=order.upper(),
    )

    params = [trade_date]
    params.extend(rule_params)
    params.append(limit)
    cur = conn.execute(select_sql, params)
    return cur.fetchall()


def rows_to_dicts(rows: Sequence[Tuple[Any, ...]]) -> List[Dict[str, Any]]:
    """将数据库行转换为字典列表。"""
    out = []
    for row in rows:
        item = {}
        for idx, col in enumerate(OUTPUT_COLUMNS):
            item[col] = row[idx]
        out.append(item)
    return out


def format_table(rows: Sequence[Tuple[Any, ...]]) -> str:
    """将结果渲染为简单文本表格。"""
    text_rows = []
    for row in rows:
        text_rows.append([("" if v is None else str(v)) for v in row])

    widths = [len(col) for col in OUTPUT_COLUMNS]
    for row in text_rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value))

    def join_row(values: Sequence[str]) -> str:
        parts = []
        for i, value in enumerate(values):
            parts.append(value.ljust(widths[i]))
        return " | ".join(parts)

    lines = []
    lines.append(join_row(OUTPUT_COLUMNS))
    lines.append("-+-".join(["-" * w for w in widths]))
    for row in text_rows:
        lines.append(join_row(row))
    return "\n".join(lines)


def build_cli() -> argparse.ArgumentParser:
    """构建命令行参数。"""
    parser = argparse.ArgumentParser(
        description="Screen stocks from SQLite with nested AND/OR JSON rules."
    )
    parser.add_argument("--db", default="data/a_share.db", help="SQLite DB path")
    parser.add_argument("--rules-file", required=True, help="JSON file path for screener rule")
    parser.add_argument(
        "--trade-date",
        default="",
        help="Trade date (YYYYMMDD or YYYY-MM-DD). default: latest in DB",
    )
    parser.add_argument(
        "--sort-by",
        default="pct_chg",
        help="Sort field, default pct_chg",
    )
    parser.add_argument(
        "--sort-order",
        choices=["asc", "desc"],
        default="desc",
        help="Sort order",
    )
    parser.add_argument("--limit", type=int, default=50, help="Max rows to return")
    parser.add_argument(
        "--output",
        choices=["table", "json"],
        default="table",
        help="Output format",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Write output to file instead of stdout",
    )
    parser.add_argument(
        "--show-sql",
        action="store_true",
        help="Print compiled SQL condition and parameters",
    )
    return parser


def main() -> int:
    """程序入口。"""
    args = build_cli().parse_args()
    if args.limit <= 0:
        print("limit must be > 0", file=sys.stderr)
        return 2

    try:
        rule = load_rule_file(args.rules_file)
        rule_sql, rule_params = build_rule_sql(rule, FIELD_SQL_MAP)
    except (OSError, json.JSONDecodeError, RuleValidationError) as exc:
        print("rule file error: {0}".format(exc), file=sys.stderr)
        return 2

    if args.show_sql:
        print("Compiled rule SQL: {0}".format(rule_sql))
        print("Rule params: {0}".format(rule_params))

    try:
        conn = connect_db(args.db)
        trade_date = resolve_trade_date(conn, args.trade_date)
        rows = run_screener(
            conn=conn,
            trade_date=trade_date,
            rule_sql=rule_sql,
            rule_params=rule_params,
            sort_by=args.sort_by,
            sort_order=args.sort_order,
            limit=args.limit,
        )
    except (sqlite3.DatabaseError, RuleValidationError, RuntimeError, ValueError) as exc:
        print("screening failed: {0}".format(exc), file=sys.stderr)
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass

    payload = rows_to_dicts(rows)
    summary = "trade_date={0}, matched={1}".format(trade_date, len(rows))

    if args.output == "json":
        output_text = json.dumps(
            {"summary": summary, "rows": payload},
            ensure_ascii=False,
            indent=2,
        )
    else:
        if rows:
            output_text = summary + "\n" + format_table(rows)
        else:
            output_text = summary + "\n(no matched stocks)"

    if args.output_file:
        with open(args.output_file, "w", encoding="utf-8") as f:
            f.write(output_text + "\n")
        print("saved to {0}".format(args.output_file))
    else:
        print(output_text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
