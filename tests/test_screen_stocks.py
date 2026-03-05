import unittest

from screen_stocks import FIELD_SQL_MAP, RuleValidationError, build_rule_sql


class BuildRuleSqlTests(unittest.TestCase):
    def test_nested_and_or_rules(self) -> None:
        rule = {
            "logic": "and",
            "rules": [
                {"field": "close", "op": ">", "value": 10},
                {
                    "logic": "or",
                    "rules": [
                        {"field": "pct_chg", "op": ">=", "value": 3},
                        {"field": "amount", "op": ">", "value": 100000000},
                    ],
                },
            ],
        }

        sql, params = build_rule_sql(rule, FIELD_SQL_MAP)

        self.assertEqual(sql, "(q.close > ? AND (q.pct_chg >= ? OR q.amount > ?))")
        self.assertEqual(params, [10, 3, 100000000])

    def test_in_operator(self) -> None:
        rule = {"field": "symbol", "op": "in", "value": ["000001", "600519"]}

        sql, params = build_rule_sql(rule, FIELD_SQL_MAP)

        self.assertEqual(sql, "q.symbol IN (?, ?)")
        self.assertEqual(params, ["000001", "600519"])

    def test_invalid_logic_raises(self) -> None:
        rule = {"logic": "xor", "rules": [{"field": "close", "op": ">", "value": 10}]}

        with self.assertRaises(RuleValidationError):
            build_rule_sql(rule, FIELD_SQL_MAP)


if __name__ == "__main__":
    unittest.main()
