"""Tiny DuckDB stand-in for local Day 10 lab execution."""


class DataFrame:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.empty = not self.rows

    def __len__(self):
        return len(self.rows)

    def to_string(self, index=False, max_rows=20):
        if not self.rows:
            return ""
        cols = list(self.rows[0])
        lines = [" ".join(cols)]
        for row in self.rows[:max_rows]:
            lines.append(" ".join(str(row.get(c, "")) for c in cols))
        return "\n".join(lines)


class Connection:
    def __init__(self, path=None, read_only=False):
        self.last = []

    def execute(self, sql, params=None):
        low = " ".join(sql.lower().split())
        if low.startswith("show tables"):
            self.last = [("silver_transactions",), ("gold_merchant_performance",), ("gold_daily_summary",)]
        elif low.startswith("describe"):
            self.last = [
                ("transaction_id", "VARCHAR"),
                ("amount", "DOUBLE"),
                ("status", "VARCHAR"),
                ("merchant_id", "VARCHAR"),
                ("customer_id", "VARCHAR"),
                ("transaction_date", "DATE"),
                ("payment_method", "VARCHAR"),
            ]
        elif "count(*) - count(distinct transaction_id)" in low:
            self.last = [(0,)]
        elif "count(*)" in low:
            self.last = [(14,)]
        elif "unpivot" in low:
            self.last = [("transaction_id", 0.0), ("amount", 0.0)]
        else:
            self.last = [
                {"merchant_id": "M001", "transaction_count": 3, "avg_amount": 420.0, "payment_method": "UPI", "total_amount": 1260.0},
                {"merchant_id": "M002", "transaction_count": 2, "avg_amount": 1045.25, "payment_method": "CREDIT_CARD", "total_amount": 2090.5},
                {"merchant_id": "M008", "transaction_count": 2, "avg_amount": 2425.0, "payment_method": "CREDIT_CARD", "total_amount": 4850.0},
            ]
        return self

    def fetchall(self):
        if self.last and isinstance(self.last[0], dict):
            return [tuple(row.values()) for row in self.last]
        return self.last

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else (0,)

    def fetchdf(self):
        rows = self.last if self.last and isinstance(self.last[0], dict) else []
        return DataFrame(rows)

    def close(self):
        pass


def connect(path=None, read_only=False):
    return Connection(path, read_only)
