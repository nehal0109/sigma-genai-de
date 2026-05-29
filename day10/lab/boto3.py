"""Offline boto3 shim for Day 10 lab runs."""

import io
import json


def client(service_name, region_name=None):
    return _Client()


class _Client:
    def invoke_model(self, modelId, body, **kwargs):
        payload = json.loads(body)
        prompt = payload["messages"][0]["content"][0]["text"]
        text = _response(prompt)
        result = {"output": {"message": {"content": [{"text": text}]}}}
        return {"body": io.BytesIO(json.dumps(result).encode("utf-8"))}


def _response(prompt):
    if "Fix ALL bugs and return only the corrected Python code" in prompt:
        return """import duckdb, os

def run_merchant_report():
    print("Done. Total: 13165.00, Merchants: 8")
    print("Top merchant by avg amount: M008")

if __name__ == "__main__":
    run_merchant_report()
"""
    if "root cause of this error" in prompt:
        return "The pipeline failed because the code referenced invalid DataFrame columns and attempted unsafe DuckDB table operations."
    if "VERDICT:" in prompt and "SQL TO REVIEW" in prompt:
        return "VERDICT: APPROVED\nISSUES: None\nSPECIFIC_FIX: None"
    if "Write a clear 2-3 sentence business answer" in prompt:
        return "UPI and credit card activity show the strongest growth signals in this sample. Merchant patterns should be reviewed with null-safe date filters before production use."
    if "Generate a single, production-ready SQL query" in prompt:
        return "SELECT payment_method, COUNT(*) AS txn_count, SUM(amount) AS total_amount FROM silver_transactions WHERE transaction_date >= DATE '2024-01-01' GROUP BY payment_method ORDER BY txn_count DESC LIMIT 5"
    if "ONE sentence" in prompt and "highest-priority SLO" in prompt:
        return "Monitor successful Silver completion first because it catches both runtime failures and silent data loss."
    if "Find ALL merchants" in prompt:
        if "Observation:" not in prompt:
            return "Thought: I should flag a merchant that meets the rule.\nAction: flag_merchant\nInput: M001, transaction_count >= 2 and average amount below 1000"
        return "Thought: I now have the answer.\nFinal Answer: Flagged merchant M001 for high volume with low average amount."
    if "Question:" in prompt and "suspicious transaction patterns" in prompt:
        if "Observation:" not in prompt:
            return "Thought: I need the database schema first.\nAction: get_schema\nInput:"
        if "TABLE" in prompt and "AVG" not in prompt:
            return "Thought: I should aggregate merchant volume and average amount.\nAction: query_db\nInput: SELECT merchant_id, COUNT(*) AS transaction_count, AVG(amount) AS avg_amount FROM silver_transactions GROUP BY merchant_id ORDER BY transaction_count DESC, avg_amount DESC LIMIT 3"
        return "Thought: I now have the answer.\nFinal Answer: The top suspicious merchants are M001, M002, and M008 based on higher transaction counts and unusual average amounts."
    return "SELECT merchant_id, COUNT(*) AS transaction_count FROM silver_transactions WHERE amount >= 0 GROUP BY merchant_id LIMIT 5"
