"""Offline boto3 shim for the Day 7 lab.

The classroom scripts call Bedrock through boto3. In local environments without
AWS dependencies or credentials, this shim returns deterministic lab artefacts
with the same response shape as bedrock-runtime.converse().
"""

import json


def client(service_name, region_name=None):
    if service_name != "bedrock-runtime":
        raise NotImplementedError(f"offline boto3 shim only supports bedrock-runtime, got {service_name}")
    return _OfflineBedrockClient()


class _OfflineBedrockClient:
    def converse(self, modelId, system, messages, inferenceConfig):
        prompt = messages[0]["content"][0]["text"]
        text = _response_for_prompt(prompt, modelId)
        return {
            "output": {"message": {"content": [{"text": text}]}},
            "usage": {
                "inputTokens": max(100, len(prompt) // 4),
                "outputTokens": max(100, len(text) // 4),
            },
        }


def _response_for_prompt(prompt, model_id):
    if "Return ONLY this JSON structure" in prompt:
        return json.dumps(_code_review(), indent=2)
    if "schema_evolution_handler module" in prompt:
        return _schema_handler_code()
    if "Harden this PySpark pipeline" in prompt:
        return _hardened_pipeline_code()
    if "Generate an Airflow DAG" in prompt:
        return _dag_code()
    if "GOLD layer" in prompt:
        return _gold_code()
    return _bronze_silver_code()


def _bronze_silver_code():
    return '''from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import broadcast, col, current_timestamp, input_file_name, lit, row_number, when
from pyspark.sql.types import DateType, FloatType, StringType


def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    transactions = spark.read.option("header", True).csv(input_path)
    bronze = (
        transactions
        .withColumn("transaction_id", col("transaction_id").cast(StringType()))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("pipeline_run_id", lit(run_id))
        .withColumn("date", lit(run_date))
    )
    bronze.write.mode("append").partitionBy("date").parquet(output_path)
    return bronze


def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    bronze = spark.read.parquet(bronze_path).filter(col("date") == run_date)
    merchants = spark.read.parquet(merchants_path).cache()
    typed = (
        bronze
        .withColumn("amount", col("amount").cast(FloatType()))
        .withColumn("transaction_date", col("transaction_date").cast(DateType()))
        .withColumn("transaction_id", col("transaction_id").cast(StringType()))
        .withColumn("merchant_id", col("merchant_id").cast(StringType()))
        .withColumn("customer_id", col("customer_id").cast(StringType()))
    )
    filtered = typed.filter(col("transaction_id").isNotNull()).filter(col("amount") >= 0)
    window = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
    deduped = filtered.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
    silver = (
        deduped
        .join(broadcast(merchants), "merchant_id", "left")
        .withColumn("quality_flag", when(col("merchant_name").isNull(), "UNMATCHED").otherwise("CLEAN"))
    )
    silver.write.mode("append").partitionBy("date").parquet(output_path)
    return silver


def main():
    spark = SparkSession.builder.appName("SigmaTransactionPipeline").getOrCreate()
    run_date = "2024-01-15"
    run_id = "manual-run"
    ingest_bronze(spark, "data/transactions.csv", "data/bronze", run_date, run_id)
    transform_silver(spark, "data/bronze", "data/merchants", "data/silver", run_date)
'''


def _gold_code():
    return '''from pyspark.sql.functions import avg, col, count, countDistinct, first, last, sum as spark_sum, when


def build_merchant_performance(spark, silver_path, output_path, run_date):
    silver = spark.read.parquet(silver_path).filter(col("date") == run_date)
    result = silver.groupBy("merchant_id", "merchant_name", "category", "city", "date").agg(
        spark_sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
        count("*").alias("txn_count"),
        (spark_sum(when(col("status") == "FAILED", 1).otherwise(0)) / count("*") * 100).alias("failure_rate_pct"),
    )
    result.write.mode("append").partitionBy("date").parquet(f"{output_path}/merchant_performance")
    return result


def build_customer_ltv(spark, silver_path, output_path):
    silver = spark.read.parquet(silver_path)
    completed = silver.filter(col("status") == "COMPLETED")
    result = completed.groupBy("customer_id").agg(
        spark_sum("amount").alias("total_spent"),
        count("*").alias("total_txns"),
        avg("amount").alias("avg_txn_value"),
        first("transaction_date").alias("first_txn_date"),
        last("transaction_date").alias("last_txn_date"),
        first("payment_method").alias("preferred_payment_method"),
    )
    result.write.mode("overwrite").parquet(f"{output_path}/customer_ltv")
    return result


def build_daily_summary(spark, silver_path, output_path, run_date):
    silver = spark.read.parquet(silver_path).filter(col("date") == run_date)
    result = silver.groupBy("date").agg(
        spark_sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
        count("*").alias("total_txns"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("merchant_id").alias("unique_merchants"),
        (spark_sum(when(col("status") == "FAILED", 1).otherwise(0)) / count("*") * 100).alias("failure_rate_pct"),
    )
    result.write.mode("append").partitionBy("date").parquet(f"{output_path}/daily_summary")
    return result


def run_gold(spark, silver_path, gold_output_dir, run_date):
    merchant = build_merchant_performance(spark, silver_path, gold_output_dir, run_date)
    customer = build_customer_ltv(spark, silver_path, gold_output_dir)
    daily = build_daily_summary(spark, silver_path, gold_output_dir, run_date)
    run_metadata = {"run_date": run_date, "tables": 3, "status": "SUCCESS"}
    return {"merchant": merchant, "customer": customer, "daily": daily, "metadata": run_metadata}
'''


def _dag_code():
    return '''"""
Airflow DAG for the Sigma transaction Bronze -> Silver -> Gold pipeline.
Runs daily at 02:00 UTC with a 120 minute SLA and retry handling.
"""
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def on_failure_callback(context):
    task_instance = context.get("task_instance")
    logging.error("dag_id=%s task_id=%s execution_date=%s error=%s",
                  context.get("dag").dag_id, task_instance.task_id,
                  context.get("execution_date"), context.get("exception"))


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logging.error("SLA miss for dag_id=%s tasks=%s", dag.dag_id, task_list)


def extract_bronze(**context):
    """Ingest raw CSV files into Bronze Parquet."""
    logging.info("start extract_bronze %s", context["task_instance"])
    logging.info("end extract_bronze")


def transform_silver(**context):
    """Clean, enrich, and deduplicate Bronze data into Silver."""
    logging.info("start transform_silver %s", context["task_instance"])
    logging.info("end transform_silver")


def build_gold(**context):
    """Build merchant, customer, and daily Gold aggregates."""
    logging.info("start build_gold %s", context["task_instance"])
    logging.info("end build_gold")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="sigma_transaction_pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    sla_miss_callback=sla_miss_callback,
    tags=["sigma", "transactions", "daily"],
) as dag:
    extract_task = PythonOperator(task_id="extract_bronze", python_callable=extract_bronze)
    silver_task = PythonOperator(task_id="transform_silver", python_callable=transform_silver)
    gold_task = PythonOperator(task_id="build_gold", python_callable=build_gold)

    extract_task >> silver_task >> gold_task
'''


def _hardened_pipeline_code():
    return '''import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import broadcast, col, current_timestamp, input_file_name, lit, row_number, when
from pyspark.sql.types import DateType, FloatType, StringType

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("sigma_pipeline")


def _log_count(stage_name, label, dataframe):
    count_value = dataframe.count()
    LOGGER.info("[Stage: %s] %s: %s rows", stage_name, label, f"{count_value:,}")
    return count_value


def _delete_partition(base_path, run_date):
    # Idempotent daily reruns remove only the target partition before overwrite.
    shutil.rmtree(os.path.join(base_path, f"date={run_date}"), ignore_errors=True)


def ingest_bronze(spark, input_path, output_path, run_date, run_id, metadata):
    stage = "bronze"
    try:
        raw = spark.read.option("header", True).csv(input_path)
        metadata["counts"]["bronze_input"] = _log_count(stage, "input_count", raw)
        bronze = raw.withColumn("ingestion_timestamp", current_timestamp()).withColumn("source_file", input_file_name()).withColumn("pipeline_run_id", lit(run_id)).withColumn("date", lit(run_date))
        metadata["counts"]["bronze_output"] = _log_count(stage, "output_count", bronze)
        _delete_partition(output_path, run_date)
        bronze.write.mode("overwrite").partitionBy("date").parquet(output_path)
        return bronze
    except Exception as exc:
        LOGGER.exception("[Stage: %s] failed: %s", stage, exc)
        metadata["run_status"] = "FAILED"
        metadata["error_message"] = str(exc)
        raise


def transform_silver(spark, bronze_path, merchants_path, output_path, run_date, metadata):
    stage = "silver"
    try:
        # Partition pruning -- only read today's data, not full history.
        bronze = spark.read.parquet(bronze_path).filter(col("date") == run_date)
        metadata["counts"]["silver_input"] = _log_count(stage, "input_count", bronze)
        merchants = spark.read.parquet(merchants_path).cache()
        typed = bronze.withColumn("amount", col("amount").cast(FloatType())).withColumn("transaction_date", col("transaction_date").cast(DateType())).withColumn("transaction_id", col("transaction_id").cast(StringType())).withColumn("merchant_id", col("merchant_id").cast(StringType())).withColumn("customer_id", col("customer_id").cast(StringType()))
        filtered = typed.filter(col("transaction_id").isNotNull()).filter(col("amount") >= 0)
        metadata["counts"]["silver_after_filter"] = _log_count(stage, "after_filter_count", filtered)
        window = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
        deduped = filtered.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
        metadata["counts"]["silver_after_dedup"] = _log_count(stage, "after_dedup_count", deduped)
        silver = deduped.join(broadcast(merchants), "merchant_id", "left").withColumn("quality_flag", when(col("merchant_name").isNull(), "UNMATCHED").otherwise("CLEAN"))
        metadata["counts"]["silver_output"] = _log_count(stage, "output_count", silver)
        _delete_partition(output_path, run_date)
        silver.write.mode("overwrite").partitionBy("date").parquet(output_path)
        return silver
    except Exception as exc:
        LOGGER.exception("[Stage: %s] failed: %s", stage, exc)
        metadata["run_status"] = "FAILED"
        metadata["error_message"] = str(exc)
        raise


def write_run_metadata(metadata, output_dir, run_date):
    metadata["completed_at"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, f"run_metadata_{run_date}.json"), "w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)


def main():
    spark = SparkSession.builder.appName("SigmaTransactionPipeline").getOrCreate()
    run_date = "2024-01-15"
    run_id = "manual-run"
    metadata = {"pipeline_name": "sigma_transaction_pipeline", "run_date": run_date, "run_id": run_id, "run_status": "SUCCESS", "error_message": None, "started_at": datetime.now(timezone.utc).isoformat(), "counts": {}}
    try:
        ingest_bronze(spark, "data/transactions.csv", "data/bronze", run_date, run_id, metadata)
        transform_silver(spark, "data/bronze", "data/merchants", "data/silver", run_date, metadata)
    finally:
        write_run_metadata(metadata, "data/metadata", run_date)


if __name__ == "__main__":
    main()
'''


def _schema_handler_code():
    return '''from typing import Any


def detect_schema_drift(expected_schema: dict, actual_schema: dict) -> dict:
    """Compare expected and actual schemas."""
    expected = set(expected_schema)
    actual = set(actual_schema)
    new_columns = {name: actual_schema[name] for name in actual - expected}
    removed_columns = {name: expected_schema[name] for name in expected - actual}
    type_changes = {
        name: {"expected": expected_schema[name], "actual": actual_schema[name]}
        for name in expected & actual
        if expected_schema[name] != actual_schema[name]
    }
    severity = "NONE"
    if removed_columns:
        severity = "BREAKING"
    elif type_changes:
        severity = "HIGH"
    elif new_columns:
        severity = "LOW"
    return {"new_columns": new_columns, "removed_columns": removed_columns, "type_changes": type_changes, "drift_severity": severity}


def decide_action(drift_report: dict) -> dict:
    """Choose the safest action for each drifted column."""
    decisions = {}
    for name, data_type in drift_report.get("new_columns", {}).items():
        if data_type in {"float", "double", "decimal", "int"}:
            action = "FLAG_ANOMALY"
            reason = "numeric field may affect financial aggregates"
            risk = "MEDIUM"
        else:
            action = "ADD_TO_SCHEMA"
            reason = "additive nullable field is safe to preserve"
            risk = "LOW"
        decisions[name] = {"action": action, "reason": reason, "risk_level": risk}
    for name in drift_report.get("removed_columns", {}):
        decisions[name] = {"action": "HALT", "reason": "removed columns break downstream consumers", "risk_level": "HIGH"}
    return decisions


def apply_schema_evolution(spark_df: Any, decisions: dict, updated_schema: dict):
    """Apply schema decisions to a DataFrame when one is supplied."""
    migration_notes = []
    evolved = spark_df
    for column_name, decision in decisions.items():
        action = decision["action"]
        migration_notes.append(f"{column_name}: {action} - {decision['reason']}")
        if evolved is not None and action == "DROP_SILENTLY":
            evolved = evolved.drop(column_name)
        elif evolved is not None and action == "FLAG_ANOMALY":
            from pyspark.sql.functions import lit
            evolved = evolved.withColumn(f"{column_name}_schema_flag", lit("ANOMALY"))
    return evolved, migration_notes


def handle_drift(expected_schema: dict, actual_schema: dict, spark_df=None) -> dict:
    """Detect, decide, optionally apply, and return a full drift report."""
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
    report = {"drift_report": drift_report, "decisions": decisions, "migration_notes": migration_notes}
    print(report)
    return report
'''


def _code_review():
    checkpoints = [
        {"id": 1, "name": "IDEMPOTENCY", "status": "FAIL", "finding": "Some generated writes use append mode.", "fix": "Delete the target date partition and write with overwrite."},
        {"id": 2, "name": "ERROR_HANDLING", "status": "FAIL", "finding": "Stage functions lack try/except boundaries.", "fix": "Wrap each stage, log context, and re-raise."},
        {"id": 3, "name": "PARTITION_PRUNING", "status": "PASS", "finding": "Date filters are present on daily reads.", "fix": "Keep partition filters before joins."},
        {"id": 4, "name": "ROW_COUNT_LOGGING", "status": "WARN", "finding": "No row count logging in the generated scaffold.", "fix": "Log input, filtered, deduped, and output counts."},
        {"id": 5, "name": "BUSINESS_RULES", "status": "PASS", "finding": "Revenue uses COMPLETED transactions only.", "fix": "Add tests to lock this behavior."},
        {"id": 6, "name": "NULL_HANDLING", "status": "WARN", "finding": "Primary key nulls are filtered; other critical nulls need checks.", "fix": "Validate merchant_id, customer_id, and status."},
        {"id": 7, "name": "BROADCAST_HINT", "status": "PASS", "finding": "Merchant join uses broadcast.", "fix": "Keep merchant dimension small or remove hint."},
        {"id": 8, "name": "HARDCODED_PATHS", "status": "WARN", "finding": "Main contains sample paths.", "fix": "Read paths from config."},
        {"id": 9, "name": "SCHEMA_VALIDATION", "status": "WARN", "finding": "No explicit expected-column check.", "fix": "Validate source columns before transforms."},
        {"id": 10, "name": "DEDUPLICATION", "status": "PASS", "finding": "Silver deduplicates by transaction_id.", "fix": "Test latest ingestion_timestamp wins."},
        {"id": 11, "name": "METADATA_OUTPUT", "status": "WARN", "finding": "Generated scaffold lacks run metadata JSON.", "fix": "Write run metadata at completion."},
        {"id": 12, "name": "IMPORTS", "status": "PASS", "finding": "Imports are explicit.", "fix": "Avoid wildcard imports."},
    ]
    return {
        "checkpoints": checkpoints,
        "summary": {
            "pass_count": 5,
            "fail_count": 2,
            "warn_count": 5,
            "merge_recommendation": "APPROVE_WITH_CHANGES",
            "top_3_fixes": [
                "Replace append writes with delete-partition-then-overwrite.",
                "Add stage-level error handling and re-raise exceptions.",
                "Add run metadata and row count logging.",
            ],
        },
    }
