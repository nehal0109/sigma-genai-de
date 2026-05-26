import json
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