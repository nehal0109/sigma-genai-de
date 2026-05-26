"""Fixed version of generated_pipeline.py after Module 5 review.

This file addresses the two FAIL items from code_review.json:
1. Idempotency: daily partitions are deleted before overwrite.
2. Error handling: each stage logs context and re-raises failures.
"""

import logging
import os
import shutil
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import broadcast, col, current_timestamp, input_file_name, lit, row_number, when
from pyspark.sql.types import DateType, FloatType, StringType


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("sigma_fixed_pipeline")


def delete_partition(base_path, run_date):
    """Remove only the rerun date partition before writing new output."""
    shutil.rmtree(os.path.join(base_path, f"date={run_date}"), ignore_errors=True)


def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    """Ingest raw transaction CSV rows into Bronze with metadata."""
    try:
        raw = spark.read.option("header", True).csv(input_path)
        bronze = (
            raw
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
            .withColumn("pipeline_run_id", lit(run_id))
            .withColumn("date", lit(run_date))
        )
        delete_partition(output_path, run_date)
        bronze.write.mode("overwrite").partitionBy("date").parquet(output_path)
        return bronze
    except Exception:
        LOGGER.exception("Bronze ingestion failed for run_date=%s run_id=%s", run_date, run_id)
        raise


def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    """Clean, deduplicate, and enrich Bronze transactions into Silver."""
    try:
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
        valid = typed.filter(col("transaction_id").isNotNull()).filter(col("amount") >= 0)
        window = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
        deduped = valid.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
        silver = (
            deduped
            .join(broadcast(merchants), "merchant_id", "left")
            .withColumn("quality_flag", when(col("merchant_name").isNull(), "UNMATCHED").otherwise("CLEAN"))
        )
        delete_partition(output_path, run_date)
        silver.write.mode("overwrite").partitionBy("date").parquet(output_path)
        return silver
    except Exception:
        LOGGER.exception("Silver transform failed for run_date=%s", run_date)
        raise


def main():
    spark = SparkSession.builder.appName("SigmaTransactionPipelineFixed").getOrCreate()
    run_date = "2024-01-15"
    run_id = "manual-run"
    ingest_bronze(spark, "data/transactions.csv", "data/bronze", run_date, run_id)
    transform_silver(spark, "data/bronze", "data/merchants", "data/silver", run_date)


if __name__ == "__main__":
    main()
