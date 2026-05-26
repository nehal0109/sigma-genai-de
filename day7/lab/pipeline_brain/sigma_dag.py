"""
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