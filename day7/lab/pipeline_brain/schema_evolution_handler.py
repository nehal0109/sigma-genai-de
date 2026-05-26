from typing import Any


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