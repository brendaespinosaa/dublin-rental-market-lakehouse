
import pytest

from pyspark.sql import functions as F

from src.quality.validation import (
    run_quality_checks,
    summarize_quality_results,
)

from src.transformations.gold import build_gold_tables


def test_quality_checks_pass_with_valid_data(
    bronze_df,
    silver_df,
):
    """Test all quality checks with valid sample data."""

    gold_tables = build_gold_tables(silver_df)

    results = run_quality_checks(
        bronze_df=bronze_df,
        silver_df=silver_df,
        gold_tables=gold_tables,
        expected_record_count=4,
    )

    assert len(results) == 9
    assert all(result["passed"] for result in results)


def test_quality_summary():
    """Test quality result summary."""

    results = [
        {"check": "Check 1", "passed": True, "details": "Passed"},
        {"check": "Check 2", "passed": True, "details": "Passed"},
        {"check": "Check 3", "passed": False, "details": "Failed"},
    ]

    summary = summarize_quality_results(results)

    assert summary["total_checks"] == 3
    assert summary["passed_checks"] == 2
    assert summary["failed_checks"] == 1
    assert summary["all_passed"] is False


def test_quality_detects_missing_bronze_records(
    bronze_df,
    silver_df,
):
    """Test detection of an incorrect source record count."""

    gold_tables = build_gold_tables(silver_df)

    results = run_quality_checks(
        bronze_df=bronze_df,
        silver_df=silver_df,
        gold_tables=gold_tables,
        expected_record_count=10,
    )

    bronze_check = next(
        result for result in results
        if result["check"] == "Bronze record count matches source"
    )

    assert bronze_check["passed"] is False


def test_quality_detects_invalid_rental_values(
    bronze_df,
    silver_df,
):
    """Test detection of non-positive rental values."""

    invalid_silver = silver_df.withColumn(
        "average_monthly_rent_eur",
        F.when(
            F.col("average_monthly_rent_eur") == 1000,
            F.lit(0).cast("decimal(18,2)"),
        ).otherwise(F.col("average_monthly_rent_eur")),
    ).withColumn(
        "is_valid_rent",
        F.col("average_monthly_rent_eur").isNotNull()
        & (F.col("average_monthly_rent_eur") > 0),
    )

    gold_tables = build_gold_tables(invalid_silver)

    results = run_quality_checks(
        bronze_df=bronze_df,
        silver_df=invalid_silver,
        gold_tables=gold_tables,
        expected_record_count=4,
    )

    invalid_check = next(
        result for result in results
        if result["check"] == "No non-positive rental values"
    )

    assert invalid_check["passed"] is False


def test_quality_detects_inconsistent_missing_flags(
    bronze_df,
    silver_df,
):
    """Test detection of inconsistent missing value flags."""

    inconsistent_silver = silver_df.withColumn(
        "is_missing_rent",
        F.lit(True),
    )

    gold_tables = build_gold_tables(inconsistent_silver)

    results = run_quality_checks(
        bronze_df=bronze_df,
        silver_df=inconsistent_silver,
        gold_tables=gold_tables,
        expected_record_count=4,
    )

    missing_check = next(
        result for result in results
        if result["check"] == "Missing value flags are consistent"
    )

    assert missing_check["passed"] is False
