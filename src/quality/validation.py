
"""
Data quality validation utilities for the Dublin Rental Market Lakehouse.

Runs structural, consistency, and analytical checks across
the Bronze, Silver, and Gold layers.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_quality_checks(
    bronze_df: DataFrame,
    silver_df: DataFrame,
    gold_tables: dict[str, DataFrame],
    expected_record_count: int,
) -> list[dict]:
    """
    Execute data quality checks across the Medallion architecture.

    Args:
        bronze_df: Bronze layer DataFrame.
        silver_df: Silver layer DataFrame.
        gold_tables: Dictionary of Gold DataFrames.
        expected_record_count: Number of source observations.

    Returns:
        List of quality check results.
    """

    results = []

    def add_result(
        check_name: str,
        passed: bool,
        details: str,
    ) -> None:
        results.append({
            "check": check_name,
            "passed": bool(passed),
            "details": details,
        })

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()

    # 1. Bronze record count matches the source.
    add_result(
        "Bronze record count matches source",
        bronze_count == expected_record_count,
        f"Expected: {expected_record_count}; Actual: {bronze_count}",
    )

    # 2. Silver preserves all Bronze records.
    add_result(
        "Silver preserves Bronze record count",
        silver_count == bronze_count,
        f"Bronze: {bronze_count}; Silver: {silver_count}",
    )

    # 3. No duplicate dimensional combinations.
    dimension_columns = [
        "statistic_code",
        "quarter_code",
        "bedroom_code",
        "property_type_code",
        "location_code",
    ]

    duplicate_count = (
        silver_df
        .groupBy(*dimension_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    add_result(
        "No duplicate dimensional combinations",
        duplicate_count == 0,
        f"Duplicate combinations: {duplicate_count}",
    )

    # 4. No non-positive rental values.
    invalid_rent_count = (
        silver_df
        .filter(
            F.col("average_monthly_rent_eur").isNotNull()
            & (F.col("average_monthly_rent_eur") <= 0)
        )
        .count()
    )

    add_result(
        "No non-positive rental values",
        invalid_rent_count == 0,
        f"Invalid rental values: {invalid_rent_count}",
    )

    # 5. Missing rent flags are consistent.
    inconsistent_missing_count = (
        silver_df
        .filter(
            F.col("is_missing_rent")
            != F.col("average_monthly_rent_eur").isNull()
        )
        .count()
    )

    add_result(
        "Missing value flags are consistent",
        inconsistent_missing_count == 0,
        f"Inconsistent flags: {inconsistent_missing_count}",
    )

    # 6. Valid rent flags are consistent.
    inconsistent_valid_count = (
        silver_df
        .filter(
            F.col("is_valid_rent")
            != (
                F.col("average_monthly_rent_eur").isNotNull()
                & (F.col("average_monthly_rent_eur") > 0)
            )
        )
        .count()
    )

    add_result(
        "Valid value flags are consistent",
        inconsistent_valid_count == 0,
        f"Inconsistent flags: {inconsistent_valid_count}",
    )

    # 7. Reporting periods are valid.
    invalid_period_count = (
        silver_df
        .filter(
            F.col("year").isNull()
            | F.col("quarter_number").isNull()
            | F.col("quarter_start_date").isNull()
            | (~F.col("quarter_number").between(1, 4))
        )
        .count()
    )

    add_result(
        "Reporting periods are valid",
        invalid_period_count == 0,
        f"Invalid reporting periods: {invalid_period_count}",
    )

    # 8. Gold tables contain only valid rental values.
    gold_invalid_count = 0

    for table_name, gold_df in gold_tables.items():
        invalid_count = (
            gold_df
            .filter(
                F.col("average_monthly_rent_eur").isNull()
                | (F.col("average_monthly_rent_eur") <= 0)
            )
            .count()
        )

        gold_invalid_count += invalid_count

    add_result(
        "Gold tables contain only valid rental values",
        gold_invalid_count == 0,
        f"Invalid Gold records: {gold_invalid_count}",
    )

    # 9. Gold trend data contains one record per quarter.
    trends_df = gold_tables["gold_dublin_rent_trends"]

    trend_count = trends_df.count()
    distinct_quarters = trends_df.select("quarter_code").distinct().count()

    add_result(
        "Gold trends have unique quarterly records",
        trend_count == distinct_quarters,
        f"Records: {trend_count}; Distinct quarters: {distinct_quarters}",
    )

    return results


def summarize_quality_results(results: list[dict]) -> dict:
    """
    Summarize quality check results.

    Returns:
        Total checks, passed checks, failed checks, and overall status.
    """

    total_checks = len(results)
    passed_checks = sum(result["passed"] for result in results)
    failed_checks = total_checks - passed_checks

    return {
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "all_passed": failed_checks == 0,
    }
