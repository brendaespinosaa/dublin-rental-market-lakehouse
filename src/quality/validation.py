
"""
Data quality validation utilities for the Dublin Rental Market Lakehouse.

Runs structural, consistency, and analytical checks across
the Bronze, Silver, and Gold layers.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DUBLIN_AGGREGATE_CODE = "120500"

DIMENSION_COLUMNS = [
    "statistic_code",
    "quarter_code",
    "bedroom_code",
    "property_type_code",
    "location_code",
]


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

    required_gold_tables = {
        "gold_dublin_rent_trends",
        "gold_dublin_rent_by_property_type",
        "gold_dublin_rent_by_bedrooms",
        "gold_dublin_rent_by_location",
    }

    missing_gold_tables = required_gold_tables - set(gold_tables)

    if missing_gold_tables:
        raise ValueError(
            f"Missing required Gold tables: {sorted(missing_gold_tables)}"
        )

    required_silver_columns = set(
        DIMENSION_COLUMNS
        + [
            "average_monthly_rent_eur",
            "is_missing_rent",
            "is_valid_rent",
            "year",
            "quarter_number",
            "quarter_start_date",
        ]
    )

    missing_silver_columns = required_silver_columns - set(silver_df.columns)

    if missing_silver_columns:
        raise ValueError(
            f"Missing required Silver columns: "
            f"{sorted(missing_silver_columns)}"
        )

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
    duplicate_count = (
        silver_df
        .groupBy(*DIMENSION_COLUMNS)
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

    # 7. Reporting periods are valid and internally consistent.
    invalid_period_count = (
        silver_df
        .filter(
            F.col("year").isNull()
            | F.col("quarter_number").isNull()
            | F.col("quarter_start_date").isNull()
            | (~F.col("quarter_number").between(1, 4))
            | (
                F.year("quarter_start_date")
                != F.col("year")
            )
            | (
                F.quarter("quarter_start_date")
                != F.col("quarter_number")
            )
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
        if "average_monthly_rent_eur" not in gold_df.columns:
            raise ValueError(
                f"Missing rental value column in Gold table: {table_name}"
            )

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

    distinct_quarters = (
        trends_df
        .select("quarter_code")
        .distinct()
        .count()
    )

    add_result(
        "Gold trends have unique quarterly records",
        trend_count == distinct_quarters,
        f"Records: {trend_count}; Distinct quarters: {distinct_quarters}",
    )

    # 10. Gold trends reconcile with the corresponding Silver records.
    expected_trends = (
        silver_df
        .filter(
            (F.col("location_code") == DUBLIN_AGGREGATE_CODE)
            & (F.col("bedroom_code") == "-")
            & (F.col("property_type_code") == "-")
            & (F.col("is_valid_rent") == True)
        )
        .select(
            "quarter_code",
            "quarter",
            "year",
            "quarter_number",
            "quarter_start_date",
            "location_code",
            "location",
            "average_monthly_rent_eur",
        )
    )

    actual_trends = trends_df.select(
        "quarter_code",
        "quarter",
        "year",
        "quarter_number",
        "quarter_start_date",
        "location_code",
        "location",
        "average_monthly_rent_eur",
    )

    missing_in_gold = expected_trends.exceptAll(actual_trends).count()

    unexpected_in_gold = actual_trends.exceptAll(expected_trends).count()

    reconciliation_passed = (
        missing_in_gold == 0
        and unexpected_in_gold == 0
    )

    add_result(
        "Gold trends reconcile with Silver",
        reconciliation_passed,
        (
            f"Missing in Gold: {missing_in_gold}; "
            f"Unexpected in Gold: {unexpected_in_gold}"
        ),
    )

    return results


def summarize_quality_results(results: list[dict]) -> dict:
    """
    Summarize quality check results.

    Returns:
        Total checks, passed checks, failed checks, and overall status.
    """

    total_checks = len(results)

    passed_checks = sum(
        result["passed"] for result in results
    )

    failed_checks = total_checks - passed_checks

    return {
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "all_passed": failed_checks == 0,
    }
