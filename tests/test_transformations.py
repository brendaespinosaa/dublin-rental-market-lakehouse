
"""
Tests for Bronze, Silver, and Gold transformations.
"""

from datetime import date

from pyspark.sql import functions as F

from src.transformations.bronze import build_bronze_dataframe
from src.transformations.silver import build_silver_dataframe
from src.transformations.gold import (
    build_gold_tables,
    build_gold_rent_trends,
)


def test_bronze_record_count(bronze_df):
    """Test whether Bronze preserves the source record count."""

    assert bronze_df.count() == 4


def test_bronze_preserves_rental_values(bronze_df):
    """Test whether Bronze preserves the original rental values."""

    values = [
        row["average_monthly_rent_eur"]
        for row in bronze_df.select(
            "average_monthly_rent_eur"
        ).collect()
    ]

    assert sorted(values) == [1000.0, 1100.0, 1200.0, 1300.0]


def test_bronze_preserves_dimension_metadata(bronze_df):
    """Test whether Bronze correctly maps dimension metadata."""

    records = (
        bronze_df
        .select(
            "quarter_code",
            "bedroom_code",
            "property_type_code",
            "location_code",
        )
        .distinct()
        .collect()
    )

    assert len(records) == 4

    assert {
        row["quarter_code"] for row in records
    } == {"2024Q1", "2024Q2"}

    assert {
        row["location_code"] for row in records
    } == {"120500"}


def test_silver_creates_reporting_period(silver_df):
    """Test whether Silver correctly converts quarterly periods into dates."""

    periods = (
        silver_df
        .select(
            "quarter_code",
            "quarter_start_date",
        )
        .distinct()
        .orderBy("quarter_code")
        .collect()
    )

    assert [
        (row["quarter_code"], row["quarter_start_date"])
        for row in periods
    ] == [
        ("2024Q1", date(2024, 1, 1)),
        ("2024Q2", date(2024, 4, 1)),
    ]


def test_silver_creates_validity_flags(silver_df):
    """Test whether Silver correctly identifies valid and missing rents."""

    result = silver_df.agg(
        F.sum(
            F.when(F.col("is_valid_rent"), 1).otherwise(0)
        ).alias("valid_count"),
        F.sum(
            F.when(F.col("is_missing_rent"), 1).otherwise(0)
        ).alias("missing_count"),
    ).first()

    assert result["valid_count"] == 4
    assert result["missing_count"] == 0


def test_silver_preserves_null_values(bronze_df):
    """Test whether Silver preserves missing source rental values."""

    bronze_with_null = bronze_df.withColumn(
        "average_monthly_rent_eur",
        F.when(
            F.col("average_monthly_rent_eur") == 1000.0,
            F.lit(None).cast("double"),
        ).otherwise(F.col("average_monthly_rent_eur")),
    )

    silver_with_null = build_silver_dataframe(bronze_with_null)

    missing_records = silver_with_null.filter(
        F.col("average_monthly_rent_eur").isNull()
    )

    assert missing_records.count() == 1

    missing_record = missing_records.first()

    assert missing_record["is_missing_rent"] is True
    assert missing_record["is_valid_rent"] is False


def test_silver_preserves_source_record_count(silver_df, bronze_df):
    """Test whether Silver preserves the number of Bronze observations."""

    assert silver_df.count() == bronze_df.count()


def test_gold_builds_four_analytical_tables(silver_df):
    """Test whether all expected Gold datasets are created."""

    gold_tables = build_gold_tables(silver_df)

    assert len(gold_tables) == 4

    assert set(gold_tables.keys()) == {
        "gold_dublin_rent_trends",
        "gold_dublin_rent_by_property_type",
        "gold_dublin_rent_by_bedrooms",
        "gold_dublin_rent_by_location",
    }


def test_gold_trends_contains_valid_dublin_records(silver_df):
    """Test whether Gold trends contain valid Dublin aggregate records."""

    trends = build_gold_rent_trends(silver_df)

    assert trends.count() == 2

    assert trends.filter(
        F.col("location_code") != "120500"
    ).count() == 0

    assert trends.filter(
        F.col("average_monthly_rent_eur").isNull()
        | (F.col("average_monthly_rent_eur") <= 0)
    ).count() == 0


def test_gold_trends_calculates_expected_values(silver_df):
    """Test whether Gold trends contain the expected quarterly rents."""

    trends = build_gold_rent_trends(silver_df)

    actual_values = {
        row["quarter_code"]: float(row["average_monthly_rent_eur"])
        for row in trends.select(
            "quarter_code",
            "average_monthly_rent_eur",
        ).collect()
    }

    assert actual_values == {
        "2024Q1": 1000.0,
        "2024Q2": 1200.0,
    }
