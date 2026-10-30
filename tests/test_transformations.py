
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transformations.bronze import build_bronze_dataframe
from src.transformations.silver import build_silver_dataframe
from src.transformations.gold import (
    build_gold_tables,
    build_gold_rent_trends,
)


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for transformation tests."""

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("DublinRentalMarketTests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture
def sample_jsonstat_metadata():
    """Provide a small JSON-stat structure for testing."""

    dimension_ids = [
        "STATISTIC",
        "TLIST(Q1)",
        "C02970V03592",
        "C02969V03591",
        "C03004V03625",
    ]

    dimension_sizes = [1, 2, 1, 2, 1]

    dimension_metadata = {
        "STATISTIC": {
            "codes": ["RIQ02"],
            "labels": {"RIQ02": "RTB Average Monthly Rent Report"},
        },
        "TLIST(Q1)": {
            "codes": ["2024Q1", "2024Q2"],
            "labels": {
                "2024Q1": "2024 Q1",
                "2024Q2": "2024 Q2",
            },
        },
        "C02970V03592": {
            "codes": ["-"],
            "labels": {"-": "All bedrooms"},
        },
        "C02969V03591": {
            "codes": ["-", "04"],
            "labels": {
                "-": "All property types",
                "04": "Apartment",
            },
        },
        "C03004V03625": {
            "codes": ["120500"],
            "labels": {"120500": "Dublin"},
        },
    }

    return dimension_ids, dimension_sizes, dimension_metadata


@pytest.fixture
def bronze_df(spark, sample_jsonstat_metadata):
    """Create a small Bronze DataFrame."""

    dimension_ids, dimension_sizes, metadata = sample_jsonstat_metadata

    observations = [1000.0, 1100.0, 1200.0, 1300.0]

    return build_bronze_dataframe(
        spark,
        observations,
        dimension_ids,
        dimension_sizes,
        metadata,
    )


@pytest.fixture
def silver_df(bronze_df):
    """Create a small Silver DataFrame."""

    return build_silver_dataframe(bronze_df)


def test_bronze_record_count(bronze_df):
    assert bronze_df.count() == 4


def test_bronze_preserves_rental_values(bronze_df):
    values = [
        row["average_monthly_rent_eur"]
        for row in bronze_df.select(
            "average_monthly_rent_eur"
        ).collect()
    ]

    assert sorted(values) == [1000.0, 1100.0, 1200.0, 1300.0]


def test_silver_creates_reporting_period(silver_df):
    periods = [
        row["quarter_start_date"]
        for row in silver_df.select(
            "quarter_start_date"
        ).collect()
    ]

    assert periods == [
        # The first two observations belong to 2024Q1,
        # and the next two belong to 2024Q2.
        periods[0],
        periods[1],
        periods[2],
        periods[3],
    ]

    assert all(period is not None for period in periods)
    assert periods[0].strftime("%Y-%m-%d") == "2024-01-01"
    assert periods[2].strftime("%Y-%m-%d") == "2024-04-01"


def test_silver_creates_validity_flags(silver_df):
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


def test_silver_preserves_null_values(spark, bronze_df):
    bronze_with_null = bronze_df.withColumn(
        "average_monthly_rent_eur",
        F.when(
            F.col("average_monthly_rent_eur") == 1000.0,
            F.lit(None).cast("double"),
        ).otherwise(F.col("average_monthly_rent_eur")),
    )

    silver_with_null = build_silver_dataframe(bronze_with_null)

    assert silver_with_null.filter(
        F.col("average_monthly_rent_eur").isNull()
    ).count() == 1


def test_gold_builds_four_analytical_tables(silver_df):
    gold_tables = build_gold_tables(silver_df)

    assert len(gold_tables) == 4

    assert set(gold_tables.keys()) == {
        "gold_dublin_rent_trends",
        "gold_dublin_rent_by_property_type",
        "gold_dublin_rent_by_bedrooms",
        "gold_dublin_rent_by_location",
    }


def test_gold_trends_contains_valid_dublin_records(silver_df):
    trends = build_gold_rent_trends(silver_df)

    assert trends.count() == 2

    assert trends.filter(
        F.col("location_code") != "120500"
    ).count() == 0

    assert trends.filter(
        F.col("average_monthly_rent_eur").isNull()
        | (F.col("average_monthly_rent_eur") <= 0)
    ).count() == 0
