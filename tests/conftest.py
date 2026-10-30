
"""
Shared pytest fixtures for the Dublin Rental Market Lakehouse tests.
"""

import pytest

from pyspark.sql import SparkSession

from src.transformations.bronze import build_bronze_dataframe
from src.transformations.silver import build_silver_dataframe


@pytest.fixture(scope="session")
def spark():
    """
    Create a shared local Spark session for the test suite.
    """

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("DublinRentalMarketTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture
def sample_jsonstat_metadata():
    """
    Provide a small JSON-stat structure for testing.
    """

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
            "labels": {
                "RIQ02": "RTB Average Monthly Rent Report",
            },
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
            "labels": {
                "-": "All bedrooms",
            },
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
            "labels": {
                "120500": "Dublin",
            },
        },
    }

    return dimension_ids, dimension_sizes, dimension_metadata


@pytest.fixture
def sample_observations():
    """
    Provide sample rental observations in JSON-stat flattened order.
    """

    return [1000.0, 1100.0, 1200.0, 1300.0]


@pytest.fixture
def bronze_df(
    spark,
    sample_jsonstat_metadata,
    sample_observations,
):
    """
    Create a sample Bronze DataFrame.
    """

    dimension_ids, dimension_sizes, metadata = sample_jsonstat_metadata

    return build_bronze_dataframe(
        spark=spark,
        observations=sample_observations,
        dimension_ids=dimension_ids,
        dimension_sizes=dimension_sizes,
        dimension_metadata=metadata,
    )


@pytest.fixture
def silver_df(bronze_df):
    """
    Create a sample Silver DataFrame.
    """

    return build_silver_dataframe(bronze_df)


@pytest.fixture
def bronze_df_with_null(bronze_df):
    """
    Create a Bronze DataFrame containing a missing rental value.
    """

    from pyspark.sql import functions as F

    return bronze_df.withColumn(
        "average_monthly_rent_eur",
        F.when(
            F.col("average_monthly_rent_eur") == 1000.0,
            F.lit(None).cast("double"),
        ).otherwise(
            F.col("average_monthly_rent_eur")
        ),
    )


@pytest.fixture
def silver_df_with_null(bronze_df_with_null):
    """
    Create a Silver DataFrame containing a missing rental value.
    """

    return build_silver_dataframe(bronze_df_with_null)
