
"""
Gold layer transformations for the Dublin Rental Market Lakehouse.

Creates analytical datasets from validated Silver rental observations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DUBLIN_AGGREGATE_CODE = "120500"


def _valid_rent_records(silver_df: DataFrame) -> DataFrame:
    """Return only valid rental observations."""

    return silver_df.filter(
        F.col("is_valid_rent") == True
    )


def build_gold_rent_trends(silver_df: DataFrame) -> DataFrame:
    """
    Build the historical Dublin rental trend dataset.

    Uses the Dublin aggregate, all bedroom categories,
    and all property types.
    """

    return (
        _valid_rent_records(silver_df)
        .filter(
            (F.col("location_code") == DUBLIN_AGGREGATE_CODE)
            & (F.col("bedroom_code") == "-")
            & (F.col("property_type_code") == "-")
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
        .orderBy("quarter_start_date")
    )


def build_gold_rent_by_property_type(
    silver_df: DataFrame,
) -> DataFrame:
    """
    Build the Dublin rental dataset by property type.
    """

    return (
        _valid_rent_records(silver_df)
        .filter(
            (F.col("location_code") == DUBLIN_AGGREGATE_CODE)
            & (F.col("bedroom_code") == "-")
            & (F.col("property_type_code") != "-")
        )
        .select(
            "quarter_code",
            "quarter",
            "year",
            "quarter_number",
            "quarter_start_date",
            "property_type_code",
            "property_type",
            "average_monthly_rent_eur",
        )
        .orderBy("quarter_start_date", "property_type_code")
    )


def build_gold_rent_by_bedrooms(
    silver_df: DataFrame,
) -> DataFrame:
    """
    Build the Dublin rental dataset by bedroom category.
    """

    return (
        _valid_rent_records(silver_df)
        .filter(
            (F.col("location_code") == DUBLIN_AGGREGATE_CODE)
            & (F.col("bedroom_code") != "-")
            & (F.col("property_type_code") == "-")
        )
        .select(
            "quarter_code",
            "quarter",
            "year",
            "quarter_number",
            "quarter_start_date",
            "bedroom_code",
            "bedroom_category",
            "average_monthly_rent_eur",
        )
        .orderBy("quarter_start_date", "bedroom_code")
    )


def build_gold_rent_by_location(
    silver_df: DataFrame,
) -> DataFrame:
    """
    Build the Dublin rental dataset by detailed location.

    Excludes the Dublin aggregate to avoid mixing it with
    individual districts and localities.
    """

    return (
        _valid_rent_records(silver_df)
        .filter(
            (F.col("location_code") != DUBLIN_AGGREGATE_CODE)
            & F.lower(F.col("location")).contains("dublin")
            & (F.col("bedroom_code") == "-")
            & (F.col("property_type_code") == "-")
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
        .orderBy("quarter_start_date", "location_code")
    )


def build_gold_tables(silver_df: DataFrame) -> dict[str, DataFrame]:
    """
    Build all Gold analytical datasets.

    Returns:
        Dictionary mapping Gold table names to Spark DataFrames.
    """

    return {
        "gold_dublin_rent_trends": build_gold_rent_trends(silver_df),
        "gold_dublin_rent_by_property_type": (
            build_gold_rent_by_property_type(silver_df)
        ),
        "gold_dublin_rent_by_bedrooms": (
            build_gold_rent_by_bedrooms(silver_df)
        ),
        "gold_dublin_rent_by_location": (
            build_gold_rent_by_location(silver_df)
        ),
    }
