
"""
Silver layer transformations for the Dublin Rental Market Lakehouse.

Cleans, standardizes, and enriches Bronze rental observations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def build_silver_dataframe(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze observations into the Silver layer.

    Preserves missing rental values and adds standardized
    reporting-period and data-quality columns.

    Args:
        bronze_df: Bronze layer Spark DataFrame.

    Returns:
        Cleaned and enriched Silver DataFrame.
    """

    required_columns = {
        "statistic_code",
        "statistic_name",
        "quarter_code",
        "quarter",
        "bedroom_code",
        "bedroom_category",
        "property_type_code",
        "property_type",
        "location_code",
        "location",
        "average_monthly_rent_eur",
        "ingested_at",
    }

    missing_columns = required_columns - set(bronze_df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required Bronze columns: {sorted(missing_columns)}"
        )

    silver_df = (
        bronze_df
        .withColumn("statistic_name", F.trim("statistic_name"))
        .withColumn("quarter", F.trim("quarter"))
        .withColumn("bedroom_category", F.trim("bedroom_category"))
        .withColumn("property_type", F.trim("property_type"))
        .withColumn("location", F.trim("location"))
        .withColumn(
            "average_monthly_rent_eur",
            F.col("average_monthly_rent_eur").cast(
                DecimalType(18, 2)
            )
        )
        .withColumn(
            "year",
            F.regexp_extract("quarter_code", r"(\d{4})Q[1-4]", 1)
            .cast("integer")
        )
        .withColumn(
            "quarter_number",
            F.regexp_extract("quarter_code", r"\d{4}Q([1-4])", 1)
            .cast("integer")
        )
        .withColumn(
            "quarter_start_date",
            F.make_date(
                F.col("year"),
                ((F.col("quarter_number") - 1) * 3 + 1),
                F.lit(1)
            )
        )
        .withColumn(
            "is_missing_rent",
            F.col("average_monthly_rent_eur").isNull()
        )
        .withColumn(
            "is_valid_rent",
            F.col("average_monthly_rent_eur").isNotNull()
            & (F.col("average_monthly_rent_eur") > 0)
        )
    )

    return silver_df
