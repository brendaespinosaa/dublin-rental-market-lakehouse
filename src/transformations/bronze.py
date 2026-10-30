
"""
Bronze layer transformation for the Dublin Rental Market Lakehouse.

Converts flattened JSON-stat observations into a structured
Spark DataFrame while preserving source values and metadata.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
)


def build_bronze_dataframe(
    spark: SparkSession,
    observations: list,
    dimension_ids: list,
    dimension_sizes: list,
    dimension_metadata: dict,
):
    """
    Convert flattened JSON-stat observations into a Bronze DataFrame.

    Args:
        spark: Active Spark session.
        observations: Flattened JSON-stat value array.
        dimension_ids: Ordered dimension identifiers.
        dimension_sizes: Number of categories per dimension.
        dimension_metadata: Ordered category codes and labels.

    Returns:
        Structured Bronze Spark DataFrame.
    """

    if len(dimension_ids) != len(dimension_sizes):
        raise ValueError("Dimension identifiers and sizes do not match.")

    expected_count = 1
    for size in dimension_sizes:
        expected_count *= size

    if len(observations) != expected_count:
        raise ValueError(
            f"Expected {expected_count} observations, "
            f"received {len(observations)}."
        )

    required_dimensions = {
        "STATISTIC",
        "TLIST(Q1)",
        "C02970V03592",
        "C02969V03591",
        "C03004V03625",
    }

    if not required_dimensions.issubset(set(dimension_ids)):
        raise ValueError("One or more required dimensions are missing.")

    dimension_positions = {
        dimension_id: dimension_ids.index(dimension_id)
        for dimension_id in dimension_ids
    }

    def get_stride(position):
        return expected_count // (
            dimension_sizes[position]
            * __import__("math").prod(dimension_sizes[position + 1:])
        )

    def build_lookup(dimension_id):
        position = dimension_positions[dimension_id]
        metadata = dimension_metadata[dimension_id]

        rows = [
            (index, code, metadata["labels"].get(code, code))
            for index, code in enumerate(metadata["codes"])
        ]

        return spark.createDataFrame(
            rows,
            ["category_index", "category_code", "category_label"]
        )

    bronze_schema = StructType([
        StructField("position", LongType(), False),
        StructField("average_monthly_rent_eur", DoubleType(), True),
    ])

    base_df = spark.createDataFrame(
        list(enumerate(observations)),
        bronze_schema
    )

    for dimension_id in dimension_ids:
        position = dimension_positions[dimension_id]
        stride = get_stride(position)

        base_df = base_df.withColumn(
            f"index_{position}",
            F.floor(
                F.col("position") / F.lit(stride)
            ) % F.lit(dimension_sizes[position])
        )

        lookup_df = build_lookup(dimension_id).withColumnRenamed(
            "category_index", f"index_{position}"
        )

        base_df = base_df.join(
            F.broadcast(lookup_df),
            on=f"index_{position}",
            how="left"
        ).withColumnRenamed(
            "category_code", f"code_{position}"
        ).withColumnRenamed(
            "category_label", f"label_{position}"
        )

    def code(dimension_id):
        return F.col(
            f"code_{dimension_positions[dimension_id]}"
        )

    def label(dimension_id):
        return F.col(
            f"label_{dimension_positions[dimension_id]}"
        )

    bronze_df = base_df.select(
        code("STATISTIC").alias("statistic_code"),
        label("STATISTIC").alias("statistic_name"),
        code("TLIST(Q1)").alias("quarter_code"),
        label("TLIST(Q1)").alias("quarter"),
        code("C02970V03592").alias("bedroom_code"),
        label("C02970V03592").alias("bedroom_category"),
        code("C02969V03591").alias("property_type_code"),
        label("C02969V03591").alias("property_type"),
        code("C03004V03625").alias("location_code"),
        label("C03004V03625").alias("location"),
        F.col("average_monthly_rent_eur"),
        F.current_timestamp().alias("ingested_at"),
    )

    return bronze_df
