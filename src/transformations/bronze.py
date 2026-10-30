
"""
Bronze layer transformation for the Dublin Rental Market Lakehouse.

Converts flattened JSON-stat observations into a structured
Spark DataFrame while preserving source values and metadata.
"""

from math import prod

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    LongType,
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
        raise ValueError(
            "Dimension identifiers and sizes do not match."
        )

    if len(dimension_ids) != len(set(dimension_ids)):
        raise ValueError("Duplicate dimension identifiers found.")

    if any(size <= 0 for size in dimension_sizes):
        raise ValueError("Dimension sizes must be positive.")

    expected_count = prod(dimension_sizes)

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
        missing_dimensions = required_dimensions - set(dimension_ids)

        raise ValueError(
            f"Required dimensions are missing: {sorted(missing_dimensions)}"
        )

    for dimension_id, dimension_size in zip(
        dimension_ids, dimension_sizes
    ):
        if dimension_id not in dimension_metadata:
            raise ValueError(
                f"Metadata not found for dimension: {dimension_id}"
            )

        codes = dimension_metadata[dimension_id].get("codes", [])
        labels = dimension_metadata[dimension_id].get("labels", {})

        if len(codes) != dimension_size:
            raise ValueError(
                f"Category count mismatch in {dimension_id}: "
                f"expected {dimension_size}, found {len(codes)}."
            )

        if len(codes) != len(set(codes)):
            raise ValueError(
                f"Duplicate category codes in {dimension_id}."
            )

        if not isinstance(labels, dict):
            raise ValueError(
                f"Invalid category labels in {dimension_id}."
            )

    dimension_positions = {
        dimension_id: position
        for position, dimension_id in enumerate(dimension_ids)
    }

    def get_stride(position: int) -> int:
        """
        Calculate the flattened JSON-stat stride.

        The last dimension changes fastest in the flattened array.
        """
        return prod(dimension_sizes[position + 1:])

    def build_lookup(dimension_id: str):
        """Build a Spark lookup DataFrame for a dimension."""

        metadata = dimension_metadata[dimension_id]

        rows = [
            (
                index,
                code,
                metadata["labels"].get(code, code),
            )
            for index, code in enumerate(metadata["codes"])
        ]

        return spark.createDataFrame(
            rows,
            [
                "category_index",
                "category_code",
                "category_label",
            ],
        )

    bronze_schema = StructType([
        StructField("position", LongType(), nullable=False),
        StructField(
            "average_monthly_rent_eur",
            DoubleType(),
            nullable=True,
        ),
    ])

    base_df = spark.createDataFrame(
        list(enumerate(observations)),
        schema=bronze_schema,
    )

    for dimension_id in dimension_ids:
        position = dimension_positions[dimension_id]
        stride = get_stride(position)

        index_column = f"index_{position}"

        base_df = base_df.withColumn(
            index_column,
            (
                F.floor(
                    F.col("position") / F.lit(stride)
                )
                % F.lit(dimension_sizes[position])
            ).cast("integer"),
        )

        lookup_df = build_lookup(dimension_id).withColumnRenamed(
            "category_index",
            index_column,
        )

        base_df = (
            base_df
            .join(
                F.broadcast(lookup_df),
                on=index_column,
                how="left",
            )
            .withColumnRenamed(
                "category_code",
                f"code_{position}",
            )
            .withColumnRenamed(
                "category_label",
                f"label_{position}",
            )
        )

    def code(dimension_id: str):
        position = dimension_positions[dimension_id]
        return F.col(f"code_{position}")

    def label(dimension_id: str):
        position = dimension_positions[dimension_id]
        return F.col(f"label_{position}")

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
