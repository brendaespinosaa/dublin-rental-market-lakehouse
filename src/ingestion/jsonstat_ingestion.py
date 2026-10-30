
"""
JSON-stat source ingestion utilities.

This module loads JSON-stat datasets, extracts dimension metadata,
and validates the structure of the source data.
"""

import json
from math import prod
from pathlib import Path
from typing import Any


def load_jsonstat(file_path: str) -> dict[str, Any]:
    """
    Load a JSON-stat dataset from a local file.

    Args:
        file_path: Path to the JSON-stat source file.

    Returns:
        The loaded JSON-stat dataset.
    """

    source_path = Path(file_path)

    if not source_path.is_file():
        raise FileNotFoundError(
            f"JSON-stat source file not found: {source_path}"
        )

    try:
        with source_path.open("r", encoding="utf-8") as file:
            dataset = json.load(file)

    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(
            f"Invalid JSON-stat file: {source_path}"
        ) from error

    if not isinstance(dataset, dict):
        raise ValueError(
            "The JSON-stat dataset must be a JSON object."
        )

    return dataset


def build_dimension_metadata(
    dataset: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """
    Extract ordered category codes and labels from dataset dimensions.

    Returns:
        A dictionary containing ordered codes and category labels
        for each dimension.
    """

    dimensions = dataset.get("dimension")

    if not isinstance(dimensions, dict) or not dimensions:
        raise ValueError(
            "Dataset dimensions are missing or invalid."
        )

    dimension_metadata = {}

    for dimension_id, dimension_info in dimensions.items():

        if not isinstance(dimension_info, dict):
            raise ValueError(
                f"Invalid dimension structure: {dimension_id}."
            )

        category = dimension_info.get("category")

        if not isinstance(category, dict):
            raise ValueError(
                f"Category metadata missing in {dimension_id}."
            )

        labels = category.get("label", {})
        indexes = category.get("index")

        if not isinstance(labels, dict):
            raise ValueError(
                f"Invalid category labels in {dimension_id}."
            )

        if isinstance(indexes, dict):

            if not all(
                isinstance(position, int)
                and not isinstance(position, bool)
                for position in indexes.values()
            ):
                raise ValueError(
                    f"Invalid category indexes in {dimension_id}."
                )

            positions = list(indexes.values())

            if sorted(positions) != list(range(len(positions))):
                raise ValueError(
                    f"Category indexes must be consecutive in "
                    f"{dimension_id}."
                )

            ordered_codes = sorted(
                indexes,
                key=indexes.get,
            )

        elif isinstance(indexes, list):
            ordered_codes = indexes

        elif isinstance(labels, dict) and labels:
            ordered_codes = list(labels.keys())

        else:
            raise ValueError(
                f"Cannot determine category order for {dimension_id}."
            )

        if len(ordered_codes) != len(set(ordered_codes)):
            raise ValueError(
                f"Duplicate category codes in {dimension_id}."
            )

        if not all(isinstance(code, str) for code in ordered_codes):
            raise ValueError(
                f"Invalid category codes in {dimension_id}."
            )

        dimension_metadata[dimension_id] = {
            "codes": ordered_codes,
            "labels": labels,
        }

    return dimension_metadata


def validate_jsonstat(
    dataset: dict[str, Any],
    dimension_metadata: dict[str, dict[str, Any]],
) -> bool:
    """
    Validate JSON-stat dimensions, metadata, and observation count.

    Returns:
        True when all structural validations pass.
    """

    dimension_ids = dataset.get("id")
    dimension_sizes = dataset.get("size")
    observations = dataset.get("value")

    if not isinstance(dimension_ids, list):
        raise ValueError(
            "Dimension identifiers must be a list."
        )

    if not isinstance(dimension_sizes, list):
        raise ValueError(
            "Dimension sizes must be a list."
        )

    if not isinstance(observations, list):
        raise ValueError(
            "JSON-stat observations must be a list."
        )

    if not dimension_ids:
        raise ValueError(
            "Dataset must contain at least one dimension."
        )

    if len(dimension_ids) != len(dimension_sizes):
        raise ValueError(
            "Dimension identifiers and sizes do not match."
        )

    if len(dimension_ids) != len(set(dimension_ids)):
        raise ValueError(
            "Duplicate dimension identifiers found."
        )

    if not all(
        isinstance(size, int)
        and not isinstance(size, bool)
        and size > 0
        for size in dimension_sizes
    ):
        raise ValueError(
            "Dimension sizes must be positive integers."
        )

    if set(dimension_ids) != set(dimension_metadata):
        raise ValueError(
            "Dataset dimensions do not match extracted metadata."
        )

    for dimension_id, expected_size in zip(
        dimension_ids,
        dimension_sizes,
    ):
        metadata = dimension_metadata[dimension_id]

        if not isinstance(metadata, dict):
            raise ValueError(
                f"Invalid metadata structure for {dimension_id}."
            )

        codes = metadata.get("codes")
        labels = metadata.get("labels")

        if not isinstance(codes, list):
            raise ValueError(
                f"Invalid category codes in {dimension_id}."
            )

        if not isinstance(labels, dict):
            raise ValueError(
                f"Invalid category labels in {dimension_id}."
            )

        if len(codes) != expected_size:
            raise ValueError(
                f"Dimension size mismatch in {dimension_id}: "
                f"expected {expected_size}, found {len(codes)}."
            )

        if len(codes) != len(set(codes)):
            raise ValueError(
                f"Duplicate category codes in {dimension_id}."
            )

    expected_observations = prod(dimension_sizes)

    if len(observations) != expected_observations:
        raise ValueError(
            "Observation count mismatch: "
            f"expected {expected_observations}, "
            f"found {len(observations)}."
        )

    return True
