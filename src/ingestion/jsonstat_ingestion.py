

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
    except json.JSONDecodeError as error:
        raise ValueError(
            f"Invalid JSON-stat file: {source_path}"
        ) from error

    if not isinstance(dataset, dict):
        raise ValueError("The JSON-stat dataset must be a JSON object.")

    return dataset


def build_dimension_metadata(
    dataset: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    """
    Extract ordered category codes and labels from dataset dimensions.

    Returns:
        A dictionary containing ordered codes and category labels
        for each dimension.
    """
    dimensions = dataset.get("dimension", {})

    if not isinstance(dimensions, dict) or not dimensions:
        raise ValueError("Dataset dimensions are missing or invalid.")

    dimension_metadata = {}

    for dimension_id, dimension_info in dimensions.items():
        category = dimension_info.get("category", {})
        labels = category.get("label", {})
        indexes = category.get("index")

        if isinstance(indexes, dict):
            ordered_codes = sorted(
                indexes,
                key=lambda code: indexes[code]
            )
        elif isinstance(indexes, list):
            ordered_codes = indexes
        elif isinstance(labels, dict):
            ordered_codes = list(labels.keys())
        else:
            raise ValueError(
                f"Cannot determine category order for {dimension_id}."
            )

        if len(ordered_codes) != len(set(ordered_codes)):
            raise ValueError(
                f"Duplicate category codes in {dimension_id}."
            )

        dimension_metadata[dimension_id] = {
            "codes": ordered_codes,
            "labels": labels
        }

    return dimension_metadata


def validate_jsonstat(
    dataset: dict[str, Any],
    dimension_metadata: dict[str, dict[str, Any]]
) -> bool:
    """
    Validate dimension sizes and observation count.

    Returns:
        True when all structural validations pass.
    """
    dimension_ids = dataset.get("id", [])
    dimension_sizes = dataset.get("size", [])
    observations = dataset.get("value", [])

    if len(dimension_ids) != len(dimension_sizes):
        raise ValueError(
            "Dimension identifiers and sizes do not match."
        )

    if set(dimension_ids) != set(dimension_metadata):
        raise ValueError(
            "Dataset dimensions do not match extracted metadata."
        )

    for dimension_id, expected_size in zip(
        dimension_ids, dimension_sizes
    ):
        actual_size = len(
            dimension_metadata[dimension_id]["codes"]
        )

        if actual_size != expected_size:
            raise ValueError(
                f"Dimension size mismatch in {dimension_id}: "
                f"expected {expected_size}, found {actual_size}."
            )

    expected_observations = prod(dimension_sizes)

    if len(observations) != expected_observations:
        raise ValueError(
            "Observation count mismatch: "
            f"expected {expected_observations}, "
            f"found {len(observations)}."
        )

    return True
