
"""
Tests for JSON-stat data ingestion and validation.
"""

import json

import pytest

from src.ingestion.jsonstat_ingestion import (
    load_jsonstat,
    build_dimension_metadata,
    validate_jsonstat,
)


@pytest.fixture
def sample_dataset():
    """Create a small JSON-stat dataset for testing."""

    return {
        "id": ["REGION", "PERIOD"],
        "size": [2, 2],
        "dimension": {
            "REGION": {
                "category": {
                    "index": {
                        "DUBLIN": 0,
                        "CORK": 1,
                    },
                    "label": {
                        "DUBLIN": "Dublin",
                        "CORK": "Cork",
                    },
                },
            },
            "PERIOD": {
                "category": {
                    "index": {
                        "2024Q1": 0,
                        "2024Q2": 1,
                    },
                    "label": {
                        "2024Q1": "2024 Quarter 1",
                        "2024Q2": "2024 Quarter 2",
                    },
                },
            },
        },
        "value": [1200.50, 1250.75, 1100.00, None],
    }


def test_load_jsonstat(tmp_path, sample_dataset):
    """Test loading a valid JSON-stat file."""

    file_path = tmp_path / "sample.json"

    file_path.write_text(
        json.dumps(sample_dataset),
        encoding="utf-8",
    )

    dataset = load_jsonstat(str(file_path))

    assert dataset["id"] == ["REGION", "PERIOD"]
    assert len(dataset["value"]) == 4


def test_load_jsonstat_file_not_found():
    """Test handling of a missing source file."""

    with pytest.raises(FileNotFoundError):
        load_jsonstat("nonexistent_dataset.json")


def test_load_jsonstat_invalid_json(tmp_path):
    """Test handling of malformed JSON files."""

    file_path = tmp_path / "invalid.json"

    file_path.write_text(
        '{"id": ["REGION"], invalid}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_jsonstat(str(file_path))


def test_build_dimension_metadata(sample_dataset):
    """Test extraction and ordering of dimension metadata."""

    metadata = build_dimension_metadata(sample_dataset)

    assert metadata["REGION"]["codes"] == [
        "DUBLIN",
        "CORK",
    ]

    assert metadata["REGION"]["labels"]["DUBLIN"] == "Dublin"

    assert metadata["PERIOD"]["codes"] == [
        "2024Q1",
        "2024Q2",
    ]


def test_validate_jsonstat_success(sample_dataset):
    """Test validation of a structurally correct dataset."""

    metadata = build_dimension_metadata(sample_dataset)

    assert validate_jsonstat(sample_dataset, metadata) is True


def test_validate_jsonstat_preserves_missing_values(sample_dataset):
    """Test that missing observations are accepted."""

    metadata = build_dimension_metadata(sample_dataset)

    assert sample_dataset["value"][-1] is None
    assert validate_jsonstat(sample_dataset, metadata) is True


def test_validate_jsonstat_invalid_observation_count(sample_dataset):
    """Test detection of an incorrect observation count."""

    sample_dataset["value"].pop()

    metadata = build_dimension_metadata(sample_dataset)

    with pytest.raises(
        ValueError,
        match="Observation count mismatch",
    ):
        validate_jsonstat(sample_dataset, metadata)


def test_validate_jsonstat_invalid_dimension_size(sample_dataset):
    """Test detection of an incorrect dimension size."""

    sample_dataset["size"][0] = 3

    metadata = build_dimension_metadata(sample_dataset)

    with pytest.raises(
        ValueError,
        match="Dimension size mismatch",
    ):
        validate_jsonstat(sample_dataset, metadata)


def test_validate_jsonstat_missing_dimension_metadata(sample_dataset):
    """Test detection of missing dimension metadata."""

    metadata = build_dimension_metadata(sample_dataset)

    metadata.pop("REGION")

    with pytest.raises(ValueError):
        validate_jsonstat(sample_dataset, metadata)


def test_build_dimension_metadata_preserves_category_order(sample_dataset):
    """Test whether category codes follow JSON-stat index order."""

    metadata = build_dimension_metadata(sample_dataset)

    assert metadata["REGION"]["codes"] == ["DUBLIN", "CORK"]
    assert metadata["PERIOD"]["codes"] == ["2024Q1", "2024Q2"]
