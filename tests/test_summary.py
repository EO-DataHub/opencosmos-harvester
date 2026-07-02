from datetime import datetime

import pytest
from pystac import Extent, SpatialExtent, TemporalExtent

from opencosmos_harvester.summary import Summary


def test_deserialize_legacy_shape_with_values() -> None:
    summary = Summary.model_validate(
        {
            "coordinates": [[0.0, 0.0], [1.0, 1.0]],
            "start_time": ["2024-01-01T00:00:00"],
            "stop_time": ["2024-01-02T00:00:00"],
        }
    )

    assert summary.bbox == (0.0, 0.0, 1.0, 1.0)
    assert summary.start == datetime(2024, 1, 1)
    assert summary.end == datetime(2024, 1, 2)


def test_deserialize_legacy_shape_with_null_values() -> None:
    summary = Summary.model_validate(
        {
            "coordinates": None,
            "start_time": [None],
            "stop_time": [None],
        }
    )

    assert summary.bbox is None
    assert summary.start is None
    assert summary.end is None


@pytest.mark.parametrize(
    ("key", "attr"),
    [("coordinates", "bbox"), ("start_time", "start"), ("stop_time", "end")],
)
def test_deserialize_legacy_shape_with_empty_list(key: str, attr: str) -> None:
    data = {
        "coordinates": [[0.0, 0.0], [1.0, 1.0]],
        "start_time": ["2024-01-01T00:00:00"],
        "stop_time": ["2024-01-02T00:00:00"],
    }
    data[key] = []

    summary = Summary.model_validate(data)

    assert getattr(summary, attr) is None


@pytest.mark.parametrize(("key", "attr"), [("start_time", "start"), ("stop_time", "end")])
def test_deserialize_legacy_shape_with_missing_key(key: str, attr: str) -> None:
    data = {
        "coordinates": [[0.0, 0.0], [1.0, 1.0]],
        "start_time": ["2024-01-01T00:00:00"],
        "stop_time": ["2024-01-02T00:00:00"],
    }
    del data[key]

    summary = Summary.model_validate(data)

    assert getattr(summary, attr) is None


def test_deserialize_canonical_dict_shape() -> None:
    summary = Summary.model_validate(
        {"bbox": (0.0, 0.0, 1.0, 1.0), "start": datetime(2024, 1, 1), "end": datetime(2024, 1, 2)}
    )

    assert summary.bbox == (0.0, 0.0, 1.0, 1.0)
    assert summary.start == datetime(2024, 1, 1)
    assert summary.end == datetime(2024, 1, 2)


def test_deserialize_stac_extent() -> None:
    extent = Extent(
        spatial=SpatialExtent([[0.0, 0.0, 1.0, 1.0]]),
        temporal=TemporalExtent([[datetime(2024, 1, 1), datetime(2024, 1, 2)]]),
    )

    summary = Summary.model_validate(extent)

    assert summary.bbox == (0.0, 0.0, 1.0, 1.0)
    assert summary.start == datetime(2024, 1, 1)
    assert summary.end == datetime(2024, 1, 2)


def test_deserialize_unrecognised_shape_defaults_to_empty() -> None:
    summary = Summary.model_validate({"something_else": True})

    assert summary.bbox is None
    assert summary.start is None
    assert summary.end is None


def test_legacy_json_round_trip_with_no_data() -> None:
    summary = Summary()

    round_tripped = Summary.model_validate(summary.to_legacy_json())

    assert round_tripped == summary
