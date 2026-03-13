from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator
from pystac import Extent, SpatialExtent, TemporalExtent


class Summary(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    bbox: tuple[float, float, float, float] | None = None
    start: datetime | None = None
    end: datetime | None = None

    @staticmethod
    def _min_start(a: datetime | None, b: datetime | None) -> datetime | None:
        if a is None and b is None:
            return None

        if a is None:
            return b

        if b is None:
            return a

        return min(a, b)

    @staticmethod
    def _max_end(a: datetime | None, b: datetime | None) -> datetime | None:
        if a is None and b is None:
            return None

        if a is None:
            return b

        if b is None:
            return a

        return max(a, b)

    @staticmethod
    def _bbox_from_coords(coords: list[list[float]]) -> tuple[float, float, float, float]:
        if not coords:
            raise ValueError("coordinates must not be empty")

        # Assumes 2D bounding boxes.
        xs = [x for x, y in coords]
        ys = [y for x, y in coords]
        return min(xs), min(ys), max(xs), max(ys)

    @staticmethod
    def _bbox_to_coords(bbox: tuple[float, float, float, float]) -> list[list[float]]:
        min_x, min_y, max_x, max_y = bbox
        return [
            [min_x, max_y],
            [max_x, max_y],
            [max_x, min_y],
            [min_x, min_y],
        ]

    def union(self, other: "Summary") -> "Summary":
        # Ignore any extents other than the first, which conventionally represents the entire range.
        s = self._min_start(self.start, other.start)
        e = self._max_end(self.end, other.end)

        if self.bbox is None and other.bbox is None:
            bbox = None
        elif self.bbox is None:
            bbox = other.bbox
        elif other.bbox is None:
            bbox = self.bbox
        else:
            bbox = (
                min(self.bbox[0], other.bbox[0]),
                min(self.bbox[1], other.bbox[1]),
                max(self.bbox[2], other.bbox[2]),
                max(self.bbox[3], other.bbox[3]),
            )

        return Summary(bbox=bbox, start=s, end=e)

    def __or__(self, other: "Summary") -> "Summary":
        return self.union(other)

    @model_validator(mode="before")
    @classmethod
    def deserialize_legacy_or_stac(cls, data: Any) -> Any:
        if isinstance(data, cls):
            return data

        # Legacy JSON shape
        if isinstance(data, dict) and "coordinates" in data:
            return {
                "bbox": Summary._bbox_from_coords(data["coordinates"]),
                "start": datetime.fromisoformat(data["start_time"][0]),
                "end": datetime.fromisoformat(data["stop_time"][0]),
            }

        # Already in canonical dict shape
        if isinstance(data, dict) and "bbox" in data:
            return data

        if isinstance(data, Extent):
            return {
                "bbox": data.spatial.bboxes[0],
                "start": data.temporal.intervals[0][0],
                "end": data.temporal.intervals[0][1],
            }

        return {"bbox": None, "start": None, "end": None}

    def to_legacy_json(self) -> dict[str, Any]:
        return {
            "coordinates": Summary._bbox_to_coords(self.bbox) if self.bbox else None,
            "start_time": [None] if self.start is None else [self.start.isoformat()],
            "stop_time": [None] if self.end is None else [self.end.isoformat()],
        }

    def to_stac_extent(self) -> Extent:
        if self.bbox is None:
            raise ValueError("bbox is required")

        return Extent(
            spatial=SpatialExtent([list(self.bbox)]),
            temporal=TemporalExtent([[self.start, self.end]]),
        )
