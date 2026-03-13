from typing import Any

from pydantic import BaseModel, Field, model_validator

from opencosmos_harvester.summary import Summary


class Metadata(BaseModel):
    summary: Summary = Summary()
    files: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def deserialize(cls, data: dict) -> dict[str, Any]:
        if "summary" in data:
            summary = Summary.model_validate(data.pop("summary"))
        else:
            summary = Summary(bbox=None, start=None, end=None)

        files = data

        return {"summary": summary, "files": files}

    def to_legacy_json(self) -> dict:
        f = self.files.copy()
        f["summary"] = self.summary.to_legacy_json()
        return f

    def build_change_list(self, other: "Metadata") -> tuple[list, list, list]:
        left_set = set(self.files.keys())
        right_set = set(other.files.keys())

        to_delete = list(left_set.difference(right_set))
        to_add = list(right_set.difference(left_set))
        to_update = [x for x in left_set.intersection(right_set) if self.files[x] != other.files[x]]

        return to_delete, to_add, to_update

    def add_item(self, summary: Summary, file_name: str, file_hash: str) -> None:
        self.summary = self.summary | summary
        self.files[file_name] = file_hash
