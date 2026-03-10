from __future__ import annotations

import datetime
import json
from collections.abc import Sequence
from typing import Any, cast

from eodhp_utils.messagers import Messager


class JSONCustomEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()

        return super().default(o)


class OpenCosmosHarvesterMessager(Messager[dict]):
    """
    Loads STAC files harvested from the Open Cosmos API into an S3 bucket with file key relating to the
    owning catalog combined with the file path in the external catalogue.
    For example: git-harvester/supported-datasets/opencosmos/collection/item
    Then sends a catalogue harvested message via Pulsar to trigger transformer and ingester.
    """

    def process_msg(self, msg: dict) -> Sequence[Messager.Action]:
        action_list = []
        harvested_data = msg["harvested_data"]
        deleted_keys = msg["deleted_keys"]
        for key, value in harvested_data.items():
            # Retrieve data
            stac_data = value
            # return action to save file to S3
            # bucket defaults to self.output_bucket
            action = Messager.OutputFileAction(
                file_body=json.dumps(stac_data, cls=JSONCustomEncoder),
                cat_path=key,
            )
            action_list.append(action)

        for key in deleted_keys:
            # return action to delete file from S3 (file_body=None signals deletion per upstream docs)
            action = Messager.OutputFileAction(file_body=cast(str, None), cat_path=key)
            action_list.append(action)

        return action_list

    def gen_empty_catalogue_message(self, msg: Any) -> dict:
        return {
            "id": "harvester/opencosmos",
            "workspace": "default_workspace",
            "repository": "",
            "branch": "",
            "bucket_name": self.output_bucket,
            "source": "",
            "target": "",
        }
