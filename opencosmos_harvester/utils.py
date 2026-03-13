import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Any

import pystac
from botocore import client
from botocore.exceptions import ClientError
from eodhp_utils.aws.s3 import get_file_s3
from pulsar import Client as PulsarClient
from pulsar import ConnectError


def load_config(config_path: str) -> Any:
    with open(config_path) as f:
        return json.load(f)


def get_file_hash(data: str) -> str:
    """Returns hash of data available"""

    def _md5_hash(byte_str: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(byte_str)
        return md5.hexdigest()

    return _md5_hash(data.encode("utf-8"))


def flatten_item_spatial_extents(item: pystac.Item) -> tuple[float, float, float, float]:
    """Flatten an item's boudning box to 2D"""
    bb = item.bbox
    bb2d = (-180.0, -90.0, 180.0, 90.0)
    if bb:
        if len(bb) == 4:
            bb2d = (bb[0], bb[1], bb[2], bb[3])
        elif len(bb) == 6:
            bb2d = (bb[0], bb[1], bb[3], bb[4])

    return bb2d


def get_item_temporal_extents(item: pystac.Item) -> tuple[datetime | None, datetime | None]:
    if item.datetime is None:
        start_time = item.common_metadata.start_datetime
        stop_time = item.common_metadata.end_datetime
    else:
        start_time = item.datetime
        stop_time = item.datetime

    return start_time, stop_time


def get_harvester_metadata(bucket: str, key: str, s3_client: client.BaseClient) -> dict:
    """Read file at given S3 location and parse as JSON"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return {}

    return json.loads(get_file_s3(bucket, key, s3_client))


def get_pulsar_producer(identifier: str, config: dict, retry_count: int = 0) -> Any:
    """Initialise pulsar producer. Retry if connection fails"""
    try:
        pulsar_url = os.environ.get("PULSAR_URL")
        pulsar_client = PulsarClient(pulsar_url, logger=logging.getLogger(__name__))
        _producer = pulsar_client.create_producer(
            topic=f"harvested{identifier}",
            producer_name=f"stac_harvester/opencosmos/{config['collection_name']}_{uuid.uuid1().hex}",
            chunking_enabled=True,
        )
        return _producer
    except ConnectError as e:
        logging.error(f"Failed to connect to pulsar: {e}")
        if retry_count >= 10:
            logging.error(f"Failed to connect to pulsar after {retry_count + 1} attempts.")
            raise

        logging.error(f"Retrying pulsar initialisation. Attempt {retry_count + 1}")
        time.sleep(2**retry_count)
        return get_pulsar_producer(identifier, config, retry_count=retry_count + 1)
