from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import time
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import click
import pystac
import requests
from eodhp_utils.aws.s3 import get_file_s3, upload_file_s3
from eodhp_utils.runner import get_boto3_session, get_pulsar_client, setup_logging
from pulsar import ConnectError
from pystac_client import Client
from requests.exceptions import ConnectionError, HTTPError, Timeout

from opencosmos_harvester.opencosmos_harvester_messager import JSONCustomEncoder, OpenCosmosHarvesterMessager

setup_logging(verbosity=2)  # DEBUG level


minimum_message_entries = int(os.environ.get("MINIMUM_MESSAGE_ENTRIES", 100))
proxy_base_url = os.environ.get("PROXY_BASE_URL", "")
max_api_retries = int(os.environ.get("MAX_API_RETRIES", 5))

commercial_catalogue_root = os.getenv("COMMERCIAL_CATALOGUE_ROOT", "commercial")

type BBox = tuple[float, float, float, float]


@dataclass()
class Extents:
    start_datetime: datetime | None
    end_datetime: datetime | None
    bbox: BBox

    def union(self, other: Extents) -> Extents:
        if self.start_datetime is None and other.start_datetime is None:
            sd = None
        elif self.start_datetime is None:
            sd = other.start_datetime
        elif other.start_datetime is None:
            sd = self.start_datetime
        else:
            sd = min(self.start_datetime, other.start_datetime)

        if self.end_datetime is None and other.end_datetime is None:
            ed = None
        elif self.end_datetime is None:
            ed = other.end_datetime
        elif other.end_datetime is None:
            ed = self.end_datetime
        else:
            ed = min(self.end_datetime, other.end_datetime)

        return Extents(
            start_datetime=sd,
            end_datetime=ed,
            bbox=calculate_maximum_extent([self.bbox, other.bbox]),
        )


def load_config(config_path: str) -> Any:
    with open(config_path) as f:
        return json.load(f)


def get_pulsar_producer(identifier: str, config: dict, retry_count: int = 0) -> Any:
    """Initialise pulsar producer. Retry if connection fails"""
    try:
        pulsar_client = get_pulsar_client()
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


@click.group()
# you can implement any global flags here that will apply to all commands, e.g. debug
# @click.option('--debug/--no-debug', default=False) # don't feel the need to implement this, just an example
def cli() -> None:
    """This is just a placeholder to act as the entrypoint, you can do things with global options here
    if required"""


@cli.command()
# not currently used but keeping the same structure as the other harvester repos
# @click.argument("source_url", type=str, nargs=1)
@click.argument("workspace_name", type=str)
@click.argument("catalog", type=str)  # not currently used but keeping the same structure as the other harvester repos
@click.argument("s3_bucket", type=str)
def harvest(workspace_name: str, catalog: str, s3_bucket: str) -> None:
    """Harvest a given Open Cosmos catalog, and all records beneath it. Send a pulsar message
    containing all added, updated, and deleted links since the last time the catalog was
    harvested"""

    config_key = os.getenv("HARVESTER_CONFIG_KEY", "")
    config = load_config("opencosmos_harvester/config.json").get(config_key.upper())

    if not config:
        logging.error(f"Configuration key {config_key} not found in config file.")
        return

    s3_client = get_boto3_session().client("s3")
    s3_root = "git-harvester/"

    topic = os.getenv("TOPIC")
    identifier = f"_{topic}" if topic else ""
    producer = get_pulsar_producer(identifier, config)
    opencosmos_harvester_messager = OpenCosmosHarvesterMessager(
        s3_client=s3_client,
        output_bucket=s3_bucket,
        cat_output_prefix=s3_root,
        producer=producer,
    )

    harvested_data = {}
    current_harvest_keys = set()

    logging.info(f"Harvesting from Open Cosmos {config_key}")

    key_root = f"{commercial_catalogue_root}/catalogs/opencosmos"

    metadata_s3_key = f"harvested-metadata/{config['collection_name']}"
    current_harvest_metadata = get_file_data(s3_bucket, metadata_s3_key, s3_client)
    previous_harvest_metadata = copy.deepcopy(current_harvest_metadata)

    if current_harvest_metadata:
        logging.info(f"Previously harvested URLs: {len(current_harvest_metadata) - 3}")

    latest_harvested = {}

    current_extents = None

    catalogue_data = make_catalogue()
    catalogue_key = f"{key_root}.json"
    previous_hash = current_harvest_metadata.get(catalogue_key)
    current_harvest_keys.add(catalogue_key)
    file_hash = get_file_hash(json.dumps(catalogue_data, cls=JSONCustomEncoder))

    if not previous_hash or previous_hash != file_hash:
        # URL was not harvested previously
        logging.info(f"Added: {catalogue_key}")
        harvested_data[catalogue_key] = catalogue_data
        latest_harvested[catalogue_key] = file_hash

    collection_key = f"{key_root}/collections/{config['collection_name']}.json"
    current_harvest_keys.add(collection_key)

    old_collection_data = get_file_data(s3_bucket, f"{s3_root}{collection_key}", s3_client)
    old_collection = pystac.Collection.from_dict(old_collection_data) if old_collection_data else None
    logging.info(f"Found previous collection data in {s3_bucket}: {collection_key}, {old_collection_data}")

    if old_collection:
        bb = old_collection.extent.spatial.bboxes[0]
        bb2d = (-180.0, -90.0, 180.0, 90.0)
        if bb:
            if len(bb) == 4:
                bb2d = (bb[0], bb[1], bb[2], bb[3])
            elif len(bb) == 6:
                bb2d = (bb[0], bb[1], bb[3], bb[4])

        current_extents = Extents(
            old_collection.extent.temporal.intervals[0][0],
            old_collection.extent.temporal.intervals[0][1],
            bb2d,
        )

        logging.info(f"Previous harvest data recovered: {current_extents}")

    catalogue_data_summary = current_harvest_metadata.get("summary")
    if catalogue_data_summary and not current_extents:
        current_extents = summary_to_extents(catalogue_data_summary)

    first = True
    token = generate_access_token()
    client = Client.open(config["url"], headers={"Authorization": f"Bearer {token}"})
    search = client.search(max_items=6, limit=config["limit"], query=config["query"])

    # Prepare the collection template.
    stac_template = load_config(f"opencosmos_harvester/{config['collection_name']}.json")
    for _, asset in stac_template.get("assets").items():
        if "href" in asset:
            asset["href"] = asset["href"].replace("{EODHP_BASE_URL}", proxy_base_url)

    for item in search.items():
        file_name = f"{item.id}.json"
        key = f"{key_root}/collections/{config['collection_name']}/items/{file_name}"
        current_harvest_keys.add(key)

        previous_hash = current_harvest_metadata.get(key)

        item_hash = get_file_hash(json.dumps(item.to_dict(), cls=JSONCustomEncoder))
        if not previous_hash or previous_hash != item_hash:
            # Data was not harvested previously
            logging.info(f"Added: {key}")
            harvested_data[key] = item.to_dict()
            latest_harvested[key] = item_hash
        else:
            logging.info(f"Skipping: {key}")

        if not current_extents:
            current_extents = item_to_extents(item)
        else:
            current_extents = current_extents.union(item_to_extents(item))

        # Collection updates every loop so that start/stop times and bbox values are the latest
        # ones from the Open Cosmos catalogue
        collection_data = generate_stac_collection(current_extents, stac_template)
        last_run_hash = latest_harvested.get(collection_key)
        previous_hash = last_run_hash or current_harvest_metadata.get(collection_key)

        file_hash = get_file_hash(json.dumps(collection_data, cls=JSONCustomEncoder))
        # Make sure collection level is sent during first message. Only send changes after that
        if first or (not previous_hash or previous_hash != file_hash):
            first = False
            # Data was not harvested previously
            logging.info(f"Added: {collection_key}")
            harvested_data[collection_key] = collection_data
            latest_harvested[collection_key] = file_hash

        latest_harvested["summary"] = extents_to_summary(current_extents)

        if len(harvested_data.keys()) >= minimum_message_entries:
            # Send message for altered keys
            msg = {
                "harvested_data": harvested_data,
                "deleted_keys": [],
            }

            for key, value in latest_harvested.items():
                current_harvest_metadata[key] = value

            logging.info(f"Sending message with {len(harvested_data.keys())} entries")
            opencosmos_harvester_messager.consume(msg)
            logging.info(f"Uploading metadata to S3: {len(current_harvest_metadata)} items")
            upload_file_s3(
                json.dumps(current_harvest_metadata, cls=JSONCustomEncoder), s3_bucket, metadata_s3_key, s3_client
            )
            logging.info("Uploaded metadata to S3")
            harvested_data = {}
            latest_harvested = {}

    # Make sure new collection is sent in final message
    collection_data = generate_stac_collection(current_extents, stac_template)
    file_hash = get_file_hash(json.dumps(collection_data, cls=JSONCustomEncoder))

    logging.info(f"Added: {collection_key}")
    harvested_data[collection_key] = collection_data
    latest_harvested[collection_key] = file_hash

    # Any leftover items not sent during final loop because the minimum wasn't met
    logging.info(f"Adding final keys: {len(current_harvest_metadata)} items")
    for key, value in latest_harvested.items():
        current_harvest_metadata[key] = value
        current_harvest_keys.add(key)

    # Compare items harvested this run to the ones harvested in the previous run to find deletions
    deleted_keys = find_deleted_keys(current_harvest_keys, previous_harvest_metadata)

    logging.info(f"Removing {len(deleted_keys)} deleted keys: {len(current_harvest_metadata)} items")
    for key in deleted_keys:
        del current_harvest_metadata[key]
    logging.info(f"Deleted keys removed: {len(current_harvest_metadata)} items")

    # Send message for altered keys
    msg = {"harvested_data": harvested_data, "deleted_keys": deleted_keys}

    logging.info(f"Sending message with {len(harvested_data.keys())} entries and {len(deleted_keys)} deleted keys")
    opencosmos_harvester_messager.consume(msg)

    logging.info(f"Uploading metadata to S3: {len(current_harvest_metadata)} items")
    upload_file_s3(json.dumps(current_harvest_metadata, cls=JSONCustomEncoder), s3_bucket, metadata_s3_key, s3_client)
    logging.info("Uploaded metadata to S3")


def find_deleted_keys(new: set, old: dict) -> list:
    """Find differences between two dictionaries"""
    return list(set(old).difference(new))


def item_to_extents(item: pystac.Item) -> Extents:
    if item.datetime is None:
        start_time = item.common_metadata.start_datetime
        stop_time = item.common_metadata.end_datetime
    else:
        start_time = item.datetime
        stop_time = item.datetime

    bb = item.bbox
    bb2d = (-180.0, -90.0, 180.0, 90.0)
    if bb:
        if len(bb) == 4:
            bb2d = (bb[0], bb[1], bb[2], bb[3])
        elif len(bb) == 6:
            bb2d = (bb[0], bb[1], bb[3], bb[4])

    return Extents(start_time, stop_time, bb2d)


def calculate_maximum_extent(bboxes: list[BBox]) -> BBox:
    minx = max(-180.0, min(b[0] for b in bboxes))
    miny = max(-90.0, min(b[1] for b in bboxes))
    maxx = min(180.0, max(b[2] for b in bboxes))
    maxy = min(90.0, max(b[3] for b in bboxes))

    return minx, miny, maxx, maxy


def generate_access_token(env: str = "dev", retry_count: int = 0) -> str:
    """Generate access token for Open Cosmos API"""
    url = "https://login.open-cosmos.com/oauth/token"

    client_id = os.environ["OPENCOSMOS_CLIENT_ID"]
    client_secret = os.environ["OPENCOSMOS_CLIENT_SECRET"]

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = [
        ("client_secret", client_secret),
        ("grant_type", "client_credentials"),
        ("audience", "https://beeapp.open-cosmos.com"),
        ("client_id", client_id),
    ]

    try:
        logging.info(f"Making POST request to {url} for access token")
        response = requests.post(url, headers=headers, data=data, timeout=10)
        logging.info(f"Response status code: {response.status_code}")
        response.raise_for_status()
        access_token = response.json().get("access_token")

        if access_token:
            return access_token
        else:
            raise ValueError("Access token is None")

    except (ConnectionError, HTTPError, Timeout, ValueError) as e:
        logging.error(e)
        logging.error(traceback.format_exc())
        if retry_count >= max_api_retries:
            logging.error(f"Failed to generate access token after {retry_count + 1} attempts.")
            raise

        logging.error(f"Retrying access token generation. Attempt {retry_count + 1}")
        time.sleep(2**retry_count)
        return generate_access_token(env, retry_count=retry_count + 1)


def get_file_hash(data: str) -> str:
    """Returns hash of data available"""

    def _md5_hash(byte_str: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(byte_str)
        return md5.hexdigest()

    return _md5_hash(data.encode("utf-8"))


def get_file_data(bucket: str, key: str, s3_client: Any) -> dict:
    """Read file at given S3 location and parse as JSON"""
    previously_harvested = get_file_s3(bucket, key, s3_client)
    try:
        previously_harvested = json.loads(previously_harvested)
    except TypeError:
        previously_harvested = {}
    return previously_harvested


def bbox_to_coordinates(bbox: BBox) -> list:
    # This function exists to maintain compatibility with other harvesters.
    """Converts bbox to coordinates in clockwise order."""
    return [[bbox[0], bbox[1]], [bbox[0], bbox[3]], [bbox[2], bbox[1]], [bbox[2], bbox[3]]]


def summary_to_extents(summary: dict) -> Extents:
    # This function exists to maintain compatibility with other harvesters.
    # Coordinates were calculated incorrectly with other harvesters, and we just want a bbox.
    coords = summary["coordinates"]
    minx = min(c[0] for c in coords)
    miny = min(c[1] for c in coords)
    maxx = max(c[0] for c in coords)
    maxy = max(c[1] for c in coords)
    current_extents = Extents(
        summary["start_time"][0],
        summary["stop_time"][0],
        (minx, miny, maxx, maxy),
    )
    return current_extents


def extents_to_summary(extents: Extents) -> dict:
    # This function exists to maintain compatibility with other harvesters.
    coords = bbox_to_coordinates(extents.bbox)

    return {
        "coordinates": coords,
        "start_time": [extents.start_datetime.isoformat() if extents.start_datetime else None],
        "stop_time": [extents.end_datetime.isoformat() if extents.end_datetime else None],
    }


def generate_stac_collection(total_extents: Extents | None, stac_template: dict) -> dict:
    """Top level collection for Open Cosmos data"""
    collection = stac_template.copy()

    if total_extents:
        collection["extent"] = {
            "spatial": {"bbox": [total_extents.bbox]},
            "temporal": {"interval": [[total_extents.start_datetime, total_extents.end_datetime]]},
        }

    return collection


def make_catalogue() -> dict:
    """Top level catalogue for Open Cosmos data"""
    stac_catalog = {
        "type": "Catalog",
        "id": "opencosmos",
        "stac_version": "1.0.0",
        "description": "Open Cosmos Datasets",
        "links": [],
    }
    return stac_catalog


if __name__ == "__main__":
    cli(obj={})
