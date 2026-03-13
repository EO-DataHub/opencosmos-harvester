from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import traceback
import uuid
from datetime import datetime
from itertools import batched
from pathlib import Path
from typing import Any

import click
import pystac
import requests
from botocore import client
from botocore.exceptions import ClientError
from eodhp_utils.aws.s3 import get_file_s3, upload_file_s3
from eodhp_utils.runner import get_boto3_session, setup_logging
from pulsar import Client as PulsarClient
from pulsar import ConnectError
from pystac_client import Client
from requests.exceptions import ConnectionError, HTTPError, Timeout

from opencosmos_harvester.metadata import Metadata
from opencosmos_harvester.opencosmos_harvester_messager import JSONCustomEncoder, OpenCosmosHarvesterMessager
from opencosmos_harvester.summary import Summary

setup_logging(verbosity=2)  # DEBUG level


minimum_message_entries = int(os.environ.get("MINIMUM_MESSAGE_ENTRIES", 100))
proxy_base_url = os.environ.get("PROXY_BASE_URL", "")
max_api_retries = int(os.environ.get("MAX_API_RETRIES", 5))

commercial_catalogue_root = os.getenv("COMMERCIAL_CATALOGUE_ROOT", "catalogs/commercial")
thumbnail_bucket = os.getenv("THUMBNAIL_BUCKET", "eodhp-thumbnails")


def load_config(config_path: str) -> Any:
    with open(config_path) as f:
        return json.load(f)


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

    # Load application config.
    config_key = os.getenv("HARVESTER_CONFIG_KEY", "")
    config = load_config("opencosmos_harvester/config.json").get(config_key.upper())

    if not config:
        logging.error(f"Configuration key {config_key} not found in config file.")
        return

    logging.info(f"Harvesting from Open Cosmos {config_key}")

    # Initialise S3.
    logging.info("Initialising S3 client")
    s3_client = get_boto3_session().client("s3")
    s3_root = "git-harvester/"
    key_root = f"{commercial_catalogue_root}/catalogs/opencosmos"

    # Initialise Pulsar.
    logging.info("Initialising Pulsar client")
    topic = os.getenv("TOPIC")
    identifier = f"_{topic}" if topic else ""
    producer = get_pulsar_producer(identifier, config)
    opencosmos_harvester_messager = OpenCosmosHarvesterMessager(
        s3_client=s3_client,
        output_bucket=s3_bucket,
        cat_output_prefix=s3_root,
        producer=producer,
    )

    logging.info("Generating access token")
    open_cosmos_api_token = generate_access_token()

    # Create a requests session for managing thumbnail downloading.
    session = requests.session()
    session.headers.update({"Authorization": f"Bearer {open_cosmos_api_token}"})

    # Load the existing metadata, if any.
    logging.info("Loading existing metadata")
    metadata_s3_key = f"harvested-metadata/{config['collection_name']}"
    previous_harvest_metadata = Metadata.model_validate(get_harvester_metadata(s3_bucket, metadata_s3_key, s3_client))
    current_harvest_metadata = Metadata()
    current_harvest_items = {}

    thumbnail_urls = {}
    thumbnail_keys = {}

    # Search the catalogue.
    logging.info("Searching catalogue")
    stac_client = Client.open(config["url"], headers={"Authorization": f"Bearer {open_cosmos_api_token}"})
    search = stac_client.search(max_items=8, limit=config["limit"], query=config["query"])

    for item in search.items():
        logging.info(f"Processing item: {item.id}")
        file_name = f"{item.id}.json"
        item_key = f"{key_root}/collections/{config['collection_name']}/items/{file_name}"

        # Modify the Item's thumbnail href to point to a public bucket.
        thumbnail_file_name = Path(item_key).with_suffix(".webp").name
        thumbnail_key = f"opencosmos/{config['collection_name']}/thumb_{thumbnail_file_name}"
        thumbnail_urls[item_key] = item.assets["thumbnail"].href
        thumbnail_keys[item_key] = thumbnail_key
        item.assets["thumbnail"].href = f"https://eodhp-thumbnails.s3.eu-west-2.amazonaws.com/{thumbnail_key}"

        item_hash = get_file_hash(json.dumps(item.to_dict(), cls=JSONCustomEncoder))
        item_start, item_end = get_item_temporal_extents(item)
        item_bbox = flatten_item_spatial_extents(item)
        summary = Summary(bbox=item_bbox, start=item_start, end=item_end)
        current_harvest_metadata.add_item(summary, item_key, item_hash)
        current_harvest_items[item_key] = item.to_dict()

    logging.info(f"Found {len(current_harvest_items)} items")

    logging.info("Creating catalogue and collection objects")
    catalogue_data = make_catalogue()
    catalogue_key = f"{key_root}.json"
    catalogue_hash = get_file_hash(json.dumps(catalogue_data, cls=JSONCustomEncoder))
    current_harvest_metadata.files[catalogue_key] = catalogue_hash
    current_harvest_items[catalogue_key] = catalogue_data

    combined_summary = previous_harvest_metadata.summary | current_harvest_metadata.summary
    collection_data = make_collection(combined_summary, config)
    collection_key = f"{key_root}/collections/{config['collection_name']}.json"
    collection_hash = get_file_hash(json.dumps(collection_data, cls=JSONCustomEncoder))
    current_harvest_metadata.files[collection_key] = collection_hash
    current_harvest_items[collection_key] = collection_data

    to_delete, to_add, to_update = previous_harvest_metadata.build_change_list(current_harvest_metadata)

    # The harvester doesn't distinguish between updated and added items.
    to_upsert = to_add + to_update

    logging.info(f"Upserting {len(to_upsert)} STAC Items to S3 ({len(to_add)} added, {len(to_update)} updated)")
    num_chunks = len(to_upsert) // minimum_message_entries + 1
    current_chunk = 1

    for upsert_chunk in batched(to_upsert, minimum_message_entries, strict=False):
        logging.info(f"Sending message chunk {current_chunk}/{num_chunks}")
        chunk_data = {key: current_harvest_items[key] for key in upsert_chunk}
        msg = {"harvested_data": chunk_data, "deleted_keys": []}
        opencosmos_harvester_messager.consume(msg)
        current_chunk += 1

    thumbnail_urls = {k: v for k, v in thumbnail_urls.items() if k in to_upsert}
    logging.info(f"Uploading {len(thumbnail_urls)} thumbnails")
    for item_key in to_upsert:
        if item_key not in thumbnail_urls:
            continue

        with session.get(thumbnail_urls[item_key], stream=True) as r:
            if r.status_code == 200:
                s3_client.put_object(Bucket=thumbnail_bucket, Key=thumbnail_keys[item_key], Body=r.content)

    if len(to_delete) > 0:
        logging.info(f"Deleting {len(to_delete)} STAC Items from S3")
        num_chunks = len(to_delete) // minimum_message_entries + 1
        current_chunk = 1

        for deleted_chunk in batched(to_delete, minimum_message_entries, strict=False):
            logging.info(f"Sending message chunk {current_chunk}/{num_chunks}")
            msg = {"harvested_data": [], "deleted_keys": list(deleted_chunk)}
            opencosmos_harvester_messager.consume(msg)
            current_chunk += 1

    logging.info("Uploading metadata to S3")
    upload_file_s3(
        json.dumps(current_harvest_metadata.to_legacy_json(), cls=JSONCustomEncoder),
        s3_bucket,
        metadata_s3_key,
        s3_client,
    )

    logging.info("Finished")


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


def make_collection(summary: Summary, config: dict) -> dict:
    collection = load_config(f"opencosmos_harvester/{config['collection_name']}.json")

    for _, asset in collection.get("assets").items():
        if "href" in asset:
            asset["href"] = asset["href"].replace("{EODHP_BASE_URL}", proxy_base_url)

    collection["extent"] = {
        "spatial": {"bbox": [summary.bbox]},
        "temporal": {"interval": [[summary.start, summary.end]]},
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
