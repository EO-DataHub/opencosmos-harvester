import logging
import os
import time
import traceback

import requests
from requests.exceptions import ConnectionError, HTTPError, Timeout

from opencosmos_harvester.summary import Summary
from opencosmos_harvester.utils import load_config


def generate_access_token(env: str = "dev", retry_count: int = 0) -> str:
    """Generate access token for Open Cosmos API"""
    url = "https://login.open-cosmos.com/oauth/token"
    max_api_retries = int(os.environ.get("MAX_API_RETRIES", 5))

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
    proxy_base_url = os.environ.get("PROXY_BASE_URL", "")

    for _, asset in collection.get("assets", {}).items():
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
