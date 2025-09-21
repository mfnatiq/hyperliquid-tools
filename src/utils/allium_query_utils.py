import logging
import os
import time
from dotenv import load_dotenv
import requests
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# region allium api details
ALLIUM_BASE_URL = "https://api.allium.so/api/v1/explorer"
ALLIUM_API_KEY = os.getenv("ALLIUM_API_KEY")
if not ALLIUM_API_KEY:
    logger.error("ALLIUM_API_KEY environment variable not set")
    exit(1)
# endregion

def query_allium(params: dict, run_config: dict, query_id: str) -> list:
    headers = { "X-API-Key": ALLIUM_API_KEY }
    all_rows = []
    leaderboard_addresses_query_id = os.getenv("ALLIUM_LEADERBOARD_QUERY_ADDRESSES_ID")
    if not leaderboard_addresses_query_id:
        logger.error("ALLIUM_LEADERBOARD_QUERY_ADDRESSES_ID environment variable not set")
        return all_rows

    query_run_id = None
    status = "queued"
    poll_interval = 2

    try:
        # step 1: trigger async allium query
        logger.info(f"querying with params {params} (query id {query_id})")
        response = requests.post(
            f"{ALLIUM_BASE_URL}/queries/{query_id}/run-async",
            json={ "parameters": params, 'run_config': run_config },
            headers=headers,
        )
        response.raise_for_status()
        query_run_id = response.json()['run_id']

        # step 2: poll for results
        while status in ['queued', 'running']:
            time.sleep(poll_interval)
            response = requests.get(
                f"{ALLIUM_BASE_URL}/query-runs/{query_run_id}",
                headers=headers,
            )
            response.raise_for_status()
            data = response.json()
            status = data['status']
            logger.info(f"query results polling status: {status}")

        if status == 'failed':
            logger.error(f"query failed: {data.get('error', 'Unknown error')}")
            return all_rows
        elif status != 'success':
            logger.error(f"query did not succeed, final status: {status}, data: {data}")
            return all_rows

        logger.info("query successful, fetching results")

        # step 3: get results once done
        response = requests.get(f"{ALLIUM_BASE_URL}/query-runs/{query_run_id}/results", headers=headers)
        response.raise_for_status()
        data_response = response.json()
        all_rows = data_response.get("data", [])

        return all_rows
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP Request error during allium api call: {e}")
        return all_rows
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error during database operation: {e}")
        return all_rows
    except Exception as e:
        logger.error(f"unexpected error occurred: {e}")
        return all_rows