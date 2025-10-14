import os
import time
import base64
import json
import gzip
import asyncio
import logging
import aiohttp
from aiohttp_retry import ExponentialRetry, RetryClient
from datetime import datetime, timedelta, timezone
import boto3
from botocore.exceptions import ClientError

from typing import Any, Dict, List

# Configure logging level based on DEBUG_LOGGING environment variable
logger  = logging.getLogger()
debug_env = os.getenv("DEBUG_LOGGING", "false")
logger.setLevel(logging.DEBUG if debug_env.lower() in ("true", "1", "yes") else logging.INFO)

# Required env variables
TALEND_REGION = os.environ.get("TALEND_REGION", "us").lower()
NR_REGION = os.environ.get("NR_REGION", "US")
COLLECT_TALEND_LOGS = os.environ.get("COLLECT_TALEND_LOGS", "true")
TALEND_LOG_LEVEL = os.environ.get("TALEND_LOG_LEVEL", "WARN")
LOOKBACK_TIME_MIN = int(os.environ.get("LOOKBACK_TIME_MIN", 60))
AWS_SECRET_ID = os.environ.get("AWS_SECRET_ID")
AWS_SECRET_REGION = os.environ.get("AWS_SECRET_REGION")

if not AWS_SECRET_ID or not AWS_SECRET_REGION:
    raise RuntimeError("Missing required AWS_SECRET_ID or AWS_SECRET_REGION env variable(s)")

# Global Caches/Constants
secrets_cache = None
token_cache: Dict[str, Any] = {"access_token": None, "expiry_time": 0}

TASK_EXECUTION_TABLE = 'talendTaskExecutionStats'
JOB_COMPONENT_TABLE = 'talendJobComponentStats'
TALEND_URL = f"https://api.{TALEND_REGION}.cloud.talend.com"
TASKS_FILE_PATH = os.path.join(os.path.dirname(__file__), 'data', 'tasks.json')

"""
Loads and parses tasks defined from bundled tasks.json
"""
def load_tasks_to_monitor():
    logger.info(f"Loading tasks to monitor from: {TASKS_FILE_PATH}")
    with open(TASKS_FILE_PATH, 'r', encoding='utf-8') as tasks:
        return json.load(tasks)


"""
Retrieve all secrets from AWS Secrets Manager.
This fetches a single secret containing a JSON object and caches it
in memory for the lifetime of a Lambda container.
"""
def get_secrets():
    global secrets_cache

    # Use existing secret in cache
    if secrets_cache:
        logger.debug("Returning existing secrets from cache")
        return secrets_cache

    session = boto3.session.Session()
    secrets_client = session.client(service_name='secretsmanager', region_name=AWS_SECRET_REGION)

    # Fetch secret from Secrets Manager
    try:
        secret_response = secrets_client.get_secret_value(SecretId=AWS_SECRET_ID)
    except ClientError as e:
        logger.error(f"Error retrieving secrets from Secret Manager: {e}")
        raise e

    if 'SecretString' in secret_response:
        secret = secret_response['SecretString']
        secrets_cache = json.loads(secret)
        logger.info(f"Successfully fetched and cached {len(secrets_cache)} secrets.")
        return secrets_cache
    else:
        logger.error("Secret data is binary, not a JSON string as expected.")
        raise ValueError("Secret is not in expected format")

"""
Retrieves a Talend bearer token via service account id/secret.
Only used if secret fetched has TALEND_CLIENT_ID/SECRET variables.
Generates a new token if the cached one is expired or non-existent.
"""
async def get_talend_token(session: aiohttp.ClientSession, client_id: str, client_secret: str) -> str:
    if token_cache["access_token"] and time.monotonic() < token_cache["expiry_time"]:
        logger.debug("Using cached Talend bearer token.")
        return

    logger.info("Talend service account token expired or not found. Generating a new one.")
    auth_string = f"{client_id}:{client_secret}"
    encoded_auth_string = base64.b64encode(auth_string.encode()).decode()

    token_url = f"{TALEND_URL}/security/oauth/token"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {encoded_auth_string}"
    }
    payload = {
        "audience": TALEND_URL,
        "grant_type": "client_credentials"
    }

    async with session.post(token_url, headers=headers, json=payload) as resp:
        resp.raise_for_status()
        token_resp = await resp.json()

        access_token = token_resp["access_token"]

        # Cache token w/ 29 min TTL (1 min safety buffer) - mainly for running the lambda < 30 min intervals
        token_cache["access_token"] = access_token
        token_cache["expiry_time"] = time.monotonic() + (29 * 60)

        logger.info("Successfully generated and cached a new Talend service account token")

"""
A generic function to fetch all pages from a paginated Talend API.
Handles both offset-based and index-based pagination types.
"""
async def fetch_all_pages(session: RetryClient, url: str, params: Dict[str, Any], type: str, headers: Dict[str, str]) -> List[Any]:
    all_items = []
    is_first_page = True
    current_offset = 0
    
    while True:
        try:
            async with session.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                # Append a single page of data to list
                items = data.get("items", data.get("data", data.get("metrics", {}).get("items",)))
                if isinstance(items, list):
                    if type == 'component':
                        for c in items:
                            c["eventType"] = JOB_COMPONENT_TABLE
                            c["artifactName"] = data.get("artifact_name")
                            c["artifactVersion"] = data.get("artifact_version")
                            c["taskId"] = data.get("task_id")
                            c["executionId"] = data.get("task_execution_id")
                            c["operator"] = data.get("operator")
                    all_items.extend(items)
                
                # Determine if there's a next page
                # Offset-based pagination (i.e- Component Metrics)
                if "total" in data and "limit" in data and "offset" in data:
                    if data["offset"] == data["total"]:
                        break
                    current_offset += 1
                    params["offset"] = current_offset
                # Index-based pagination (i.e- Execution Logs)
                elif "nextIndex" in data and data["nextIndex"] is not None:
                    params["startIndex"] = data["nextIndex"]
                # Single page only (all data fetched)
                else:
                    break
            
            if is_first_page:
                logger.debug(f"Successfully fetched first page from {url}", extra={"params": params})
                is_first_page = False

        except aiohttp.ClientResponseError as e:
            logger.error(f"HTTP error while fetching from {url}: {e.status} {e.message}", extra={"params": params})
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred during pagination for {url}: {e}", extra={"params": params})
            break

    return all_items  


"""
Fetches all task executions for a given list of task IDs 
within a user defined time window.
"""
async def get_task_executions(session: RetryClient, task_ids: List[Dict[str, str]], token: str) -> List[Any]:   
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=int(LOOKBACK_TIME_MIN))
    
    # Convert to milliseconds since epoch
    now_ms = int(now.timestamp() * 1000)
    start_ms = int(start.timestamp() * 1000)

    headers = {"Authorization": f"Bearer {token}"}

    async def fetch_for_task(task_id: str) -> List:
        endpoint = f"{TALEND_URL}/processing/executables/tasks/{task_id}/executions"
        params = {"from": start_ms, "to": now_ms}
        return await fetch_all_pages(session, endpoint, params, 'executions', headers)

    # Create a list of coroutines, one for each task ID
    tasks = [fetch_for_task(task["id"]) for task in task_ids]
    # Execute all tasks concurrently
    results_per_task = await asyncio.gather(*tasks, return_exceptions=True)

    all_executions = []
    for i, result in enumerate(results_per_task):
        task_info = task_ids[i]

        if isinstance(result, Exception):
            logger.error(f"Failed to fetch executions for task {task_ids[i]}: {result}")
            continue
        
        for t in result:
            try:
                t["taskName"] = task_info.get("name")
                start_ts = t.get("startTimestamp")
                finish_ts = t.get("finishTimestamp")
                if start_ts and finish_ts:
                    start = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
                    end = datetime.fromisoformat(finish_ts.replace("Z", "+00:00"))
                    t["executionDurationSec"] = (end - start).total_seconds()
                else:
                    t["executionDurationSec"] = -1
                
                t["runtime"] = t.get("runtime", {}).get("type", "unknown")
                t["eventType"] = TASK_EXECUTION_TABLE
                all_executions.append(t)
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not process execution record due to data issue: {t}. Error: {e}")

    return all_executions

"""
Fetches component-level statistics for a list of task executions.
"""
async def get_executions_components_stats(session: RetryClient, executions: List[Any], token: str) -> List[Any]:
    if not executions:
        logging.debug("No executions found to collect component metrics against.")
        return

    headers = {"Authorization": f"Bearer {token}"}

    async def fetch_single_execution(execution: Dict[str, Any]) -> List[Any]:
        execution_id = execution.get("executionId")
        if not execution_id:
            logging.debug("No execution_id found")
            return
            
        endpoint = f"{TALEND_URL}/monitoring/observability/executions/{execution_id}/component"
        # This endpoint uses offset/limit pagination
        params = {"limit": 200, "offset": 0}
        
        components = await fetch_all_pages(session, endpoint, params, 'component', headers)

        processed_components = []
        for c in components:
            c["executionDurationSec"] = execution.get("executionDurationSec", -1)
            processed_components.append(c)
        return processed_components

    tasks = [fetch_single_execution(ex) for ex in executions]
    results_per_execution = await asyncio.gather(*tasks, return_exceptions=True)
    
    all_components = []
    for i, result in enumerate(results_per_execution):
        if isinstance(result, Exception):
            logger.error(f"Failed to fetch components for execution {executions[i].get('executionId')}: {result}")
            continue
        all_components.extend(result)
        
    return all_components

"""
Fetches execution logs for a list of task executions.
"""
async def get_execution_logs(session: RetryClient, executions: List[Any], token: str) -> List[Any]:
    if not executions:
        logging.debug("No executions found to collect logs against.")
        return

    headers = {"Authorization": f"Bearer {token}"}
    log_levels = {
        0: "TRACE",
        1: "DEBUG",
        2: "INFO",
        3: "WARN",
        4: "ERROR",
        5: "FATAL"
    }

    async def fetch_single_execution(execution: Dict[str, Any]) -> List[Any]:
        execution_id = execution.get("executionId")
        if not execution_id:
            return
            
        endpoint = f"{TALEND_URL}/monitoring/executions/{execution_id}/logs"
        # This endpoint uses startIndex/count pagination
        params = {"count": 200, "startIndex": 0}
        
        logs = await fetch_all_pages(session, endpoint, params, 'logs', headers)
        timestamp = int(time.time() * 1000)

        processed_logs = []
        level_map = {name: level for level, name in log_levels.items()}
        min_log_level = level_map[TALEND_LOG_LEVEL.upper()]
        filtered_logs = [
            log for log in logs
            if level_map[log['severity']] >= min_log_level
        ]
        for log in filtered_logs:
            log["logType"] = "talend-etl"
            log["timestamp"] = timestamp
            log["talendTimestamp"] = log.pop("logTimestamp", None)
            # log["timestamp"] = log.pop("logTimestamp", None)
            log["message"] = log.pop("logMessage", None)
            log["taskId"] = execution.get("taskId")
            log["taskVersion"] = execution.get("taskVersion")
            log["executionId"] = execution.get("executionId")
            log["executionStatus"] = execution.get("executionStatus")
            log["executionErrorMessage"] = execution.get("errorMessage")
            log["executionType"] = execution.get("executionType")
            processed_logs.append(log)
        return processed_logs

    tasks = [fetch_single_execution(ex) for ex in executions]
    results_per_execution = await asyncio.gather(*tasks, return_exceptions=True)

    all_logs = []
    for i, result in enumerate(results_per_execution):
        if isinstance(result, Exception):
            logger.error(f"Failed to fetch logs for execution {executions[i].get('executionId')}: {result}")
            continue
        all_logs.extend(result)

    return all_logs

"""
Compresses json payload using GZIP.
"""
def compress_payload(data):
    try:
        payload_json = json.dumps(data).encode('utf-8')
        compressed = gzip.compress(payload_json)
        return compressed
    except Exception as e:
        logger.error("Failed to compress payload")
        raise e

"""
Sends a compressed payload to the appropriate New Relic API endpoint.
Handles both events and logs.
"""
async def post_to_nr(session: RetryClient, payload: List[Any], payload_type: str, api_key: str, account_id: str) -> bool:
    if not payload:
        logger.info(f"No payload to send for type '{payload_type}'. Skipping.")
        return True

    headers = {
        "Content-Type": "application/json",
        "Api-Key": api_key,
        "Content-Encoding": "gzip"
    }

    if payload_type == "events":
        url = f"https://insights-collector.{'eu01.' if NR_REGION.upper() == "EU" else ''}newrelic.com/v1/accounts/{account_id}/events"
        expected_status = 200
    elif payload_type == "logs":
        url = f"https://log-api.{'eu.' if NR_REGION.upper() == "EU" else ''}newrelic.com/log/v1"
        expected_status = 202
    else:
        logger.error(f"Unknown payload type for New Relic: {payload_type}")
        return False

    compressed_payload = compress_payload(payload)

    try:
        async with session.post(url, headers=headers, data=compressed_payload) as response:
            if response.status == expected_status:
                return True
            else:
                response_text = await response.text()
                logger.error(
                    f"Error posting to New Relic {payload_type} API. Status: {response.status}. Response: {response_text}"
                )
                return False
    except Exception as e:
        logger.error(f"Exception while posting to New Relic {payload_type} API: {e}")
        return False


"""
Main function to handle all pull/push logic
"""
async def main():
    # Load tasks to monitor from file
    try:
        tasks_array = load_tasks_to_monitor()
        logger.info(f"Successfully loaded {len(tasks_array)} tasks from file")
    except Exception as e:
        logger.error(f"Error loading tasks from file: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'An error occurred loading tasks from file'})
        }

    # Fetch secret from AWS Secret Manager
    secrets = get_secrets()
    nr_ingest_key = secrets.get("NEW_RELIC_INGEST_KEY")
    nr_account_id = secrets.get("NEW_RELIC_ACCOUNT_ID")
    talend_client_id = secrets.get("TALEND_CLIENT_ID", None)
    talend_client_secret = secrets.get("TALEND_CLIENT_SECRET", None)
    talend_user_pat = secrets.get("TALEND_USER_PAT", None)
    service_account_creds = True

    # Required secrets validation
    if not all([nr_account_id, nr_ingest_key]):
        raise ValueError("One or more required New Relic secrets missing from Secrets Manager.")

    if not all([talend_client_id, talend_client_secret]):
        service_account_creds = False
    
    if service_account_creds == False:
        if not talend_user_pat:
            raise ValueError("Talend User PAT secret missing from Secrets Manager.")
    else:
        if not all([talend_client_id, talend_client_secret]):
            raise ValueError("Talend Service Account ID/Secret missing from Secrets Manager.")

    # Request retry config
    retry_opts = ExponentialRetry(
        attempts = 3,
        statuses={500, 502, 503, 504},
        exceptions={aiohttp.ClientConnectorError, asyncio.TimeoutError}
    )

    # Fetch Talend Data via single ClientSession for conn pooling
    async with aiohttp.ClientSession() as session:
        retry_client = RetryClient(client_session = session, retry_options=retry_opts)

        # Populate global token cache (User PAT or fetched SA token)
        if service_account_creds == True:
            await get_talend_token()
        else:
            token_cache["access_token"] = talend_user_pat


        # Fetch task executions
        logger.info(f"Starting data collection for {len(tasks_array)} Talend tasks")
        task_executions = await get_task_executions(retry_client, tasks_array, token_cache["access_token"])

        if len(task_executions) == 0:
            logger.warning(f"No executions returned for configured tasks within the last {LOOKBACK_TIME_MIN} minutes.")
            return {
                'statusCode': 404,
                'body': json.dumps({'warning': 'No executions found within configured time window.'})
            }
        
        # Fetch executions component metrics
        component_metrics = await get_executions_components_stats(retry_client, task_executions, token_cache["access_token"])
        
        # Write events to New Relic
        logger.info("Posting task execution events to New Relic.")
        execution_write_result = post_to_nr(retry_client, task_executions, "events", nr_ingest_key, nr_account_id)
        components_write_result = post_to_nr(retry_client, component_metrics, "events", nr_ingest_key, nr_account_id)

        event_write_results = await asyncio.gather(execution_write_result, components_write_result)
        if all(event_write_results):
            logger.info("All events successfully written to New Relic")

        # Fetch and publish logs
        if (COLLECT_TALEND_LOGS.lower() == "true"):
            execution_logs = await get_execution_logs(retry_client, task_executions, token_cache["access_token"])
            log_write_result = await post_to_nr(retry_client, execution_logs, "logs", nr_ingest_key, nr_account_id)
            if log_write_result:
                logger.info("All logs successfully written to New Relic.")


"""
AWS Lambda entry point
"""
def lambda_handler(event, context):
    return asyncio.run(main())