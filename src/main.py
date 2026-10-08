import os
import time
import base64
import json
import gzip
import asyncio
import logging
import math
import random
import aiohttp
from aiohttp_retry import ExponentialRetry, RetryClient
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

from typing import Any, Dict, List, Optional, Tuple

# Configure logging level based on DEBUG_LOGGING environment variable
logger  = logging.getLogger()
debug_env = os.getenv("DEBUG_LOGGING", "false")
logger.setLevel(logging.DEBUG if debug_env.lower() in ("true", "1", "yes") else logging.INFO)


def number_env(name: str, default, min_value, max_value=None, cast=int):
    """Reads a numeric env variable (int or float, per `cast`); invalid values fall back to the default and out-of-range values are clamped (with a warning)."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = cast(raw)
        if not math.isfinite(value):
            raise ValueError
    except ValueError:
        logger.warning(f"{name}={raw!r} is not a valid {'integer' if cast is int else 'number'}; using {default}")
        return default
    if value < min_value:
        logger.warning(f"{name}={value} is below {min_value}; using {min_value}")
        return min_value
    if max_value is not None and value > max_value:
        logger.warning(f"{name}={value} is above {max_value}; using {max_value}")
        return max_value
    return value


# Required env variables
TALEND_REGION = os.environ.get("TALEND_REGION", "us").lower()
NR_REGION = os.environ.get("NR_REGION", "US")
COLLECT_TALEND_LOGS = os.environ.get("COLLECT_TALEND_LOGS", "true")
TALEND_LOG_LEVEL = os.environ.get("TALEND_LOG_LEVEL", "WARN")
# Collection timing (see README "Choosing values"):
# - SCHEDULE_INTERVAL_MIN: minutes between scheduled runs; must equal the EventBridge cron interval in template.yaml.
#   Each run sends executions that finished in [event time - interval, event time), so a mismatch loses
#   (interval too small) or duplicates (interval too large) final states.
# - LOOKBACK_TIME_DAYS: each run lists every execution triggered in the last N days (Talend `lastDays`, 1-60)
#   and picks the finished ones from that list. Must exceed the longest task runtime: an execution that runs
#   longer than the lookback never gets a final state.
SCHEDULE_INTERVAL_MIN = number_env("SCHEDULE_INTERVAL_MIN", 60, 1)
LOOKBACK_TIME_DAYS = number_env("LOOKBACK_TIME_DAYS", 2, 1, 60)
INGEST_COMPLETE_TASKS_ONLY = os.environ.get("INGEST_COMPLETE_TASKS_ONLY", "false").lower() == "true"
USE_TALEND_LOG_TS = os.environ.get("USE_TALEND_LOG_TS", "false").lower() == "true"
TALEND_MAX_REQUESTS_PER_SEC = number_env("TALEND_MAX_REQUESTS_PER_SEC", 8.0, 1.0, cast=float)
AWS_SECRET_ID = os.environ.get("AWS_SECRET_ID")
AWS_SECRET_REGION = os.environ.get("AWS_SECRET_REGION")

if not AWS_SECRET_ID or not AWS_SECRET_REGION:
    raise RuntimeError("Missing required AWS_SECRET_ID or AWS_SECRET_REGION env variable(s)")

if "LOOKBACK_TIME_MIN" in os.environ and "SCHEDULE_INTERVAL_MIN" not in os.environ:
    logger.warning(f"LOOKBACK_TIME_MIN was renamed to SCHEDULE_INTERVAL_MIN and is ignored; using SCHEDULE_INTERVAL_MIN={SCHEDULE_INTERVAL_MIN}")

if SCHEDULE_INTERVAL_MIN > LOOKBACK_TIME_DAYS * 24 * 60:
    logger.warning(f"SCHEDULE_INTERVAL_MIN={SCHEDULE_INTERVAL_MIN} exceeds LOOKBACK_TIME_DAYS={LOOKBACK_TIME_DAYS}; executions that finished early in the interval may be missed")

# Global Caches/Constants
secrets_cache = None
token_cache: Dict[str, Any] = {"access_token": None, "expiry_time": 0}
rate_limiter = None  # Created per invocation in main(), since each asyncio.run() starts a new event loop
run_errors: List[str] = []  # Failures during the current invocation; reset in main(), raised at the end of the run

TASK_EXECUTION_TABLE = 'talendTaskExecutionStats'
JOB_COMPONENT_TABLE = 'talendJobComponentStats'
# Talend states of an execution that has not finished yet (`executionStatus`, plus the coarse `status` field)
IN_FLIGHT_STATUSES = {"STARTING_FLOW_EXECUTION", "EXECUTION_EVENT_RECEIVED", "DISPATCHING_FLOW"}
IN_FLIGHT_COARSE_STATUSES = {"executing", "dispatching"}
TALEND_URL = f"https://api.{TALEND_REGION}.cloud.talend.com"
TASKS_FILE_PATH = os.path.join(os.path.dirname(__file__), 'data', 'tasks.json')

# Talend API max page sizes per endpoint (larger values are rejected with HTTP 400)
EXECUTIONS_PAGE_LIMIT = 100
COMPONENTS_PAGE_LIMIT = 200
LOGS_PAGE_COUNT = 200
# Defensive cap on pagination depth and request safety caps
API_MAX_OFFSET = 1000
MAX_PAGES = 50
HTTP_TIMEOUT_SEC = 30  # Per request; with 3 retry attempts this stays under the 120s Lambda timeout
NR_BATCH_MAX_BYTES = 1_000_000  # NR Event API and Log API limit is 1MB per POST
TALEND_LOG_TS_MAX_AGE_MS = 47 * 60 * 60 * 1000  # NR may drop logs with timestamps older than 48h


"""
Logs an error and records it for the current run, so the invocation fails (raises) once everything collectable is posted.
"""
def record_error(message: str):
    logger.error(message)
    run_errors.append(message)


"""
Raises if any error was recorded during the run, so the Lambda Errors metric (and any alarm on it) reflects lost data.
"""
def raise_if_run_errors():
    if run_errors:
        summary = "; ".join(run_errors[:10]) + (f"; and {len(run_errors) - 10} more" if len(run_errors) > 10 else "")
        raise RuntimeError(f"Collection finished with {len(run_errors)} error(s); data for this window may be incomplete: {summary}")


"""
Spaces requests evenly to stay under Talend's rate limit (~10 req/s per endpoint).
Each caller reserves the next free slot under the lock, then sleeps until it outside the lock.
"""
class RateLimiter:
    def __init__(self, requests_per_sec: float):
        self.interval = 1.0 / requests_per_sec
        self.next_time = 0.0
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            slot = max(now, self.next_time)
            self.next_time = slot + self.interval
        if slot > now:
            await asyncio.sleep(slot - now)


"""
Exponential backoff with jitter that also honors an integer Retry-After header on a 429.
Talend's 429s currently carry no Retry-After and clear within ~1s, so the backoff is what normally applies.
"""
class TalendRetry(ExponentialRetry):
    def get_timeout(self, attempt: int, response: aiohttp.ClientResponse | None = None) -> float:
        timeout = super().get_timeout(attempt, response) + random.uniform(0, 0.5)
        if response is not None and response.status == 429:
            retry_after = response.headers.get("Retry-After", "")
            if retry_after.isdigit():
                timeout = min(float(retry_after), self._max_timeout)
            logger.warning(f"Rate limited (429) by {response.url.host}{response.url.path}; retrying in {timeout:.1f}s")
        return timeout

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
async def get_talend_token(session: RetryClient, client_id: str, client_secret: str) -> str:
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
    pages = 0
    
    while pages < MAX_PAGES:
        pages += 1
        try:
            if rate_limiter:
                await rate_limiter.wait()
            async with session.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                # Offset-based pagination metadata is top-level for Task Executions, nested under "metrics" for Component Metrics
                page = data if "offset" in data else data.get("metrics", {})
                # Guard against an API that ignores the requested offset, which would resend an already-collected page
                if "offset" in page and page["offset"] != params.get("offset", 0):
                    logger.warning(f"Pagination for {url} returned offset {page['offset']} instead of {params.get('offset', 0)}; stopping")
                    break

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
                # Offset-based pagination (i.e- Task Executions, Component Metrics)
                if "offset" in page and "limit" in page:
                    # Advance by the number of items actually received in this page
                    page_items = items if isinstance(items, list) else []
                    next_offset = page["offset"] + len(page_items)
                    total = page.get("total")
                    if not page_items:
                        break
                    # "total" is optional for Task Executions; without it, a short page is the last page
                    if (total is not None and next_offset >= total) or (total is None and len(page_items) < page["limit"]):
                        break
                    if next_offset > API_MAX_OFFSET:
                        logger.warning(f"Reached max API offset ({API_MAX_OFFSET}) for {url}; results truncated at {len(all_items)} of {total if total is not None else 'unknown'} items")
                        if type == 'executions':
                            # Results are newest first, so the oldest executions (the long runners) are the ones cut
                            logger.warning(f"LOOKBACK_TIME_DAYS={LOOKBACK_TIME_DAYS} may be too large for this task's volume; the oldest executions in the lookback were not checked and their final states may be missed")
                        break
                    params["offset"] = next_offset
                # Index-based pagination (i.e- Execution Logs)
                elif "nextIndex" in data and data["nextIndex"] is not None:
                    if data["nextIndex"] <= params.get("startIndex", 0):
                        logger.warning(f"Pagination for {url} did not advance past startIndex {params.get('startIndex', 0)}; stopping")
                        break
                    params["startIndex"] = data["nextIndex"]
                # Single page only (all data fetched)
                else:
                    break
            
            if is_first_page:
                logger.debug(f"Successfully fetched first page from {url}", extra={"params": params})
                is_first_page = False

        except aiohttp.ClientResponseError as e:
            # Component stats/logs may not exist for an execution (e.g. rejected before it started): treat as no data
            if e.status == 404 and type in ('component', 'logs'):
                logger.debug(f"No {type} data for {url} (404)")
                break
            record_error(f"HTTP error while fetching from {url}: {e.status} {e.message}")
            break
        except Exception as e:
            record_error(f"An unexpected error occurred during pagination for {url}: {e}")
            break

    else:
        logger.warning(f"Stopped paginating {url} after {MAX_PAGES} pages; results may be incomplete")

    return all_items


"""
Returns the (start_ms, end_ms) finish window as epoch milliseconds: a run sends the executions whose
final state was reached in [start_ms, end_ms). This is not the API query range (see get_task_executions).
Scheduled runs use the EventBridge event time as the window end, so with a cron that matches
SCHEDULE_INTERVAL_MIN consecutive windows are contiguous (no gaps/overlaps regardless of runtime jitter).
Manual/local runs without an event time use a rolling window ending now.
"""
def get_time_window(event) -> tuple:
    interval_ms = SCHEDULE_INTERVAL_MIN * 60 * 1000
    event_time = event.get("time") if isinstance(event, dict) else None
    end_ms = None

    if event_time:
        try:
            end_ms = int(datetime.fromisoformat(event_time.replace("Z", "+00:00")).timestamp() * 1000)
            # Scheduled rules fire within the scheduled minute, so drop any seconds to get the exact cron time
            end_ms -= end_ms % 60000
        except (ValueError, AttributeError):
            logger.warning(f"Could not parse event time '{event_time}'; using current time for collection window")

    if end_ms is None:
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    return end_ms - interval_ms, end_ms  


"""
Fetches task executions for a given list of task IDs and splits them into:
- finals: executions whose final state was reached in [window_start_ms, window_end_ms), each sent exactly once
- snapshots: executions still in flight (empty when INGEST_COMPLETE_TASKS_ONLY is set)
"""
async def get_task_executions(session: RetryClient, task_ids: List[Dict[str, str]], token: str, window_start_ms: int, window_end_ms: int) -> Tuple[List[Any], List[Any]]:
    headers = {"Authorization": f"Bearer {token}"}

    async def fetch_for_task(task_id: str) -> List:
        endpoint = f"{TALEND_URL}/processing/executables/tasks/{task_id}/executions"
        # The API's from/to filter on triggerTimestamp only (not start/finish, despite the Talend docs), so querying
        # the window itself misses executions that finish after the window they were triggered in. Instead, list every
        # execution triggered within the lookback (lastDays overrides from/to) and select finals by finish time below.
        params = {"lastDays": LOOKBACK_TIME_DAYS, "limit": EXECUTIONS_PAGE_LIMIT, "offset": 0}
        return await fetch_all_pages(session, endpoint, params, 'executions', headers)

    def parse_ts(ts: Optional[str]) -> Optional[datetime]:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None

    # Create a list of coroutines, one for each task ID
    tasks = [fetch_for_task(task["id"]) for task in task_ids]
    # Execute all tasks concurrently
    results_per_task = await asyncio.gather(*tasks, return_exceptions=True)

    finals, snapshots = [], []
    seen_ids = set()
    fetched = duplicates = in_flight = 0
    for i, result in enumerate(results_per_task):
        task_info = task_ids[i]

        if isinstance(result, Exception):
            record_error(f"Failed to fetch executions for task {task_ids[i]}: {result}")
            continue

        for t in result:
            fetched += 1
            try:
                # New executions arriving while paging a newest-first list shift offsets and can repeat items across pages
                execution_id = t.get("executionId")
                if execution_id in seen_ids:
                    duplicates += 1
                    continue
                if execution_id:
                    seen_ids.add(execution_id)

                t["taskName"] = task_info.get("name")
                status = t.get("executionStatus")
                is_in_flight_status = status in IN_FLIGHT_STATUSES or t.get("status") in IN_FLIGHT_COARSE_STATUSES
                start_dt = parse_ts(t.get("startTimestamp"))
                finish_dt = parse_ts(t.get("finishTimestamp"))

                # A finish time always means a final state; without one, only known in-flight states are snapshots.
                # Other states without a finish time (e.g. rejected before start) use the trigger time as their final time.
                if finish_dt:
                    final_dt = finish_dt
                elif is_in_flight_status:
                    final_dt = None
                else:
                    final_dt = parse_ts(t.get("triggerTimestamp"))
                    if final_dt is None:
                        logger.warning(f"Skipping execution {execution_id} with status {status}: no finishTimestamp or triggerTimestamp")
                        continue

                if final_dt:
                    # Each final state is sent only in the window containing its final time ([start, end)), so it is sent
                    # exactly once even though every run within the lookback lists it
                    if not (window_start_ms <= int(final_dt.timestamp() * 1000) < window_end_ms):
                        continue
                    if not finish_dt:
                        logger.warning(f"Execution {execution_id} has status {status} but no finishTimestamp; using triggerTimestamp as its final time")
                    elif is_in_flight_status:
                        logger.warning(f"Execution {execution_id} has in-flight status {status} but a finishTimestamp; treating it as final")
                    t["executionDurationSec"] = (finish_dt - start_dt).total_seconds() if (start_dt and finish_dt) else -1
                else:
                    in_flight += 1
                    # Optionally skip in-progress snapshots
                    if INGEST_COMPLETE_TASKS_ONLY:
                        continue
                    t["executionDurationSec"] = -1

                runtime = t.get("runtime")
                t["runtime"] = runtime.get("type", "unknown") if isinstance(runtime, dict) else (runtime or "unknown")
                t["eventType"] = TASK_EXECUTION_TABLE
                (finals if final_dt else snapshots).append(t)
            except (ValueError, TypeError, AttributeError) as e:
                record_error(f"Could not process execution record {t.get('executionId') if isinstance(t, dict) else t} due to data issue: {e}")

    logger.info(f"Fetched {fetched} executions triggered in the last {LOOKBACK_TIME_DAYS} days ({duplicates} duplicates); "
                f"{len(finals)} finished in the window, {in_flight} in flight ({len(snapshots)} sent as snapshots)")
    return finals, snapshots

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
        params = {"limit": COMPONENTS_PAGE_LIMIT, "offset": 0}
        
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
            record_error(f"Failed to fetch components for execution {executions[i].get('executionId')}: {result}")
            continue
        all_components.extend(result or [])
        
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
        # This endpoint uses startIndex/count pagination. Newest first, so when MAX_PAGES is hit the dropped lines
        # are the oldest and a failed job's final errors are kept. Logs are only fetched for finished executions,
        # so no new lines arrive to shift indexes while paging
        params = {"count": LOGS_PAGE_COUNT, "startIndex": 0, "order": "DESC"}
        
        logs = await fetch_all_pages(session, endpoint, params, 'logs', headers)
        timestamp = int(time.time() * 1000)

        processed_logs = []
        level_map = {name: level for level, name in log_levels.items()}
        min_log_level = level_map.get(TALEND_LOG_LEVEL.upper(), level_map["WARN"])
        # Logs with an unknown/missing severity are kept rather than raising
        filtered_logs = [
            log for log in logs
            if level_map.get(str(log.get('severity')).upper(), min_log_level) >= min_log_level
        ]
        for log in filtered_logs:
            log["logType"] = "talend-etl"
            log["talendTimestamp"] = log.pop("logTimestamp", None)
            # Optionally use when the log actually happened (epoch ms); fall back to collection time if missing,
            # or if older than NR accepts (< 48 hrs).
            # logs of long-running jobs are only collected once the job finishes
            if USE_TALEND_LOG_TS and isinstance(log["talendTimestamp"], int) and log["talendTimestamp"] >= timestamp - TALEND_LOG_TS_MAX_AGE_MS:
                log["timestamp"] = log["talendTimestamp"]
            else:
                log["timestamp"] = timestamp
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
            record_error(f"Failed to fetch logs for execution {executions[i].get('executionId')}: {result}")
            continue
        all_logs.extend(result or [])

    if USE_TALEND_LOG_TS:
        stale = sum(1 for log in all_logs if isinstance(log["talendTimestamp"], int) and log["timestamp"] != log["talendTimestamp"])
        if stale:
            logger.warning(f"{stale} log(s) have a Talend timestamp older than {TALEND_LOG_TS_MAX_AGE_MS // 3600000}h; using collection time as their timestamp")

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
        record_error(f"Unknown payload type for New Relic: {payload_type}")
        return False

    # Logs (whole executions at INFO) and component stats (up to ~1,200 per execution) can be large,
    # so split both to stay under the per-POST limit
    batches = batch_by_size(payload, NR_BATCH_MAX_BYTES)

    all_ok = True
    for i, batch in enumerate(batches, start=1):
        batch_label = f" (batch {i}/{len(batches)}, {len(batch)} records)" if len(batches) > 1 else ""
        try:
            async with session.post(url, headers=headers, data=compress_payload(batch)) as response:
                if response.status != expected_status:
                    response_text = await response.text()
                    record_error(f"Error posting to New Relic {payload_type} API{batch_label}. Status: {response.status}. Response: {response_text}")
                    all_ok = False
        except Exception as e:
            record_error(f"Exception while posting to New Relic {payload_type} API{batch_label}: {e}")
            all_ok = False
    return all_ok


"""
Splits records into batches whose JSON array encoding (uncompressed, UTF-8) stays within max_bytes.
A single record larger than max_bytes is sent in a batch of its own.
"""
def batch_by_size(records: List[Any], max_bytes: int) -> List[List[Any]]:
    batches, batch, size = [], [], 2  # 2 bytes for the enclosing "[]"
    for record in records:
        record_size = len(json.dumps(record).encode("utf-8")) + 2  # plus the ", " separator
        if batch and size + record_size > max_bytes:
            batches.append(batch)
            batch, size = [], 2
        batch.append(record)
        size += record_size
    if batch:
        batches.append(batch)
    return batches


"""
Main function to handle all pull/push logic
"""
async def main(event=None):
    run_errors.clear()

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

    # Request retry config (429 waits ~1s then ~2s, matching Talend's observed ~1s rate-limit recovery)
    retry_opts = TalendRetry(
        attempts = 3,
        start_timeout=0.5,
        max_timeout=10,
        statuses={429, 500, 502, 503, 504},
        # ClientConnectionError covers connect failures, resets (ClientOSError) and ServerDisconnectedError
        exceptions={aiohttp.ClientConnectionError, asyncio.TimeoutError}
    )

    # Shared limiter for all paginated Talend GETs
    global rate_limiter
    rate_limiter = RateLimiter(TALEND_MAX_REQUESTS_PER_SEC)

    # Fetch Talend Data via single ClientSession for conn pooling
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)) as session:
        retry_client = RetryClient(client_session = session, retry_options=retry_opts)

        # Populate global token cache (User PAT or fetched SA token)
        if service_account_creds == True:
            await get_talend_token(retry_client, talend_client_id, talend_client_secret)
        else:
            token_cache["access_token"] = talend_user_pat


        # Fetch task executions
        logger.info(f"Starting data collection for {len(tasks_array)} Talend tasks")
        window_start_ms, window_end_ms = get_time_window(event)
        logger.info(f"Collection window: {window_start_ms} - {window_end_ms} (epoch ms)")
        final_executions, snapshot_executions = await get_task_executions(retry_client, tasks_array, token_cache["access_token"], window_start_ms, window_end_ms)
        task_executions = final_executions + snapshot_executions

        if len(task_executions) == 0:
            raise_if_run_errors()
            logger.warning(f"No executions finished within the collection window ({SCHEDULE_INTERVAL_MIN} minutes) or in flight within the lookback ({LOOKBACK_TIME_DAYS} days) for configured tasks.")
            return {
                'statusCode': 404,
                'body': json.dumps({'warning': 'No executions found within configured time window.'})
            }

        # Fetch component metrics for final states only; in-flight snapshots get them once they finish, so they are sent once
        component_metrics = await get_executions_components_stats(retry_client, final_executions, token_cache["access_token"])
        
        # Write events to New Relic
        logger.info("Posting task execution events to New Relic.")
        execution_write_result = post_to_nr(retry_client, task_executions, "events", nr_ingest_key, nr_account_id)
        components_write_result = post_to_nr(retry_client, component_metrics, "events", nr_ingest_key, nr_account_id)

        event_write_results = await asyncio.gather(execution_write_result, components_write_result)
        if all(event_write_results):
            logger.info("All events successfully written to New Relic")

        # Fetch and publish logs
        if (COLLECT_TALEND_LOGS.lower() == "true"):
            execution_logs = await get_execution_logs(retry_client, final_executions, token_cache["access_token"])
            log_write_result = await post_to_nr(retry_client, execution_logs, "logs", nr_ingest_key, nr_account_id)
            if log_write_result:
                logger.info("All logs successfully written to New Relic.")

        # Everything collectable has been posted; fail the invocation if anything was lost
        raise_if_run_errors()


"""
AWS Lambda entry point
"""
def lambda_handler(event, context):
    return asyncio.run(main(event))