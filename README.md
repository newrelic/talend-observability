[![Community Project header](https://github.com/newrelic/open-source-office/raw/master/examples/categories/images/Community_Project.png)](https://github.com/newrelic/open-source-office/blob/master/examples/categories/index.md#category-community-project)


# Talend Observability

AWS Cloudformation Stack that periodically polls and forwards Talend task execution metrics/logs to New Relic. The AWS stack consists of:

* Python 3.13 Lambda
* EventBridge Rule
* Necessary IAM Policies

## Pre-Requirements

* [AWS CLI](https://medium.com/@jeffreyomoakah/installing-aws-cli-using-homebrew-a-simple-guide-486df9da3092) installed/configured
* [AWS SAM CLI](https://formulae.brew.sh/formula/aws-sam-cli) installed
* Talend Authentication configured [(see Talend Authentication section)](#talend-authentication)
* AWS Secret Created [(see AWS Secret section)](#aws-secret)
* Configuration
  * [Talend Tasks](./src/data/tasks.json)
  * [Lambda/EventBridge](./template.yaml)
* Python 3.13 (local development)
* Docker (local development)
* Necessary IAM permissions configured (see below)

### IAM Minimal Permissions for SAM CLI Deployments & Lambda Execution

#### AWS CloudFormation
Allows creation, update, deletion, and management of stacks and change sets, including IAM resource acknowledgements:

- `cloudformation:CreateStack`
- `cloudformation:UpdateStack`
- `cloudformation:DeleteStack`
- `cloudformation:DescribeStacks`
- `cloudformation:ListStackResources`
- `cloudformation:CreateChangeSet`
- `cloudformation:ExecuteChangeSet`
- `cloudformation:DeleteChangeSet`
- `cloudformation:GetTemplate`
- `cloudformation:DescribeStackEvents`
- **Capability flags:** must allow `CAPABILITY_IAM` or `CAPABILITY_NAMED_IAM` when deploying templates that include IAM resources.

---

#### S3
Used by SAM to package and upload function code and artifacts:

- `s3:PutObject`
- `s3:GetObject`
- `s3:ListBucket`
- `s3:DeleteObject`
- *(Optional)* `s3:CreateBucket` if SAM needs to provision the deployment bucket for you

---

#### AWS Identity and Access Management (IAM)
Needed to create and manage execution roles, inline policies, and to allow Lambda to assume roles:

- **Role lifecycle:**  
  - `iam:CreateRole`  
  - `iam:DeleteRole`
- **Policy management:**  
  - `iam:AttachRolePolicy`  
  - `iam:DetachRolePolicy`  
  - `iam:PutRolePolicy`  
  - `iam:DeleteRolePolicy`
- **Role passing:**  
  - `iam:PassRole`

---

#### Lambda
Required to create, update, configure, and delete Lambda functions:

- `lambda:CreateFunction`
- `lambda:UpdateFunctionCode`
- `lambda:UpdateFunctionConfiguration`
- `lambda:DeleteFunction`
- `lambda:GetFunction`
- `lambda:GetFunctionConfiguration`
- `lambda:AddPermission`
- `lambda:RemovePermission`

---

#### EventBridge (CloudWatch Events)
Grants ability to schedule the Lambda and manage targets:

- `events:PutRule`
- `events:DeleteRule`
- `events:PutTargets`
- `events:RemoveTargets`
- `events:DescribeRule`
- `events:ListRules`

---

#### CloudWatch Logs
Allows Lambda function to create log groups/streams and write logs:

- `logs:CreateLogGroup`
- `logs:CreateLogStream`
- `logs:PutLogEvents`


---

#### Secrets Manager
Allows Lambda function to retrieve specific secret containing Talend/New Relic keys & secrets:

- `secretsmanager:GetSecretValue`

## Talend Authentication

The lambda polls the following Talend API endpoints to collect metrics/logs:

* [Task Executions](https://talend.qlik.dev/apis/processing/2021-03/#operation_get-task-executions)
* [Component Metrics](https://talend.qlik.dev/apis/observability-metrics/2021-03/#operation_get-component-metrics-of-task-runs)
* [Execution Logs](https://talend.qlik.dev/apis/execution-logs/2021-03/#operation_get-task-execution-logs)


To successfully make requests to these endpoints, either a [Service Account id and secret](https://help.qlik.com/talend/en-US/management-console-user-guide/Cloud/creating-service-account) or [PAT (personal access token)](https://help.qlik.com/talend/en-US/management-console-user-guide/Cloud/cloud-access-token?utm_source=tadoc&utm_medium=learn_more) must be created.


## AWS Secret

The lambda function fetches and caches an AWS secret that contains several required inputs in order to poll Talend and forward data to New Relic, including:

* Talend Service Account secret/id OR Talend User PAT **NOTE: only one of these are required, not both**
* [New Relic ingest key](https://docs.newrelic.com/docs/apis/intro-apis/new-relic-api-keys/#license-key)
* New Relic account id

Example secrets can be found under [example_secrets/](./example_secrets/), depending on if you wish to authenticate via Talend Service Account or User PAT. To create the secret via AWS CLI, run (as an example):

```bash
aws secretsmanager create-secret \
    --name "talend/newrelic-integration" \
    --description "Secrets for the Talend to New Relic monitoring lambda" \
    --secret-string file://secrets.json \
    --region "us-east-1"
```


## Configuration

A list of tasks must be input within [tasks.json](./src/data/tasks.json) before deploying. Each object must contain a task id to fetch executions for, and an associated name or alias for each task id.

The following are available function environment variables that can be configured under the SAM `template.yaml`, or within AWS itself after the function is deployed.

| Input | Type | Required | Description
| ----- | ---- | -------- | -----------
| TALEND_REGION | string | FALSE | Talend region where tasks are running. Can be one of: `US`\|`EU`\|`AP`\|`AU`\|`US-WEST`. Default: `US`
| NR_REGION | enum | FALSE | Region of New Relic account. Can be one of `US`\|`EU`. Default: `US`
| DEBUG_LOGGING | string | FALSE | Enables lambda debug logging for troubleshooting. Can be one of: `true`\|`false`. Default: `false`
| COLLECT_TALEND_LOGS | string | FALSE | When `true`, collects all task execution logs. Logs are posted to New Relic in batches of up to 1MB (uncompressed) each, the Log API's per-request limit. Can be one of: `true`\|`false`. Default: `true`
| TALEND_LOG_LEVEL | string | FALSE | The minimum log level of Talend execution logs to forward to NR. Can be one of: `TRACE`\|`DEBUG`\|`INFO`\|`WARN`\|`ERROR`\|`FATAL`. An invalid value falls back to `WARN`. Default: `WARN`
| SCHEDULE_INTERVAL_MIN | int | FALSE | Minutes between scheduled runs. **Must equal the EventBridge cron interval** (see [Choosing values](#choosing-values)). Each run sends the executions that finished in the previous interval. Minimum: `1`. Default: `60`
| LOOKBACK_TIME_DAYS | int | FALSE | How many days of executions (by trigger time) each run checks for newly finished ones, and for in-flight snapshots. Must exceed your longest task runtime. Range: `1`-`60` (out-of-range values are clamped). Default: `2`
| INGEST_COMPLETE_TASKS_ONLY | string | FALSE | When `true`, only final states are sent to New Relic. When `false`, executions that are still running are also sent on every run as snapshots (`executionDurationSec = -1`, without logs or component stats). Can be one of: `true`\|`false`. Default: `false`
| USE_TALEND_LOG_TS | string | FALSE | When `true`, the New Relic log `timestamp` is set to the time the log was written in Talend (`logTimestamp`, also kept as `talendTimestamp`). When `false`, `timestamp` is the time the Lambda collected the log. Logs missing a Talend timestamp, or older than 47 hours (New Relic may drop logs older than 48 hours), fall back to collection time. Can be one of: `true`\|`false`. Default: `false`
| TALEND_MAX_REQUESTS_PER_SEC | float | FALSE | Max Talend API requests per second, shared across all endpoints. Talend rate-limits at roughly 10 requests/second per endpoint (HTTP 429); lower this if `Rate limited (429)` warnings appear in the Lambda logs. At 8 req/s, one run fits roughly 900 Talend requests in the 120s Lambda timeout, so large task counts or high execution volumes may need a longer timeout or a shorter `LOOKBACK_TIME_DAYS` (see [Choosing values](#choosing-values)). Retries of rate-limited or failed requests are not paced by this limit, so a burst of 429s can briefly exceed it. Invalid values fall back to the default. Minimum: `1`. Default: `8`
| AWS_SECRET_ID | string | TRUE | ARN of AWS Secret containing required Talend/NR keys and secrets. Configured under the `Parameters` section within the SAM template.yaml
| AWS_SECRET_REGION | string | TRUE | AWS Region that the secret resides in. Default: `us-east-1`

The SAM template also creates an EventBridge rule that executes the Lambda every 60 minutes by default. If you change the cron schedule, set `SCHEDULE_INTERVAL_MIN` to the same interval.

### How collection works

Talend's executions API filters `from`/`to` on an execution's **trigger** time only (not its start or finish time, despite the Talend docs), so querying just the last interval misses any execution that finishes after the interval it was triggered in. Instead, each run:

1. Lists every execution triggered (per task) in the last `LOOKBACK_TIME_DAYS` days (Talend `lastDays`, a rolling window of `LOOKBACK_TIME_DAYS` x 24 hours back from now, by trigger time; not calendar days).
2. Sends as a **final state** each execution whose final time falls in the finish window `[event time - SCHEDULE_INTERVAL_MIN, event time)`, together with its component stats and logs. The final time is `finishTimestamp`; an execution that ended without one (for example rejected before it started) uses its `triggerTimestamp`. The event time is the scheduled EventBridge time, floored to the minute, so consecutive windows are contiguous and every final state is sent **once**, regardless of how long the job ran or how late the run starts (see the limitations below for the exceptions).
3. When `INGEST_COMPLETE_TASKS_ONLY` is `false`, the lambda sends every execution that is still in flight (no `finishTimestamp`, status `DISPATCHING_FLOW`, `EXECUTION_EVENT_RECEIVED` or `STARTING_FLOW_EXECUTION`, or a coarse `status` of `executing` or `dispatching`) as a **snapshot**, on every run until it finishes. Snapshots have `executionDurationSec = -1` and no `finishTimestamp`, and carry no logs or component stats, so those are sent only once, with the final state.

#### Limitations/Important Notes
- An execution that runs longer than `LOOKBACK_TIME_DAYS` is never finalized (and stops getting snapshots once its trigger time leaves the lookback).
- Each final state is checked only once, by the run at the end of its finish window.
- Each task's list is capped at ~1,100 executions per run (a defensive pagination limit). Results are newest first, so above that the **oldest** executions, which are the long runners, are not checked, and a `Reached max API offset` warning is logged. See [Choosing values](#choosing-values).
- Component stats are capped at ~1,200 per execution and logs at 10,000 lines per execution (50 pages of 200). When a cap is reached, a `Reached max API offset` or `Stopped paginating` warning is logged and the newest items are kept, but the run does **not** fail, so the Lambda `Errors` metric does not fire. Alert on those log messages if you need to know.
- A failed run loses the final states of its interval when retries are disabled (`MaximumRetryAttempts: 0` - default) so that a partially posted run is not re-sent as duplicates. A run fails (raises after posting everything it could collect) on any Talend fetch error or New Relic post error, except a `404` from the component stats or logs endpoints, which counts as no data. A Lambda **timeout** is also a failed run: it stops the run part-way, so data already posted stays in New Relic and the rest of that interval is lost; with retries enabled, the retry would re-send what was already posted. To recover a lost interval, invoke the function with the original scheduled time (i.e: `aws lambda invoke --function-name NewRelicTalendPoller --cli-binary-format raw-in-base64-out --payload '{"time":"2026-10-08T14:15:00Z"}' out.json`). The interval's finals are re-sent from Talend's current data, and anything the failed run had already posted is sent again. Execution and component events are stamped with ingest time, so recovered finals appear at the time of the recovery run rather than in their original interval; `finishTimestamp` still holds the real finish time.
- Logs and component stats of a running task only arrive once it **finishes**. This includes failure/terminated states as well.
- Manual invokes (console test, `sam local invoke`) without a scheduled event use a rolling window ending now, and can re-send final states that the schedule also sends. Avoid them against a production New Relic account. A manual invoke whose payload contains `{"time": "<EventBridge time>"}` uses that scheduled window instead.

### Choosing values

**`SCHEDULE_INTERVAL_MIN`** must equal the cron interval: a smaller value may lose final states, a larger one duplicates them. The cron must fire exactly every interval, so use minutes that divide 60 (`5` with `cron(0/5 * * * ? *)`, `15`, `30`, `60` with `cron(0/60 * * * ? *)`) or whole hours that divide 24 (`120` with `cron(0 0/2 * * ? *)`). A shorter interval gives fresher data and a more current running view, but more runs (each run repeats the lookback listing). As a guide: `5` for near real-time dashboards and alerts, `15` as a balance, `60` (default) for reporting. When you change the interval on a live deployment, deploy right after a boundary of the new interval (for example just after the top of the hour when moving to 60 minutes). The first run's window reaches back one new interval from its scheduled time, so deploying mid-hour re-sends finals that the previous schedule already sent, or leaves a gap when moving to a shorter interval.

**`LOOKBACK_TIME_DAYS`** must exceed your longest task runtime, including any time queued before it starts:

| Longest task runtime | `LOOKBACK_TIME_DAYS` |
| -------------------- | -------------------- |
| Up to ~20 hours | `1` |
| Up to ~1 day | `2` (default) |
| Up to ~2 days | `3` |
| Up to ~1 week | `8` |
| Longer | up to `60` (Talend's maximum) |

A larger lookback costs more requests on every run, so lower it when tasks are short and frequent.

**Request budget-** Talend requests per run are roughly:

- Lookback listing: `tasks x ceil(executions per day per task x LOOKBACK_TIME_DAYS / 100)`, capped at about 1,100 executions (11 requests) per task.
- Per final state: 1 component stats request (more for jobs with over 200 components) plus `ceil(logs / 200)` log requests when `COLLECT_TALEND_LOGS` is `true`.

At the default `TALEND_MAX_REQUESTS_PER_SEC` of 8, a 120s run fits about 900 requests. Examples:

- 3 tasks running ~10 times a day, 60-minute interval: ~3 listing requests plus ~2 per finished execution, well under 20 per run.
- 50 tasks running hourly, 60-minute interval: 50 listing requests plus ~100 for the 50 finals, about 150 per run.
- 20 tasks running every 5 minutes (288 a day) with the default 2-day lookback: ~580 executions per task, so 6 listing requests per task (120 per run); a 3-day lookback (~860) gets close to the 1,100 cap. With a 60-minute interval, the ~240 finals add ~480 more requests. Use `LOOKBACK_TIME_DAYS: 1` (3 requests per task) and a shorter interval, so that each run has fewer finals.

If runs approach the Lambda timeout, or `Reached max API offset` warnings appear: lower `LOOKBACK_TIME_DAYS`, shorten the interval, raise `Timeout` (and `MemorySize`) in `template.yaml`, set `COLLECT_TALEND_LOGS` to `false` or raise `TALEND_LOG_LEVEL` (this filters logs after they are fetched, so it reduces ingest but not requests), or raise `TALEND_MAX_REQUESTS_PER_SEC` if you see no `Rate limited (429)` warnings.

**`INGEST_COMPLETE_TASKS_ONLY`**: use `true` when you only report on final states; use `false` (default) for a live view of running executions. With `false`, filter New Relic charts of durations or outcomes with `WHERE finishTimestamp IS NOT NULL` (or `executionDurationSec >= 0`), and use `uniqueCount(executionId)` rather than `count(*)` to count executions, because a running execution has one snapshot row per run.

**`USE_TALEND_LOG_TS`**: logs are fetched when their execution's final state is sent, up to one interval plus the run time after they were written. With `true`, logs keep the time they were written in Talend, so they appear in the past; make sure your time range starts before the execution started. New Relic may drop logs older than 48 hours, so logs older than 47 hours at collection time (jobs running for about two days or more) are stamped with the collection time instead, with a warning. With `false`, they are stamped with the collection time.

## Deployment

After completing the pre-requirements and configuration steps, the SAM CLI can be used to deploy the AWS stack.

1. Clone repo
2. Configure `src/data/tasks.json` with tasks to collect metrics/logs against
3. Configure `template.yaml` with required env variables
4. Run `sam build`
5. Run `sam deploy --guided` and follow the prompts

## Querying & Dashboarding

Metrics are stored under the eventTypes (tables) `talendTaskExecutionStats` and `talendJobComponentStats`.

Logs are found under the `Log` eventType, or under `Logs` within the NR1 UI.

In-flight snapshots (sent when `INGEST_COMPLETE_TASKS_ONLY` is `false`) have `executionDurationSec = -1` and no `finishTimestamp`. Filter them out of duration and outcome charts with `WHERE finishTimestamp IS NOT NULL`.

An example dashboard is available under [/dashboard](./dashboard/example.json) that can be imported following [these instructions](https://docs.newrelic.com/docs/query-your-data/explore-query-data/dashboards/dashboards-charts-import-export-data/#import-json).

## Local Development

1. Clone repo
2. Configure `src/data/tasks.json` with tasks to collect metrics/logs against
3. Configure `template.yaml` with required env variables
4. Run `sam build`
5. Run `sam local invoke`


## Support

<a href="https://github.com/newrelic?q=nrlabs-viz&amp;type=all&amp;language=&amp;sort="><img src="https://user-images.githubusercontent.com/1786630/214122263-7a5795f6-f4e3-4aa0-b3f5-2f27aff16098.png" height=50 /></a>

This project is actively maintained by the New Relic Labs team. Connect with us directly by [creating issues](../../issues) or [asking questions in the discussions section](../../discussions) of this repo.

We also encourage you to bring your experiences and questions to the [Explorers Hub](https://discuss.newrelic.com) where our community members collaborate on solutions and new ideas.

New Relic has open-sourced this project, which is provided AS-IS WITHOUT WARRANTY OR DEDICATED SUPPORT.

## Security

As noted in our [security policy](https://github.com/newrelic/nr-labs-pages/security/policy), New Relic is committed to the privacy and security of our customers and their data. We believe that providing coordinated disclosure by security researchers and engaging with the security community are important means to achieve our security goals.

If you believe you have found a security vulnerability in this project or any of New Relic's products or websites, we welcome and greatly appreciate you reporting it to New Relic through [HackerOne](https://hackerone.com/newrelic).

## Contributing

Contributions are welcome (and if you submit a Enhancement Request, expect to be invited to contribute it yourself :grin:). Please review our [Contributors Guide](CONTRIBUTING.md).

Keep in mind that when you submit your pull request, you'll need to sign the CLA via the click-through using CLA-Assistant. If you'd like to execute our corporate CLA, or if you have any questions, please drop us an email at opensource@newrelic.com.

## Open Source License

This project is distributed under the [Apache 2 license](LICENSE).