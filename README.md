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
  * [Talend Tasks](./data/tasks.json)
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

The lambda function fetches and caches an AWS secret to that contains several required inputs in order to poll Talend and forward data to New Relic, including:

* Talend Service Account secret/id OR Talend User PAT **NOTE: only one of these are required, not both**
* [New Relic ingest key](https://docs.newrelic.com/docs/apis/intro-apis/new-relic-api-keys/#license-key)
* New Relic account id

Example secrets can be found under [examples_secrets/](./example_secrets/), depending on if you wish to authenticate via Talend Service Account or User PAT. To create the secret via AWS CLI, run (as an example):

```bash
aws secretsmanager create-secret \
    --name "talend/newrelic-integration" \
    --description "Secrets for the Talend to New Relic monitoring lambda" \
    --secret-string file://secrets.json \
    --region "us-east-1"
```


## Configuration

A list of tasks must be input within [tasks.json](./data/tasks.json) before deploying. Each object must contain a task id to fetch executions for, and an associated name or alias for each task id.

The following are available function environment variables that can be configured under the SAM `template.yaml`, or within AWS itself after the function is deployed.

| Input | Type | Required | Description
| ----- | ---- | -------- | -----------
| TALEND_REGION | string | FALSE | Talend region where tasks are running. Can be one of: _US|EU|AP|AU|US-WEST_. Default: `US`
| NR_REGION | enum | FALSE | Region of New Relic account. Can be one of _US|EU_. Default: `US`
| DEBUG_LOGGING | string | FALSE | Enables lambda debug logging for troubleshooting. Can be one of _true|false_. Default: `false`
| COLLECT_TALEND_LOGS | string | FALSE | When `true`, collects all task execution logs. Can be one of _true|false_. Default: `true`
| TALEND_LOG_LEVEL | string | FALSE | The minimum log level of Talend execution logs to forward to NR. Can be one of _TRACE|DEBUG|INFO|WARN|ERROR|FATAL_. Default: `WARN`
| LOOKBACK_TIME_MIN | int | FALSE | Amount of time to lookback for task executions, in minutes. Default: `60`
| AWS_SECRET_ID | string | TRUE | ARN of AWS Secret containing required Talend/NR keys and secrets. Configured under the `Parameters` section within the SAM template.yaml
| AWS_SECRET_REGION | string | TRUE | AWS Region that the secret resides in. Default: `us-east-1`

The SAM template also creates an EventBridge rule that executes the Lambda every 60 minutes by default. Configure the cron schedule accordingly based on how often your Talend tasks execute. 

**NOTE that the `LOOKBACK_TIME_MIN` should match the configured cron frequency. For example, the default `60` value in both means that the lambda will execute every 60 minutes, and poll the previous 60 minutes worth of task execution metrics/logs.**


## Deployment

After completing the pre-requirements and configuration steps, the SAM CLI can be used to deploy the AWS stack.

1. Clone repo
2. Configure `data/tasks.json` with tasks to collect metrics/logs against
3. Configure `template.yaml` with required env variables
4. Run `sam build`
5. Run `sam deploy --guided` and follow the prompts

## Querying & Dashboarding

Metrics are stored under the eventTypes (tables) `talendTaskExecutionStats` and `talendJobComponentStats`.

Logs are found under the `Log` eventType, or under `Logs` within the NR1 UI.

An example dashboard is available under [/dashboard](./dashboard/example.json) that can be imported following [these instructions](https://docs.newrelic.com/docs/query-your-data/explore-query-data/dashboards/dashboards-charts-import-export-data/#import-json).

## Local Development

1. Clone repo
2. Configure `data/tasks.json` with tasks to collect metrics/logs against
3. Configure `template.yaml` with required env variables
4. Run `sam build`
5. Run `sam local invoke`


## Contributing

We encourage your contributions to improve talend-observability! Keep in mind when you submit your pull request, you'll need to sign the CLA via the click-through using CLA-Assistant. You only have to sign the CLA one time per project. If you have any questions, or to execute our corporate CLA, required if your contribution is on behalf of a company, please drop us an email at opensource@newrelic.com.

**A note about vulnerabilities**

As noted in our [security policy](../../security/policy), New Relic is committed to the privacy and security of our customers and their data. We believe that providing coordinated disclosure by security researchers and engaging with the security community are important means to achieve our security goals.

If you believe you have found a security vulnerability in this project or any of New Relic's products or websites, we welcome and greatly appreciate you reporting it to New Relic through [HackerOne](https://hackerone.com/newrelic).

## License

talend-observability is licensed under the [Apache 2.0](http://apache.org/licenses/LICENSE-2.0.txt) License.