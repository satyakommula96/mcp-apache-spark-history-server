# Spark History Server MCP for AWS Glue

📺 **See it in action:**

[![Watch the demo video](https://img.shields.io/badge/YouTube-Watch%20Demo-red?style=for-the-badge&logo=youtube)](https://www.youtube.com/watch?v=FaduuvMdGxI)

AWS Glue doesn't expose a Spark History Server REST API, and the Spark UI in the Glue
console can't be queried by the MCP server. Instead, have your Glue jobs write Spark
event logs to S3, run a Spark History Server against that S3 path, and point the MCP
server at it.

| Option | When to use it |
|---|---|
| [A. Local History Server](#option-a-local-history-server-docker) | Quick analysis from your laptop; nothing to deploy in AWS |
| [B. History Server on EC2](#option-b-history-server-on-ec2-cloudformation) | A long-lived server your team shares |

## Step 1: Set up the project

```bash
git clone https://github.com/kubeflow/mcp-apache-spark-history-server.git
cd mcp-apache-spark-history-server

# Install Task (if not already installed)
brew install go-task  # macOS, see https://taskfile.dev/installation/ for others
task install
```

## Step 2: Make sure your Glue jobs write event logs

Jobs created in the Glue console have the Spark UI turned on by default, but check that
they write logs **in the format a Spark History Server can read**.

**Console**: open the job, then **Job details → Advanced properties → Spark UI**:

- Turn on **Write Spark UI logs to Amazon S3** and choose an S3 path.
- Under **Spark UI logging and monitoring configuration**, select **Legacy**, which is the
  format for a Spark History Server. **Standard** is only for viewing in the Glue console.
  You can select both.

**CLI / job parameters**:

```text
--enable-spark-ui              true
--spark-event-logs-path        s3://my-bucket/glue-spark-events/
--enable-spark-ui-legacy-path  true
```

**Interactive sessions and notebooks**: set this before the session starts. The Glue
console doesn't show a Spark UI for sessions, so a History Server is the only way to
view them:

```text
%%configure
{ "--enable-spark-ui": "true", "--spark-event-logs-path": "s3://my-bucket/glue-spark-events/" }
```

**Long-running or streaming jobs**: turn on rolling event logs with
`--conf spark.eventLog.rolling.enabled=true`. According to the Glue docs, the Spark UI
supports event logs up to 0.5 GB without rolling, and up to 2 GB in total with it.

Glue writes event logs to S3 every 30 seconds, so a running job shows up in the History
Server shortly after it starts.

To find where an existing job writes its logs:

```bash
aws glue get-job --job-name <job-name> \
  --query 'Job.DefaultArguments."--spark-event-logs-path"' --output text
# A run can override it:
aws glue get-job-run --job-name <job-name> --run-id <jr_...> \
  --query 'JobRun.Arguments."--spark-event-logs-path"' --output text
```

## Step 3: Run a Spark History Server against the event logs

### Option A: Local History Server (Docker)

[`start_cloud_spark_history.sh`](../../cloud/README.md) starts an Apache Spark History
Server in Docker, reading directly from S3. It downloads the matching S3 connector jars
and takes credentials from your environment or an AWS CLI profile (including SSO):

```bash
aws sso login --profile my-profile   # or export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
./examples/cloud/start_cloud_spark_history.sh \
  --event-dir=s3://my-bucket/glue-spark-events/ \
  --profile=my-profile --region=us-east-1
```

The default `--spark-version` (3.5.5) can read event logs from every current Glue
version (Glue 5.0 runs Spark 3.5, Glue 4.0 runs Spark 3.3).

Then add it to [config.yaml](../../../config.yaml):

```yaml
servers:
  glue:
    url: "http://localhost:18080"
```

See [examples/cloud](../../cloud/README.md) for flags and troubleshooting. If the
container exits right away, run `docker logs spark-history-server-s3`; a `403` usually
means your credentials can't read the bucket.

AWS also publishes a [Docker sample](https://github.com/aws-samples/aws-glue-samples/tree/master/utilities/Spark_UI/)
that you build yourself. Either works with the MCP server.

### Option B: History Server on EC2 (CloudFormation)

Launch the History Server with the
[CloudFormation template from the Glue docs](https://docs.aws.amazon.com/glue/latest/dg/monitor-spark-ui-history.html#monitor-spark-ui-history-cfn).
When you fill in the stack parameters:

- **Event log directory**: the path from Step 2, using `s3a://` instead of `s3://`.
- **IP address range**: include the address of the machine that runs the MCP server.
- **Subnet**: if it's private, the MCP server needs network access to it (VPN, peering,
  or a bastion; see [the EMR guide](../emr/README.md) for an SSH-tunnel example).

When the stack is ready, copy **SparkUiPublicUrl** (public subnet) or
**SparkUiPrivateUrl** (private subnet) from the **Outputs** tab, and make sure it opens in
a browser.

The server uses HTTPS. Choose the config that matches its certificate:

```yaml
servers:
  # You provided a keystore with a certificate issued by your own CA
  glue_ec2:
    url: "https://<host>:<port>"
    ssl_ca_cert: "/path/to/your-ca-bundle.pem"

  # You left the keystore empty, so the template generated a self-signed certificate.
  # The certificate can't be verified, so verification has to be turned off.
  # Only do this on a network you trust.
  # glue_ec2:
  #   url: "https://<host>:<port>"
  #   verify_ssl: false
```

## Step 4: Start the MCP server

```bash
task start-mcp-bg
```

## Step 5: Ask your agent

Connect your agent as described in [Connect an AI Agent](../../../README.md#connect-an-ai-agent)
([Claude Desktop](../../integrations/claude-desktop/README.md),
[Kiro](../../integrations/kiro/README.md), and others), then ask things like
*"list the Glue applications on the glue server"* or *"why was yesterday's run of my
job slower than today's?"*.
