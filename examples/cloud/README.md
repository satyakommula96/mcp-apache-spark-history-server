# Spark History Server MCP for event logs in GCS or S3

The MCP server talks to a Spark History Server's REST API, so it works with any platform
as long as a History Server can read that platform's event logs. If your event logs
live in Google Cloud Storage or Amazon S3 and you don't already have a History Server
running against them, `start_cloud_spark_history.sh` starts one locally in Docker,
pointed directly at the bucket. You don't need to download the logs first.

Platform-specific guides (where each platform writes its event logs, and alternatives
to this script):

| Platform | Storage | Guide |
|---|---|---|
| GKE (spark-operator) | `gs://` | [examples/gcp/gke](../gcp/gke/README.md) |
| Dataproc on Compute Engine | `gs://` | [examples/gcp/dataproc](../gcp/dataproc/README.md) |
| Dataproc Serverless (Serverless for Apache Spark) | `gs://` | [examples/gcp/dataproc-serverless](../gcp/dataproc-serverless/README.md) |
| EKS (spark-operator, EMR on EKS) | `s3://` | [examples/aws/eks](../aws/eks/README.md) |
| EMR on EC2 | managed | [examples/aws/emr](../aws/emr/README.md) (built-in Persistent UI support) |
| AWS Glue | `s3://` | [examples/aws/glue](../aws/glue/README.md) |

## Step 1: Set up the project

```bash
git clone https://github.com/kubeflow/mcp-apache-spark-history-server.git
cd mcp-apache-spark-history-server
task install
```

## Step 2: Authenticate

The script only reads from the bucket. It never writes to it.

**GCS**: Application Default Credentials, or a service-account key file:

```bash
gcloud auth application-default login
# or: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

**S3**: credentials in the environment, or an AWS CLI v2 profile (SSO and assumed-role
profiles work; the script exports temporary credentials with
`aws configure export-credentials`):

```bash
aws sso login --profile my-profile        # if it's an SSO profile
# or: export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... [AWS_SESSION_TOKEN=...]
```

## Step 3: Start a History Server against the bucket

```bash
# GCS
./examples/cloud/start_cloud_spark_history.sh --event-dir=gs://my-bucket/spark-logs

# S3 (s3:// is rewritten to s3a://, which is what open-source Hadoop reads)
./examples/cloud/start_cloud_spark_history.sh --event-dir=s3://my-bucket/spark-logs \
  --profile=my-profile --region=us-west-2
```

On the first run the script downloads the matching storage connector into
`examples/cloud/.cache/` (gitignored):

- **GCS**: the GCS Hadoop connector.
- **S3**: `hadoop-aws` and the AWS SDK bundle. The script reads the Hadoop version from
  the chosen `apache/spark` image and the SDK version from that Hadoop release's POM,
  so any `--spark-version` works. The SDK bundle is large (~280 MB for Hadoop 3.3.x).

Useful flags:

| Flag | Default | Notes |
|---|---|---|
| `--port` | `18080` | Use different ports to run several servers side by side |
| `--container-name` | `spark-history-server-<gcs\|s3>` | Must be unique per server |
| `--spark-version` | `3.5.5` | Use a version at least as new as the Spark that wrote the logs |
| `--project` | `gcloud config get-value project` | GCS only, used for billing/quota |
| `--profile` / `--region` | `AWS_PROFILE` / `AWS_REGION` / `aws configure get region` | S3 only |
| `--conf=KEY=VALUE` | | Extra History Server property; repeatable |
| `--dry-run` | | Checks Docker and credentials and downloads the connector jars, without starting the server |

## Step 4: Point the MCP server at it

Edit [config.yaml](../../config.yaml). You can run one History Server per platform
and compare runs across them, for example Dataproc against GKE:

```yaml
servers:
  dataproc:
    url: "http://localhost:18080"
  gke:
    url: "http://localhost:18081"
```

## Step 5: Start the MCP server and query

```bash
task start-mcp-bg
```

Then ask your agent things like *"list the Spark applications on the gke server"* or
*"compare query q67 between app A on dataproc and app B on gke"*.

## Troubleshooting

- **Container exits right after starting.** The History Server stops if it can't read
  the log directory. Run `docker logs <container-name>` *while it starts* (the container
  is started with `--rm`). An `AccessDeniedException` / `403` means a credentials or
  bucket-permission problem; `FileNotFoundException` means the path is wrong.
- **S3 works for about an hour, then fails.** Temporary (SSO/STS) credentials have
  expired. Re-run the script to refresh them.
- **Wildcards (`gs://bucket/*/spark-job-history`).** The open-source History Server
  doesn't expand wildcards (Dataproc's Persistent History Server adds that). Point at
  one directory, or start one server per directory on different ports.
- **The first application listing is slow.** `FsHistoryProvider` reads every event log
  before listing applications, which can take a minute or more on large buckets.
- **Stop:** `docker stop <container-name>`.

This is for local analysis, in the same spirit as
[`start_local_spark_history.sh`](../../start_local_spark_history.sh). It isn't a
production History Server deployment. For production, run
`org.apache.spark.deploy.history.HistoryServer` on your own infrastructure with the
same `spark.hadoop.fs.gs.*` / `spark.hadoop.fs.s3a.*` properties.
