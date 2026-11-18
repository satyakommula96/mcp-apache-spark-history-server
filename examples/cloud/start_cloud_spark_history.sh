#!/bin/bash
set -e

# Help function
show_help() {
    cat << EOF
Starting a Spark History Server directly against cloud-storage event logs
==========================================================================

USAGE:
    ./start_cloud_spark_history.sh --event-dir=<gs://|s3://|s3a://>bucket/path [OPTIONS]

OPTIONS:
    -h, --help                 Show this help message
    --event-dir=PATH           gs:// or s3:// / s3a:// path containing Spark event logs (required)
    --spark-version=VERSION    Spark version, must be a published apache/spark image tag (default: 3.5.5)
    --container-name=NAME      Docker container name (default: spark-history-server-<gcs|s3>)
    --port=PORT                Port to expose (default: 18080)
    --conf=KEY=VALUE           Extra History Server property; repeatable
                               (e.g. --conf=spark.history.fs.update.interval=30s)
    --dry-run                  Validate prerequisites without starting the server

  GCS (gs://) options:
    --project=PROJECT_ID       GCP project used for GCS billing/quota
                               (default: "gcloud config get-value project")

  S3 (s3:// / s3a://) options:
    --profile=NAME             AWS CLI profile to export credentials from (default: AWS_PROFILE / default)
    --region=REGION            Bucket region (default: AWS_REGION, then "aws configure get region")

DESCRIPTION:
    Starts a local Spark History Server (via Docker) with its
    spark.history.fs.logDirectory pointed directly at a cloud-storage path, so
    the spark-history-mcp tools can query event logs from GKE, Dataproc,
    Dataproc Serverless (gs://), or EKS / EMR on EKS (s3://) without
    downloading them first. The matching storage connector jars are
    downloaded once and cached under .cache/ next to this script.

    Credentials are read-only and never written to the bucket or the repo:
      gs://  Application Default Credentials. Uses GOOGLE_APPLICATION_CREDENTIALS
             if set, otherwise the gcloud ADC file. The file is copied into a
             local, gitignored run directory (the container runs as a non-root
             user and can't read ~/.config/gcloud), overwritten on every run.
      s3://  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN from
             the environment if set; otherwise exported from the AWS CLI
             ("aws configure export-credentials"), which covers SSO and
             assumed-role profiles. They're passed to the container as
             environment variables only. Temporary credentials expire (often
             after 1h); re-run the script to refresh them.

PREREQUISITES:
    - Docker running
    - gs://  gcloud ADC (gcloud auth application-default login) or a service-account key
    - s3://  AWS credentials in the environment, or AWS CLI v2 with a configured profile
    - Read access to the event-dir bucket

EXAMPLES:
    ./start_cloud_spark_history.sh --event-dir=gs://my-bucket/spark-logs
    ./start_cloud_spark_history.sh --event-dir=s3://my-bucket/spark-logs --region=us-west-2
    ./start_cloud_spark_history.sh --event-dir=s3://my-bucket/logs --profile=prod --port=18081
    ./start_cloud_spark_history.sh --event-dir=gs://my-bucket/spark-logs --dry-run

Once it's up, point spark-history-mcp at it in config.yaml:
    servers:
      cloud:
        url: "http://localhost:18080"
EOF
}

DRY_RUN=false
SPARK_VERSION="3.5.5"
CONTAINER_NAME=""
PORT="18080"
EVENT_DIR=""
PROJECT_ID=""
AWS_CLI_PROFILE="${AWS_PROFILE:-}"
REGION=""
EXTRA_CONF=()

for arg in "$@"; do
    case $arg in
        -h|--help) show_help; exit 0 ;;
        --dry-run) DRY_RUN=true ;;
        --event-dir=*) EVENT_DIR="${arg#*=}" ;;
        --spark-version=*) SPARK_VERSION="${arg#*=}" ;;
        --container-name=*) CONTAINER_NAME="${arg#*=}" ;;
        --port=*) PORT="${arg#*=}" ;;
        --project=*) PROJECT_ID="${arg#*=}" ;;
        --profile=*) AWS_CLI_PROFILE="${arg#*=}" ;;
        --region=*) REGION="${arg#*=}" ;;
        --conf=*) EXTRA_CONF+=("${arg#*=}") ;;
        *)
            echo "Unknown option: $arg"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

echo "Starting Spark History Server against cloud-storage event logs"
echo "================================================================"

if [ -z "$EVENT_DIR" ]; then
    echo "Error: --event-dir is required (gs://bucket/path or s3://bucket/path)."
    echo "Use --help for usage information."
    exit 1
fi

case "$EVENT_DIR" in
    gs://*) STORAGE="gcs" ;;
    s3a://*) STORAGE="s3" ;;
    s3://*|s3n://*)
        # Event logs written by EMR / EKS jobs are usually referenced as s3://,
        # but open-source Hadoop only reads S3 through the s3a:// connector.
        STORAGE="s3"
        EVENT_DIR="s3a://${EVENT_DIR#*://}"
        ;;
    *) echo "Error: --event-dir must start with gs://, s3:// or s3a://"; exit 1 ;;
esac

case "$EVENT_DIR" in
    *\**|*\?*)
        echo "Error: wildcards in --event-dir are not supported by the open-source History Server"
        echo "   (Dataproc's Persistent History Server adds them). Point at a single directory,"
        echo "   e.g. gs://bucket/<cluster-uuid>/spark-job-history, or start one server per directory."
        exit 1
        ;;
esac

CONTAINER_NAME="${CONTAINER_NAME:-spark-history-server-$STORAGE}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_CACHE_DIR="$SCRIPT_DIR/.cache/$STORAGE"
MAVEN_REPO="https://repo1.maven.org/maven2"
IMAGE="docker.io/apache/spark:$SPARK_VERSION"

check_docker() {
    if ! docker info >/dev/null 2>&1; then
        echo "Error: Docker is not running. Please start Docker first."
        exit 1
    fi
}

download() {
    # download <url> <dest>: fetch to a .part file and rename, so an
    # interrupted download never leaves a truncated jar in the cache.
    if [ ! -f "$2" ]; then
        echo "   Downloading $(basename "$2") (first run only)..."
        curl -fsSL -o "$2.part" "$1"
        mv "$2.part" "$2"
    fi
}

# ---------------------------------------------------------------- GCS setup
GCS_ADC_FILE=""

check_gcs_credentials() {
    GCS_ADC_FILE="${GOOGLE_APPLICATION_CREDENTIALS:-$HOME/.config/gcloud/application_default_credentials.json}"
    if [ ! -f "$GCS_ADC_FILE" ]; then
        echo "Error: No Google credentials found at $GCS_ADC_FILE"
        echo "   Run: gcloud auth application-default login"
        echo "   or set GOOGLE_APPLICATION_CREDENTIALS to a service-account key file."
        exit 1
    fi
    if [ -z "$PROJECT_ID" ]; then
        PROJECT_ID=$(gcloud config get-value project 2>/dev/null || true)
    fi
    if [ -z "$PROJECT_ID" ]; then
        echo "Error: could not determine a GCP project id. Pass --project=<id>."
        exit 1
    fi
}

fetch_gcs_connector() {
    mkdir -p "$JAR_CACHE_DIR"
    download "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar" \
        "$JAR_CACHE_DIR/gcs-connector-hadoop3-latest.jar"
}

# ----------------------------------------------------------------- S3 setup
check_s3_credentials() {
    if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
        if ! command -v aws >/dev/null 2>&1; then
            echo "Error: no AWS credentials in the environment and the AWS CLI is not installed."
            echo "   Export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (and AWS_SESSION_TOKEN),"
            echo "   or install AWS CLI v2 and configure a profile."
            exit 1
        fi
        local profile_args=()
        [ -n "$AWS_CLI_PROFILE" ] && profile_args=(--profile "$AWS_CLI_PROFILE")
        local exported
        if ! exported=$(aws configure export-credentials "${profile_args[@]}" --format env-no-export 2>&1); then
            echo "Error: could not export AWS credentials${AWS_CLI_PROFILE:+ for profile $AWS_CLI_PROFILE}:"
            echo "   $exported"
            echo "   For SSO profiles, run: aws sso login${AWS_CLI_PROFILE:+ --profile $AWS_CLI_PROFILE}"
            exit 1
        fi
        # Only accept the three expected variables; never eval CLI output.
        while IFS='=' read -r key value; do
            case "$key" in
                AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN) export "$key=$value" ;;
            esac
        done <<< "$exported"
    fi
    if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
        echo "Error: AWS credentials are incomplete (need AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)."
        exit 1
    fi

    if [ -z "$REGION" ]; then
        REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
    fi
    if [ -z "$REGION" ] && command -v aws >/dev/null 2>&1; then
        local profile_args=()
        [ -n "$AWS_CLI_PROFILE" ] && profile_args=(--profile "$AWS_CLI_PROFILE")
        REGION=$(aws configure get region "${profile_args[@]}" 2>/dev/null || true)
    fi
    if [ -z "$REGION" ]; then
        echo "Error: could not determine the bucket's AWS region. Pass --region=<region>."
        exit 1
    fi
}

fetch_s3_connector() {
    # hadoop-aws must exactly match the Hadoop version bundled in the Spark
    # image, and the AWS SDK must match what that Hadoop release was built
    # against. Read both from the image and Hadoop's own published POMs rather
    # than hardcoding a table, so any apache/spark tag works.
    echo "   Detecting the Hadoop version bundled in $IMAGE..."
    docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" >/dev/null
    local hadoop_version
    hadoop_version=$(docker run --rm --entrypoint ls "$IMAGE" /opt/spark/jars \
        | sed -n 's/^hadoop-client-api-\(.*\)\.jar$/\1/p' | head -1)
    if [ -z "$hadoop_version" ]; then
        echo "Error: could not find hadoop-client-api in $IMAGE:/opt/spark/jars."
        exit 1
    fi

    JAR_CACHE_DIR="$JAR_CACHE_DIR/hadoop-$hadoop_version"
    mkdir -p "$JAR_CACHE_DIR"
    download "$MAVEN_REPO/org/apache/hadoop/hadoop-aws/$hadoop_version/hadoop-aws-$hadoop_version.jar" \
        "$JAR_CACHE_DIR/hadoop-aws-$hadoop_version.jar"

    # Already resolved on a previous run?
    if ls "$JAR_CACHE_DIR"/aws-java-sdk-bundle-*.jar "$JAR_CACHE_DIR"/bundle-*.jar >/dev/null 2>&1; then
        return
    fi

    local aws_pom project_pom sdk_version
    aws_pom=$(curl -fsSL "$MAVEN_REPO/org/apache/hadoop/hadoop-aws/$hadoop_version/hadoop-aws-$hadoop_version.pom")
    project_pom=$(curl -fsSL "$MAVEN_REPO/org/apache/hadoop/hadoop-project/$hadoop_version/hadoop-project-$hadoop_version.pom")
    if grep -q '<groupId>software.amazon.awssdk</groupId>' <<< "$aws_pom"; then
        # Hadoop 3.4+: AWS SDK v2
        sdk_version=$(sed -n 's:.*<aws-java-sdk-v2.version>\(.*\)</aws-java-sdk-v2.version>.*:\1:p' <<< "$project_pom" | head -1)
        [ -n "$sdk_version" ] || { echo "Error: could not resolve the AWS SDK v2 version for Hadoop $hadoop_version."; exit 1; }
        download "$MAVEN_REPO/software/amazon/awssdk/bundle/$sdk_version/bundle-$sdk_version.jar" \
            "$JAR_CACHE_DIR/bundle-$sdk_version.jar"
    else
        # Hadoop 3.3.x: AWS SDK v1
        sdk_version=$(sed -n 's:.*<aws-java-sdk.version>\(.*\)</aws-java-sdk.version>.*:\1:p' <<< "$project_pom" | head -1)
        [ -n "$sdk_version" ] || { echo "Error: could not resolve the AWS SDK version for Hadoop $hadoop_version."; exit 1; }
        download "$MAVEN_REPO/com/amazonaws/aws-java-sdk-bundle/$sdk_version/aws-java-sdk-bundle-$sdk_version.jar" \
            "$JAR_CACHE_DIR/aws-java-sdk-bundle-$sdk_version.jar"
    fi
}

# ------------------------------------------------------------------ prepare
echo "Checking prerequisites..."
check_docker
if [ "$STORAGE" = "gcs" ]; then
    check_gcs_credentials
    fetch_gcs_connector
else
    check_s3_credentials
    fetch_s3_connector
fi

# A fixed, per-container run dir rather than a mktemp+EXIT-trap dir: the
# container runs detached (-d) and keeps reading these files after this
# script exits, so they can't be deleted on exit. Wiped and recreated at the
# start of every run instead, so nothing accumulates across invocations.
CONF_DIR="$SCRIPT_DIR/run-$CONTAINER_NAME"
rm -rf "$CONF_DIR"
mkdir -p "$CONF_DIR"
chmod 755 "$CONF_DIR"

DOCKER_ENV=()
{
    echo "spark.history.fs.logDirectory=$EVENT_DIR"
    echo "spark.history.ui.port=18080"
    if [ "$STORAGE" = "gcs" ]; then
        echo "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        echo "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        echo "spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT"
        echo "spark.hadoop.fs.gs.project.id=$PROJECT_ID"
    else
        echo "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
        echo "spark.hadoop.fs.s3a.endpoint.region=$REGION"
        # No credentials.provider override: S3A's default chain already reads
        # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN.
    fi
    for kv in "${EXTRA_CONF[@]}"; do
        echo "$kv"
    done
} > "$CONF_DIR/history-server.conf"
chmod 644 "$CONF_DIR/history-server.conf"

if [ "$STORAGE" = "gcs" ]; then
    # The container's non-root user generally cannot traverse into
    # ~/.config/gcloud (mode 700) or read the ADC file (mode 600), so the
    # credential is copied into this run dir instead of bind-mounting the
    # host's gcloud config directory. Not committed (see .gitignore).
    cp "$GCS_ADC_FILE" "$CONF_DIR/adc.json"
    chmod 644 "$CONF_DIR/adc.json"
    DOCKER_ENV+=(-e GOOGLE_APPLICATION_CREDENTIALS=/mnt/data/adc.json)
else
    # Credentials go to the container by name only (-e VAR), so the values
    # never appear in the docker command line or on disk.
    DOCKER_ENV+=(-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION="$REGION")
    [ -n "$AWS_SESSION_TOKEN" ] && DOCKER_ENV+=(-e AWS_SESSION_TOKEN)
fi

echo ""
echo "Configuration:"
echo "   Event dir:  $EVENT_DIR"
if [ "$STORAGE" = "gcs" ]; then
    echo "   Project:    $PROJECT_ID"
else
    echo "   Region:     $REGION"
fi
echo "   Port:       $PORT"
echo "   Container:  $CONTAINER_NAME"
echo "   Image:      $IMAGE"

if [ "$DRY_RUN" = true ]; then
    echo ""
    echo "Dry run complete. All prerequisites are met."
    exit 0
fi

echo ""
echo "Stopping any existing container named $CONTAINER_NAME..."
docker stop "$CONTAINER_NAME" >/dev/null 2>&1 && echo "   Stopped existing container" || echo "   No existing container found"

echo ""
echo "Starting Docker container..."
docker run -d \
  --name "$CONTAINER_NAME" \
  --label "mcp-spark-test=true" \
  --rm \
  -v "$CONF_DIR:/mnt/data:ro" \
  -v "$JAR_CACHE_DIR:/mnt/extra-jars:ro" \
  "${DOCKER_ENV[@]}" \
  -p "$PORT:18080" \
  "$IMAGE" \
  /opt/java/openjdk/bin/java \
  -cp '/opt/spark/conf:/opt/spark/jars/*:/mnt/extra-jars/*' \
  -Xmx4g \
  org.apache.spark.deploy.history.HistoryServer \
  --properties-file /mnt/data/history-server.conf

echo ""
echo "Spark History Server started in the background."
echo "   Web UI / API: http://localhost:$PORT"
echo "   Logs:         docker logs -f $CONTAINER_NAME"
echo "   Stop:         docker stop $CONTAINER_NAME"
echo "   It may take 30-60s to finish listing applications on first load."
