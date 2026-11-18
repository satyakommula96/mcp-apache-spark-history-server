# Spark History Server MCP for Dataproc Serverless (Serverless for Apache Spark)

Serverless batches and sessions have no cluster to tunnel to. The Spark UI shown in the
Cloud console isn't a History Server REST endpoint the MCP server can call. To analyze
serverless workloads, make sure their event logs land in a GCS location you control,
then put a History Server in front of that location.

## Option A: Attach a Persistent History Server (PHS) cluster

Create a PHS once (see [examples/gcp/dataproc](../dataproc/README.md#option-b-persistent-history-server-cluster)),
then attach it when you submit batches. Dataproc sets the batch's event-log directory
to match the PHS:

```bash
gcloud dataproc batches submit spark \
  --region=<region> \
  --history-server-cluster=projects/<project>/regions/<region>/clusters/my-phs \
  ... # your usual batch arguments
```

Then tunnel to the PHS master and point the MCP server at it:

```bash
gcloud compute ssh my-phs-m --zone=<zone> --tunnel-through-iap -- -N -L 18080:localhost:18080
```

```yaml
# config.yaml
servers:
  dataproc_serverless:
    url: "http://localhost:18080"
```

The PHS must be in the same region as the batches.

## Option B: Write event logs to your own bucket and run a local History Server

If you don't want a PHS cluster, set the event-log directory on each batch yourself:

```bash
gcloud dataproc batches submit spark \
  --region=<region> \
  --properties='spark.eventLog.enabled=true,spark.eventLog.dir=gs://my-bucket/serverless-events' \
  ... # your usual batch arguments
```

Then start a local History Server against that directory:

```bash
gcloud auth application-default login
./examples/cloud/start_cloud_spark_history.sh --event-dir=gs://my-bucket/serverless-events
```

The batch's service account needs write access to the bucket. See
[examples/cloud](../../cloud/README.md) for flags and troubleshooting.

## Batches that have already run

If a batch ran with neither option, there may be no event logs you can reach. Check the
batch's runtime properties for an event-log directory:

```bash
gcloud dataproc batches describe <batch-id> --region=<region> \
  --format='yaml(runtimeConfig.properties,environmentConfig.peripheralsConfig)'
```

If `spark.eventLog.dir` or a `sparkHistoryServerConfig` appears there, use that location
with option A or B. Otherwise, use option A or B for future runs.
