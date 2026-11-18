# Spark History Server MCP for Dataproc (on Compute Engine)

Every Dataproc cluster runs a Spark History Server on its master node (port `18080`).
By default, Spark event logs are written to the cluster's **temp bucket** under a
`spark-job-history` directory. You have three ways to connect the MCP server.

| Option | When to use it |
|---|---|
| A. Tunnel to the cluster's own History Server | The cluster is still running |
| B. A Persistent History Server (PHS) cluster | Logs from many clusters, or clusters that are already deleted, in one long-lived server |
| C. A local History Server against the bucket | No cluster to tunnel to, and you just want to analyze some logs |

> **Why not the Component Gateway URL?** The Component Gateway
> (`https://<id>-dot-<region>.dataproc.googleusercontent.com/sparkhistory/`) requires an
> interactive Google sign-in in a browser, so the MCP server can't call it directly. Use a
> tunnel to port 18080 instead.

## Option A: Tunnel to a running cluster

```bash
# Through IAP (no public IP / firewall changes needed; requires IAP TCP forwarding access)
gcloud compute ssh <cluster-name>-m --zone=<zone> --tunnel-through-iap \
  -- -N -L 18080:localhost:18080
```

```yaml
# config.yaml
servers:
  dataproc:
    url: "http://localhost:18080"
```

For a high-availability cluster, the master nodes are `<cluster-name>-m-0`, `-m-1`, and
`-m-2`.

## Option B: Persistent History Server cluster

A PHS is a single-node Dataproc cluster that only runs the History Server and reads
event logs from GCS. Dataproc's PHS accepts wildcards in the log directory, so one PHS
can cover many job clusters:

```bash
gcloud dataproc clusters create my-phs \
  --project=<project> --region=<region> --single-node \
  --enable-component-gateway \
  --properties='spark:spark.history.fs.logDirectory=gs://my-bucket/*/spark-job-history'
```

Make the job clusters write their logs where the PHS reads them, for example by
creating them with `--properties='spark:spark.eventLog.dir=gs://my-bucket/<cluster-name>/spark-job-history'`
(see the [Dataproc PHS docs](https://cloud.google.com/dataproc/docs/concepts/jobs/history-server)).
Then tunnel to the PHS master as in option A (`gcloud compute ssh my-phs-m ...`).

## Option C: Local History Server against the bucket

Find the cluster's event-log directory. If `spark:spark.eventLog.dir` was set explicitly,
it's in the cluster properties:

```bash
gcloud dataproc clusters describe <cluster-name> --region=<region> \
  --format='value(config.softwareConfig.properties."spark:spark.eventLog.dir")'
```

Otherwise it's under the cluster's temp bucket. List it to find the `spark-job-history`
directory:

```bash
gcloud dataproc clusters describe <cluster-name> --region=<region> \
  --format='value(config.tempBucket,clusterUuid)'
gcloud storage ls gs://<temp-bucket>/<cluster-uuid>/
```

Then start a local History Server against that directory:

```bash
gcloud auth application-default login
./examples/cloud/start_cloud_spark_history.sh \
  --event-dir=gs://<temp-bucket>/<cluster-uuid>/spark-job-history
```

The open-source History Server used by the script doesn't expand wildcards, so it reads
one cluster's directory at a time. Start one per cluster on different `--port` values
if you need several. See [examples/cloud](../../cloud/README.md) for flags and
troubleshooting.

The temp bucket has a default 90-day lifecycle rule, so copy event logs you want to keep
to another bucket, or set `spark:spark.eventLog.dir` to a bucket you manage.
