# Spark History Server MCP for Spark on GKE

Spark on GKE (usually through the [Kubeflow Spark Operator](https://github.com/kubeflow/spark-operator))
has no managed History Server. Event logs end up wherever each job's `spark.eventLog.dir`
points, which is normally a GCS bucket. You can either connect the MCP server to a
History Server already running in the cluster, or start one locally against the bucket.

## Step 1: Make sure your jobs write event logs

Event logging is off by default in Spark. Turn it on in each `SparkApplication`
(or in a shared `spark-defaults.conf` in your image):

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
spec:
  sparkConf:
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: "gs://my-bucket/spark-events"
    # Optional: makes long-running apps readable before they finish
    spark.eventLog.rolling.enabled: "true"
```

The driver image needs the GCS connector, and the driver's Kubernetes service account
needs write access to the bucket, typically through
[Workload Identity Federation for GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity).

To find where an existing job writes its logs:

```bash
kubectl get sparkapplication <name> -n <namespace> \
  -o jsonpath='{.spec.sparkConf.spark\.eventLog\.dir}'
```

## Step 2a: Use an in-cluster History Server, if you have one

```bash
kubectl port-forward -n <namespace> svc/<spark-history-server-svc> 18080:18080
```

```yaml
# config.yaml
servers:
  gke:
    url: "http://localhost:18080"
```

If you deploy the MCP server into the same cluster with the
[Helm chart](../../../deploy/kubernetes/helm/README.md), set its server URL to the
History Server's in-cluster Service DNS name instead (e.g.
`http://spark-history-server.<namespace>.svc.cluster.local:18080`). You won't need
a port-forward.

## Step 2b: Or start a local History Server against the bucket

```bash
gcloud auth application-default login
./examples/cloud/start_cloud_spark_history.sh --event-dir=gs://my-bucket/spark-events
```

See [examples/cloud](../../cloud/README.md) for flags, running several servers side by
side, and troubleshooting.

## Step 3: Start the MCP server

```bash
task start-mcp-bg
```
