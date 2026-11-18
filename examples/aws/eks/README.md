# Spark History Server MCP for Spark on EKS

This covers Spark on Amazon EKS run either with the
[Kubeflow Spark Operator](https://github.com/kubeflow/spark-operator) or with
[EMR on EKS](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/emr-eks.html).
In both cases, event logs go wherever the job's `spark.eventLog.dir` points, normally an
S3 bucket. You can either connect the MCP server to a History Server already running in
the cluster, or start one locally against the bucket.

(For EMR on EC2, use the built-in [EMR Persistent UI support](../emr/README.md) instead.)

## Step 1: Make sure your jobs write event logs to S3

**Spark Operator**: in each `SparkApplication`:

```yaml
spec:
  sparkConf:
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: "s3a://my-bucket/spark-events"
    spark.eventLog.rolling.enabled: "true"   # optional
```

The image needs `hadoop-aws` and the AWS SDK, and the driver's service account needs
write access to the bucket, typically through
[IAM roles for service accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
or EKS Pod Identity.

**EMR on EKS**: set it in the job's `spark-defaults` configuration override:

```json
{
  "applicationConfiguration": [{
    "classification": "spark-defaults",
    "properties": {
      "spark.eventLog.enabled": "true",
      "spark.eventLog.dir": "s3://my-bucket/spark-events"
    }
  }]
}
```

The job execution role needs write access to that bucket.

To find where an existing Spark Operator job writes its logs:

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
  eks:
    url: "http://localhost:18080"
```

If you deploy the MCP server into the same cluster with the
[Helm chart](../../../deploy/kubernetes/helm/README.md), use the History Server's
in-cluster Service DNS name as the server URL instead.

## Step 2b: Or start a local History Server against the bucket

```bash
aws sso login --profile my-profile   # or export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
./examples/cloud/start_cloud_spark_history.sh \
  --event-dir=s3://my-bucket/spark-events --profile=my-profile --region=us-west-2
```

`s3://` paths are rewritten to `s3a://`, the connector open-source Hadoop uses. The
script detects the Hadoop version in the Spark image and downloads the matching
`hadoop-aws` and AWS SDK jars. See [examples/cloud](../../cloud/README.md) for flags and
troubleshooting.

## Step 3: Start the MCP server

```bash
task start-mcp-bg
```
