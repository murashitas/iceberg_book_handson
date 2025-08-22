# Example of Custom Metrics Reporter

## Prerequisites

* Java 17+
* Spark 3.5+

## How to build

1. After cloning this repository, move the current directory:

```
$ cd ./custom-metrics-reporter-iceberg
```

2. Run the following command to build this package

```
$ ./gradlew jar
```

3. After the build is complete, `custom-metrics-reporter-iceberg-<version>.jar` is created in `./build/libs`.

## Configure Metrics Reporter for Spark

```py
from pyspark.sql import SparkSession
CATALOG = "my_catalog"
METRICS_CATALOG = "metrics_catalog"
CATALOG_URL = "http://server:8181/"
S3_ENDPOINT = "http://minio:9000"


spark = (
    SparkSession.builder
    .config("spark.jars", "./path/to/custom-metrics-reporter-iceberg-0.1.jar")
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
    .config(f"spark.sql.catalog.{CATALOG}.uri", CATALOG_URL)
    .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint",S3_ENDPOINT)
    .config(f"spark.sql.catalog.{CATALOG}.view-endpoints-supported", "true")
    .config(f"spark.sql.catalog.{CATALOG}.metrics-reporter-impl", "jp.gihyo.iceberg.IcebergTableMetricsReporter")
    .config(f"spark.sql.catalog.{CATALOG}.metrics-catalog-name", METRICS_CATALOG)
    .config(f"spark.sql.catalog.{METRICS_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{METRICS_CATALOG}.type", "rest")
    .config(f"spark.sql.catalog.{METRICS_CATALOG}.uri", CATALOG_URL)
    .config(f"spark.sql.catalog.{METRICS_CATALOG}.s3.endpoint",S3_ENDPOINT)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", "my_catalog")
    .getOrCreate()
)
```
