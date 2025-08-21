# Example of Custom Metrics Reporter

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
spark = (
    SparkSession.builder
        .config("spark.jars.packages", "./custom-metrics-reporter-iceberg-1.0-SNAPSHOT.jar")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", CATALOG_URL)
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", S3_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.view-endpoints-supported", "true")
        .config(f"spark.sql.catalog.{CATALOG}.metrics-reporter-impl", "jp.gihyo.iceberg.IcebergTableMetricsReporter")
        .config(f"spark.sql.catalog.{CATALOG}.namespace", "db")
        .config(f"spark.sql.catalog.{CATALOG}.table-prefix", "metrics_report")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "my_catalog")
        .getOrCreate()
)
```
