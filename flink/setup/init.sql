-- iceberg_catalog と言う名前で Flink カタログを作成
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://server:8181',
    'warehouse' = 's3://amzn-s3-demo-bucket',
    's3.endpoint' = 'http://minio:9000',
    's3.path.style.access' = 'true'
);

-- デフォルトカタログを iceberg_catalog に設定
USE CATALOG iceberg_catalog;

-- Checkpointing の設定
SET 'execution.checkpointing.interval' = '10s';
SET 'state.checkpoints.dir' = 's3://amzn-s3-demo-bucket/flink/checkpoint';
