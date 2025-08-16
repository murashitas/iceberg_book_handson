# 第 5 章: Apache Flink

## Flink 環境のセットアップ

Flink SQL クライアントを利用することで、SQL を利用し様々な処理を実行できます。以下のコマンドをターミナル上で実行し、Flink SQL クライアントを起動します。

```sh
# Podman を利用している場合
$ podman compose run flink-sql-client

# Docker を利用している場合
$ docker compose run flink-sql-client
```

なお、... なので、自動でカタログが ... にセットされます。

### 実行モードをバッチモードあるいはストリーミングモードに設定する

... 
`SET` コマンドにより`execution.runtime-mode` に対して `batch` (バッチモード)または `streaming` (ストリーミングモード)を設定します。

```sql
-- バッチモードで実行する
SET 'execution.runtime-mode' = 'batch';

-- ストリーミングモードで実行する
SET 'execution.runtime-mode' = 'streaming';
```

### SQL 実行結果の出力方法を変更する

`SET` コマンドにより設定できます。

```sql
SET 'sql-client.execution.result-mode'='tableau';
```

## 事前準備

### (Optional) データベースの作成

以下のコマンドを実行することで、データベースを作成します。

```sql
CREATE DATABASE db;
```

## Flink でデータ処理を実行する

```sql
-- 商品の売上データを読み出すためのテーブル sales_data を作成する
CREATE TABLE db.sales_data (
    product_name string,
    price decimal(10, 2),
    customer_id bigint,
    order_id string,
    created_at string,
    category string,
    recorded_at TIMESTAMP(3) METADATA FROM 'file.modification-time' VIRTUAL,
    file_name STRING METADATA FROM 'file.name' VIRTUAL,
    WATERMARK FOR recorded_at AS recorded_at - INTERVAL '5' SECONDS
) WITH (
    'connector' = 'filesystem',
    'path' = 's3://amzn-s3-demo-bucket/flink/data',
    'format' = 'json'
);
```

## Iceberg の利用を開始する - テーブルの作成と基本的な操作

## 基本的な Iceberg 機能の利用

## 高度な Iceberg 機能の利用