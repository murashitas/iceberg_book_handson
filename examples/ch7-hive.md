# 第 7 章: Apache Hive

本ファイルでは、本書第 7 章の内容に沿って、ターミナル上で HiveQL をインタラクティブに実行するためのクエリサンプルを紹介しています。ハンズオン環境で HiveQL を実行するにあたって、最初に**Hive環境のセットアップ**と**事前準備**を完了させる必要があります。**Hive でデータ処理を実行する**以降の節では、本書の節名に対応したサンプルクエリを紹介しており、本書を参照しながら Iceberg テーブルを操作するための様々な Hive クエリを実行できます。

## Hive 環境のセットアップ

ハンズオン環境のコンテナを起動した後、Beeline クライアントを起動し、必要なJAR (Java Archive)ファイルをセッションに追加する必要があります。まず、ターミナルのカレントディレクトリが `iceberg_book_handson` であることを確認してから、以下の手順を実行してください。

### Beeline クライアントの起動

Beeline クライアントは HiveQL を実行するためのコマンドラインツールです。以下のコマンドをターミナルで実行してクライアントを起動します。

```sh
# Podman を利用している場合
$ podman exec -it hive beeline -u 'jdbc:hive2://localhost:10000/' -n hive

# Docker を利用している場合
$ docker exec -it hive beeline -u 'jdbc:hive2://localhost:10000/' -n hive
```

Beeline クライアントが正常に起動すると、以下のような出力が表示されます。

```txt
$ podman exec -it hive beeline -u 'jdbc:hive2://localhost:10000/' -n hive

SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.18.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop/share/hadoop/common/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.18.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop/share/hadoop/common/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Connecting to jdbc:hive2://localhost:10000/
Connected to: Apache Hive (version 4.0.1)
Driver: Hive JDBC (version 4.0.1)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 4.0.1 by Apache Hive
0: jdbc:hive2://localhost:10000/>
```

### Iceberg および Amazon S3 (本環境では MinIO)を使用するための設定

Beeline クライアントの起動後、Iceberg テーブルと MinIO を使用するために必要な 2 つの JAR ファイルを追加します。これらのファイルはコンテナ環境に既に含まれているため、追加のダウンロードは不要です。

```sql
ADD JAR /opt/hive/lib/iceberg-aws-bundle-1.4.3.jar;
ADD JAR /opt/hive/lib/iceberg-hive-runtime-1.4.3.jar;
```

実行例：

```txt
0: jdbc:hive2://localhost:10000/> ADD JAR /opt/hive/lib/iceberg-aws-bundle-1.4.3.jar;
INFO  : Added [/opt/hive/lib/iceberg-aws-bundle-1.4.3.jar] to class path
INFO  : Added resources: [/opt/hive/lib/iceberg-aws-bundle-1.4.3.jar]
No rows affected (0.095 seconds)

0: jdbc:hive2://localhost:10000/> ADD JAR /opt/hive/lib/iceberg-hive-runtime-1.4.3.jar;
INFO  : Added [/opt/hive/lib/iceberg-hive-runtime-1.4.3.jar] to class path
INFO  : Added resources: [/opt/hive/lib/iceberg-hive-runtime-1.4.3.jar]
No rows affected (0.009 seconds)
```

## 事前準備

### (Optional) データベースの作成

以下のコマンドを実行することで、Hive メタストアにデータベースを作成します。

```sql
CREATE DATABASE db LOCATION 's3a://amzn-s3-demo-bucket/hive';
```

### サンプルデータファイルを MinIO にアップロードする

サンプルデータファイル [`data.json`](https://github.com/murashitas/iceberg_book_handson/blob/main/sample-data/data.json)を MinIO にアップロードします。事前に[MinIO にファイルをアップロードする](https://github.com/murashitas/iceberg_book_handson/blob/main/README.md#minio-%E3%81%AB%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB%E3%82%92%E3%82%A2%E3%83%83%E3%83%97%E3%83%AD%E3%83%BC%E3%83%89%E3%81%99%E3%82%8B)を参考に、データファイルのアップロードを完了してください:

1. [`data.json`](https://github.com/murashitas/iceberg_book_handson/blob/main/sample-data/data.json)よりデータファイルをダウンロードする
2. `localhost:9001`よりMinIOコンソールにアクセスする
3. `amzn-s3-demo-bucket`配下に`hive/data`パスを作成し、ダウンロードした`data.json`を`data`配下にアップロードします

## Hive でデータ処理を実行する

```sql
-- 商品の売上データを格納するテーブル sales_data と、集計結果を格納するテーブル sales_analysis を作成する
CREATE EXTERNAL TABLE db.sales_data (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
WITH SERDEPROPERTIES ("timestamp.formats" = "yyyy-MM-dd'T'HH:mm:ss'Z'")
STORED AS TEXTFILE
LOCATION 's3a://amzn-s3-demo-bucket/hive/data';

CREATE EXTERNAL TABLE db.sales_analysis (
  category string,
  total_sales double,
  count_by_year int,
  year int)
STORED AS PARQUET
LOCATION 's3a://amzn-s3-demo-bucket/hive/analysis';

-- sales_data からデータを読み取り、各 category と year ごとの取引数と合計売上額を集計する
-- 結果を sales_analysis テーブルに書き込む
INSERT INTO db.sales_analysis
SELECT category, round(sum(price)) as total_sales, count(*) as count_by_year,  year
FROM (SELECT category, price, year(datetime) as year FROM db.sales_data2) t
GROUP BY category, year
```

`sales_analysis` へクエリすると以下の結果が得られます。

```sql
SELECT category, total_sales, count_by_year, year FROM db.sales_analysis ORDER BY year DESC;

/*
+-----------+--------------+----------------+-------+
| category  | total_sales  | count_by_year  | year  |
+-----------+--------------+----------------+-------+
| drink     | 906.0        | 640            | 2024  |
| grocery   | 984.0        | 519            | 2024  |
| kitchen   | 4536.0       | 525            | 2024  |
| drink     | 1007.0       | 704            | 2023  |
| grocery   | 950.0        | 484            | 2023  |
| kitchen   | 4560.0       | 501            | 2023  |
| drink     | 914.0        | 656            | 2022  |
| grocery   | 989.0        | 497            | 2022  |
| kitchen   | 5448.0       | 474            | 2022  |
+-----------+--------------+----------------+-------+
*/
```

## Iceberg の利用を開始する - テーブルの作成と操作

以下に Hive 経由で Iceberg テーブルを作成し、データの読み書きを行うクエリ例です。

```sql
-- 商品の売上データを格納するテーブル sales_iceberg_example を作成する
CREATE EXTERNAL TABLE db.sales_iceberg_example (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string) 
STORED BY ICEBERG 
TBLPROPERTIES('iceberg.catalog'='hive_catalog');

-- 作成したテーブルにレコードを追加する
INSERT INTO db.sales_iceberg_example VALUES 
    ('tomato juice', 2.00, 1698, 'DRE8DLTFNX0MLCE8DLTFNX0MLC', '2023-07-18 02:20:58', 'drink'),
    ('cocoa', 2.00, 1652, 'DR1UNFHET81UNFHET8', '2024-08-26 11:36:48', 'drink'),
    ('espresso', 2.00, 1037, 'DRBFZUJWPZ9SRABFZUJWPZ9SRA', '2024-04-19 12:17:22', 'drink'),
    ('broccoli', 1.00, 3092, 'GRK0L8ZQK0L8ZQ', '2023-03-22 18:48:04', 'grocery'),
    ('nutmeg', 1.00, 3512, 'GR15U0LKA15U0LKA', '2024-02-27 15:13:31', 'grocery');

-- 追加したデータを読む
SELECT product_name, price, customer_id, order_id, datetime, category 
FROM db.sales_iceberg_example

/* テーブル読み出し後の実行結果
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| product_name  | price  | customer_id  |          order_id           |        datetime        | category  |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| tomato juice  | 2.0    | 1698         | DRE8DLTFNX0MLCE8DLTFNX0MLC  | 2023-07-18 02:20:58.0  | drink     |
| cocoa         | 2.0    | 1652         | DR1UNFHET81UNFHET8          | 2024-08-26 11:36:48.0  | drink     |
| espresso      | 2.0    | 1037         | DRBFZUJWPZ9SRABFZUJWPZ9SRA  | 2024-04-19 12:17:22.0  | drink     |
| broccoli      | 1.0    | 3092         | GRK0L8ZQK0L8ZQ              | 2023-03-22 18:48:04.0  | grocery   |
| nutmeg        | 1.0    | 3512         | GR15U0LKA15U0LKA            | 2024-02-27 15:13:31.0  | grocery   |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
*/
```

## DDL (Data Definition Language)

DDL では以下のテーブル作成・操作方法を紹介します:

* 基本的なテーブル作成: `CREATE TABLE`, `CREATE TABLE AS SELECT` (CTAS), `CREATE TABLE LIKE TABLE` (CTLT)
* テーブル変更: `ALTER TABLE` によるパーティション・スキーマ変更
* テーブル削除: `TRUCATE`

> [!NOTE]
> 以降の例で示すテーブル名は`sales_iceberg`を共通して使用していますが、同じ名前のテーブルを重複して作成することはできないため、適宜`sales_iceberg_2`など新しいテーブル名でテーブルを作成するか、`DROP TABLE db.<テーブル名> PURGE`で既存テーブル (とそのテーブルに紐づくデータ)を削除してください。

### 基本的なテーブル作成

```sql
-- 商品の売上データを格納する Iceberg テーブル sales_iceberg を作成する
CREATE EXTERNAL TABLE db.sales_iceberg (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string) 
STORED BY ICEBERG 
TBLPROPERTIES('iceberg.catalog'='hive_catalog');
```

#### テーブルロケーションの設定

```sql
CREATE EXTERNAL TABLE db.sales_iceberg (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string) 
STORED BY ICEBERG 
LOCATION 's3a://amzn-s3-demo-bucket/hive-custom-location'
TBLPROPERTIES('iceberg.catalog'='hive_catalog');
```

#### テーブルパーティションの設定

```sql
-- 1) アイデンティティパーティションを指定した場合
CREATE EXTERNAL TABLE db.sales_iceberg (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string)
PARTITIONED BY (category) -- アイデンティティパーティションキーとして category を指定
STORED BY ICEBERG 
TBLPROPERTIES('iceberg.catalog'='hive_catalog');


-- 2) パーティション変換を指定した場合
CREATE EXTERNAL TABLE db.sales_iceberg (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string)
PARTITIONED BY SPEC (bucket(10, order_id)) -- bucket により order_id カラムの値をバケッティング
STORED BY ICEBERG 
TBLPROPERTIES('iceberg.catalog'='hive_catalog');


-- 2') パーティション変換 (`order_id` をバケッティングし、datetime から year を取得)と
-- アイデンティティパーティション (`category`)を指定した場合
CREATE EXTERNAL TABLE db.sales_iceberg (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string)
PARTITIONED BY SPEC (bucket(10, order_id), year(datetime), category)
STORED BY ICEBERG 
TBLPROPERTIES('iceberg.catalog'='hive_catalog');
```

#### テーブルプロパティの設定

```sql
CREATE EXTERNAL TABLE db.sales_iceberg (id int, name string, category string, price int, year int) 
STORED BY ICEBERG 
TBLPROPERTIES(
    'iceberg.catalog'='hive_catalog',
    'write.format.default'='orc',
    'history.expire.max-snapshot-age-ms'='2592000000'
);

```

### CTAS によるテーブルの作成

```sql
CREATE EXTERNAL TABLE db.sales_analysis_iceberg 
STORED BY ICEBERG
TBLPROPERTIES ('iceberg.catalog'='hive_catalog')
AS SELECT category, round(sum(price)) as total_sales, count(*) as count_by_year,  year
FROM (SELECT category, price, year(datetime) as year FROM db.sales_data) t
GROUP BY category, year
```

以下の CTAS によりテーブルを作成する際に、パーティションも設定できます。

```sql
CREATE EXTERNAL TABLE db.sales_analysis_iceberg 
PARTITIONED BY SPEC (bucket(3, category))
STORED BY ICEBERG
LOCATION 's3a://amzn-s3-demo-bucket/hive-custom-loc-ctas'
TBLPROPERTIES ('iceberg.catalog'='hive_catalog')
AS SELECT category, round(sum(price)) as total_sales, count(*) as count_by_year, year
FROM (SELECT category, price, year(datetime) as year FROM db.sales_data) t
GROUP BY category, year
```

### CTLT (CREATE TABLE LIKE TABLE)によるテーブルの作成

事前準備として、以下のクエリを実行し、Iceberg テーブルを作成します。

```sql
-- 商品の売上データを格納する Iceberg テーブル sales_iceberg を作成する
CREATE EXTERNAL TABLE db.sales_iceberg (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string) 
STORED BY ICEBERG 
TBLPROPERTIES('iceberg.catalog'='hive_catalog');

INSERT INTO db.sales_iceberg VALUES 
    ('tomato juice', 2.00, 1698, 'DRE8DLTFNX0MLCE8DLTFNX0MLC', '2023-07-18 02:20:58', 'drink'),
    ('cocoa', 2.00, 1652, 'DR1UNFHET81UNFHET8', '2024-08-26 11:36:48', 'drink'),
    ('espresso', 2.00, 1037, 'DRBFZUJWPZ9SRABFZUJWPZ9SRA', '2024-04-19 12:17:22', 'drink'),
    ('broccoli', 1.00, 3092, 'GRK0L8ZQK0L8ZQ', '2023-03-22 18:48:04', 'grocery'),
    ('nutmeg', 1.00, 3512, 'GR15U0LKA15U0LKA', '2024-02-27 15:13:31', 'grocery');
```

CTLT を実行し、`sales_iceberg_stg` Iceberg テーブルを作成します。

```sql
CREATE EXTERNAL TABLE db.sales_iceberg_stg
LIKE db.sales_iceberg
STORED BY ICEBERG
TBLPROPERTIES ('iceberg.catalog'='hive_catalog'); -- 元のカタログ設定が引き継がれるので明示的に記述する必要はなし

-- または
CREATE EXTERNAL TABLE db.sales_iceberg_stg LIKE db.sales_iceberg STORED BY ICEBERG;

/* DESCRIBE 出力例 (created_with_ctlt が true に設定されていることを確認できる)
+-----------------------------+----------------------------------------------------+----------+
|          col_name           |                     data_type                      | comment  |
+-----------------------------+----------------------------------------------------+----------+
| product_name                | string                                             |          |
| price                       | double                                             |          |
| customer_id                 | bigint                                             |          |
| order_id                    | string                                             |          |
| datetime                    | timestamp                                          |          |
| category                    | string                                             |          |
|                             | NULL                                               | NULL     |
| Detailed Table Information  | Table(tableName:sales_iceberg_stg, dbName:db, owner:hive, createTime:1743155243, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:product_name, type:string, comment:null), FieldSchema(name:price, type:double, comment:null), FieldSchema(name:customer_id, type:bigint, comment:null), FieldSchema(name:order_id, type:string, comment:null), FieldSchema(name:datetime, type:timestamp, comment:null), FieldSchema(name:category, type:string, comment:null)], location:s3a://amzn-s3-demo-bucket/hive/db.db/sales_iceberg_stg, inputFormat:org.apache.iceberg.mr.hive.HiveIcebergInputFormat, outputFormat:org.apache.iceberg.mr.hive.HiveIcebergOutputFormat, compressed:false, numBuckets:0, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.iceberg.mr.hive.HiveIcebergSerDe, parameters:{}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[], parameters:{write.merge.mode=merge-on-read, numRows=0, rawDataSize=0, write.delete.mode=merge-on-read, current-schema={\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"product_name\",\"required\":false,\"type\":\"string\"},{\"id\":2,\"name\":\"price\",\"required\":false,\"type\":\"double\"},{\"id\":3,\"name\":\"customer_id\",\"required\":false,\"type\":\"long\"},{\"id\":4,\"name\":\"order_id\",\"required\":false,\"type\":\"string\"},{\"id\":5,\"name\":\"datetime\",\"required\":false,\"type\":\"timestamp\"},{\"id\":6,\"name\":\"category\",\"required\":false,\"type\":\"string\"}]}, format-version=2, iceberg.orc.files.only=false, transient_lastDdlTime=1743155243, bucketing_version=2, parquet.compression=zstd, storage_handler=org.apache.iceberg.mr.hive.HiveIcebergStorageHandler, iceberg.catalog=hive_catalog, numFilesErasureCoded=0, uuid=f62a1951-7648-4dd4-adf6-9d09a458200e, created_with_ctlt=true, totalSize=0, EXTERNAL=TRUE, numFiles=0, metadata_location=s3a://amzn-s3-demo-bucket/hive/db.db/sales_iceberg_stg/metadata/00000-11f6f59c-251c-4ea7-ac2f-e3aa983491db.metadata.json, snapshot-count=0, write.update.mode=merge-on-read, table_type=ICEBERG}, viewOriginalText:null, viewExpandedText:null, tableType:EXTERNAL_TABLE, rewriteEnabled:false, catName:hive, ownerType:USER, writeId:0, accessType:8, id:4) |          |
+-----------------------------+----------------------------------------------------+----------+
*/
```

### テーブルプロパティの変更

```sql
ALTER TABLE db.sales_iceberg SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms'='15552000000',
    'history.expire.min-snapshots-to-keep'='30'
)
```

#### メタデータロケーションの変更

```sql
ALTER TABLE db.sales_iceberg SET TBLPROPERTIES (
    'metadata_location'='s3a://amzn-s3-demo-bucket/hive-new-warehouse/metadata.json'
);
```

### スキーマの変更

```sql
-- 1. price カラムを price_usd に変更
ALTER TABLE db.sales_iceberg CHANGE COLUMN price price_usd double;

-- 2. description, price_yen カラムの追加
ALTER TABLE db.sales_iceberg ADD COLUMNS (description string, price_yen double);

-- 3. description カラムの位置を product_name の後 (かつ price の前)に移動
ALTER TABLE db.sales_iceberg CHANGE COLUMN description description string AFTER product_name;
ALTER TABLE db.sales_iceberg CHANGE COLUMN price_yen price_yen string AFTER price_usd;
```

### パーティションの変更

```sql
-- category をアイデンティティパーティションとして追加
ALTER TABLE db.sales_iceberg SET PARTITION SPEC (category);

-- order_id にパーティション変換関数を適用し、追加
ALTER TABLE db.sales_iceberg SET PARTITION SPEC (year(datetime));
```

`ALTER TABLE DROP PARTITION` によりパーティションをフィルターアウトできます (ただしアイデンティティカラムのみ)。

```sql
ALTER TABLE db.sales_iceberg DROP PARTITION (category = 'drink');
/* DROP PARTITION 実行前のテーブルデータ
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| product_name  | price  | customer_id  |          order_id           |        datetime        | category  |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| tomato juice  | 2.0    | 1698         | DRE8DLTFNX0MLCE8DLTFNX0MLC  | 2023-07-18 02:20:58.0  | drink     |
| cocoa         | 2.0    | 1652         | DR1UNFHET81UNFHET8          | 2024-08-26 11:36:48.0  | drink     |
| espresso      | 2.0    | 1037         | DRBFZUJWPZ9SRABFZUJWPZ9SRA  | 2024-04-19 12:17:22.0  | drink     |
| broccoli      | 1.0    | 3092         | GRK0L8ZQK0L8ZQ              | 2023-03-22 18:48:04.0  | grocery   |
| nutmeg        | 1.0    | 3512         | GR15U0LKA15U0LKA            | 2024-02-27 15:13:31.0  | grocery   |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+

DROP PARTITION実行後テーブルデータ（drinkカテゴリのデータが結果に含まれなくなる）
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| product_name  | price  | customer_id  |          order_id           |        datetime        | category  |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| broccoli      | 1.0    | 3092         | GRK0L8ZQK0L8ZQ              | 2023-03-22 18:48:04.0  | grocery   |
| nutmeg        | 1.0    | 3512         | GR15U0LKA15U0LKA            | 2024-02-27 15:13:31.0  | grocery   |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
*/
```

### TRUNCATE TABLE

```sql
SELECT count(*) as record_count FROM db.sales_iceberg;
/*
+---------------+
| record_count  |
+---------------+
| 5             |
+---------------+
*/

TRUNCATE TABLE db.sales_iceberg; -- TRUNCATE TABLE を実行

SELECT count(*) as record_count FROM db.sales_iceberg;
/* 
+---------------+
| record_count  |
+---------------+
| 0             |
+---------------+
*/

/* DESCRIBE EXTENDED db.sales_iceberg の出力結果 (テーブルスキーマはそのまま維持される)
+-----------------------------+----------------------------------------------------+----------+
|          col_name           |                     data_type                      | comment  |
+-----------------------------+----------------------------------------------------+----------+
| product_name                | string                                             |          |
| price                       | double                                             |          |
| customer_id                 | bigint                                             |          |
| order_id                    | string                                             |          |
| datetime                    | timestamp                                          |          |
| category                    | string                                             |          |
|                             | NULL                                               | NULL     |
| Detailed Table Information  | Table(tableName:sales_iceberg, dbName:db, owner:hive, createTime:1743159865, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:product_name, type:string, comment:null), FieldSchema(name:price, type:double, comment:null), FieldSchema(name:customer_id, type:bigint, comment:null), FieldSchema(name:order_id, type:string, comment:null), FieldSchema(name:datetime, type:timestamp, comment:null), FieldSchema(name:category, type:string, comment:null)], location:s3a://amzn-s3-demo-bucket/hive/db.db/sales_iceberg, inputFormat:org.apache.iceberg.mr.hive.HiveIcebergInputFormat, outputFormat:org.apache.iceberg.mr.hive.HiveIcebergOutputFormat, compressed:false, numBuckets:0, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.iceberg.mr.hive.HiveIcebergSerDe, parameters:{}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[], parameters:{numRows=5, iceberg.orc.files.only=false, transient_lastDdlTime=1743159878, format-version=2, bucketing_version=2, storage_handler=org.apache.iceberg.mr.hive.HiveIcebergStorageHandler, uuid=54a2525c-eabe-4810-bdfc-6cbef03699a8, EXTERNAL=TRUE, numFiles=1, table_type=ICEBERG, previous_metadata_location=s3a://amzn-s3-demo-bucket/hive/db.db/sales_iceberg/metadata/00000-b0ec614c-087b-4275-a6be-c9da734bb1dd.metadata.json, current-snapshot-id=3272568258985560861, rawDataSize=0, current-schema={\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"product_name\",\"required\":false,\"type\":\"string\"},{\"id\":2,\"name\":\"price\",\"required\":false,\"type\":\"double\"},{\"id\":3,\"name\":\"customer_id\",\"required\":false,\"type\":\"long\"},{\"id\":4,\"name\":\"order_id\",\"required\":false,\"type\":\"string\"},{\"id\":5,\"name\":\"datetime\",\"required\":false,\"type\":\"timestamp\"},{\"id\":6,\"name\":\"category\",\"required\":false,\"type\":\"string\"}]}, parquet.compression=zstd, iceberg.catalog=hive_catalog, numFilesErasureCoded=0, totalSize=2089, COLUMN_STATS_ACCURATE={\"COLUMN_STATS\":{\"category\":\"true\",\"customer_id\":\"true\",\"datetime\":\"true\",\"order_id\":\"true\",\"price\":\"true\",\"product_name\":\"true\"}}, current-snapshot-timestamp-ms=1743159878093, metadata_location=s3a://amzn-s3-demo-bucket/hive/db.db/sales_iceberg/metadata/00002-8d7dc25b-0c1b-4832-a0d0-a00d207896b0.metadata.json, snapshot-count=1, serialization.format=1, current-snapshot-summary={\"added-data-files\":\"1\",\"added-records\":\"5\",\"added-files-size\":\"2089\",\"changed-partition-count\":\"1\",\"total-records\":\"5\",\"total-files-size\":\"2089\",\"total-data-files\":\"1\",\"total-delete-files\":\"0\",\"total-position-deletes\":\"0\",\"total-equality-deletes\":\"0\"}}, viewOriginalText:null, viewExpandedText:null, tableType:EXTERNAL_TABLE, rewriteEnabled:false, catName:hive, ownerType:USER, writeId:0, accessType:8, id:12) |          |
+-----------------------------+----------------------------------------------------+----------+
*/
```

#### TRUNCATE TABLE PARTITION

```sql
/* SELECT * FROM db.sales_iceberg の出力結果 (TRUNCATE TABLE PARTITION 実行前)
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| product_name  | price  | customer_id  |          order_id           |        datetime        | category  |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| tomato juice  | 2.0    | 1698         | DRE8DLTFNX0MLCE8DLTFNX0MLC  | 2023-07-18 02:20:58.0  | drink     |
| cocoa         | 2.0    | 1652         | DR1UNFHET81UNFHET8          | 2024-08-26 11:36:48.0  | drink     |
| espresso      | 2.0    | 1037         | DRBFZUJWPZ9SRABFZUJWPZ9SRA  | 2024-04-19 12:17:22.0  | drink     |
| broccoli      | 1.0    | 3092         | GRK0L8ZQK0L8ZQ              | 2023-03-22 18:48:04.0  | grocery   |
| nutmeg        | 1.0    | 3512         | GR15U0LKA15U0LKA            | 2024-02-27 15:13:31.0  | grocery   |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
*/

TRUNCATE TABLE db.sales_iceberg PARTITION (category = 'drink');

/* SELECT * FROM db.sales_iceberg の出力結果 (TRUNCATE TABLE PARTITION 実行後)
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| product_name  | price  | customer_id  |          order_id           |        datetime        | category  |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| broccoli      | 1.0    | 3092         | GRK0L8ZQK0L8ZQ              | 2023-03-22 18:48:04.0  | grocery   |
| nutmeg        | 1.0    | 3512         | GR15U0LKA15U0LKA            | 2024-02-27 15:13:31.0  | grocery   |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
*/
```

## DML (Data Manipulation Language)

DML では以下のテーブル作成・操作方法を紹介します:

* タイムトラベルクエリ
* `MERGE INTO` による Upsert 処理

### タイムトラベルクエリ

事前準備としてテーブル作成を行い、3 レコードを書き込みます。

```sql
CREATE EXTERNAL TABLE db.sales_iceberg (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string) 
STORED BY ICEBERG 
TBLPROPERTIES('iceberg.catalog'='hive_catalog');

INSERT INTO db.sales_iceberg VALUES 
    ('tomato juice', 2.00, 1698, 'DRE8DLTFNX0MLCE8DLTFNX0MLC', '2023-07-18 02:20:58', 'drink'),
    ('cocoa', 2.00, 1652, 'DR1UNFHET81UNFHET8', '2024-08-26 11:36:48', 'drink'),
    ('espresso', 2.00, 1037, 'DRBFZUJWPZ9SRABFZUJWPZ9SRA', '2024-04-19 12:17:22', 'drink');
```

さらに 2 レコードを追加します。この時テーブルバージョンは、3 バージョン (テーブル作成時点、3 レコード書き込み、2レコード書き込み)存在します。

```sql
INSERT INTO db.sales_iceberg VALUES 
    ('broccoli', 1.00, 3092, 'GRK0L8ZQK0L8ZQ', '2023-03-22 18:48:04', 'grocery'),
    ('nutmeg', 1.00, 3512, 'GR15U0LKA15U0LKA', '2024-02-27 15:13:31', 'grocery');
```

本テーブルに対してタイムトラベルを実行します

```sql
SELECT * FROM db.sales_iceberg; -- 現在のテーブルレコードを取得
/*
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| product_name  | price  | customer_id  |          order_id           |        datetime        | category  |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| tomato juice  | 2.0    | 1698         | DRE8DLTFNX0MLCE8DLTFNX0MLC  | 2023-07-18 02:20:58.0  | drink     |
| cocoa         | 2.0    | 1652         | DR1UNFHET81UNFHET8          | 2024-08-26 11:36:48.0  | drink     |
| espresso      | 2.0    | 1037         | DRBFZUJWPZ9SRABFZUJWPZ9SRA  | 2024-04-19 12:17:22.0  | drink     |
| broccoli      | 1.0    | 3092         | GRK0L8ZQK0L8ZQ              | 2023-03-22 18:48:04.0  | grocery   |
| nutmeg        | 1.0    | 3512         | GR15U0LKA15U0LKA            | 2024-02-27 15:13:31.0  | grocery   |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
*/

SELECT * FROM db.sales_iceberg.history; -- スナップショット一覧を取得
/*
+----------------------------------+----------------------------+--------------------------+------------------------------------+
|  sales_iceberg.made_current_at   | sales_iceberg.snapshot_id  | sales_iceberg.parent_id  | sales_iceberg.is_current_ancestor  |
+----------------------------------+----------------------------+--------------------------+------------------------------------+
| 2025-03-28 11:10:10.571 Etc/UTC  | 3510547431145953043        | NULL                     | true                               |
| 2025-03-28 11:11:58.376 Etc/UTC  | 5116666442024175695        | 3510547431145953043      | true                               |
+----------------------------------+----------------------------+--------------------------+------------------------------------+
*/
```

この出力結果を基に、3 レコードを追加した後の時点に、時間およびスナップショットを指定しタイムトラベルクエリを実行します。

```sql
-- 時間によるタイムトラベルクエリ
SELECT * FROM db.sales_iceberg FOR SYSTEM_TIME AS OF '2025-03-20 19:11:00';

-- スナップショットによるタイムトラベルクエリ
SELECT * FROM db.sales_iceberg FOR SYSTEM_VERSION AS OF 3510547431145953043;

/* 時間およびスナップショットによるタイムトラベルクエリ実行結果例
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| product_name  | price  | customer_id  |          order_id           |        datetime        | category  |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| tomato juice  | 2.0    | 1698         | DRE8DLTFNX0MLCE8DLTFNX0MLC  | 2023-07-18 02:20:58.0  | drink     |
| cocoa         | 2.0    | 1652         | DR1UNFHET81UNFHET8          | 2024-08-26 11:36:48.0  | drink     |
| espresso      | 2.0    | 1037         | DRBFZUJWPZ9SRABFZUJWPZ9SRA  | 2024-04-19 12:17:22.0  | drink     |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
*/
```

### MERGE INTO による Upsert 処理

事前準備として、`sales_iceberg`および`sales_logs`テーブルを作成し、それぞれレコードを書き込みます。

```sql
-- 元のテーブル
CREATE EXTERNAL TABLE db.sales_iceberg (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string) 
STORED BY ICEBERG 
TBLPROPERTIES('iceberg.catalog'='hive_catalog');

INSERT INTO db.sales_iceberg VALUES 
    ('tomato juice', 2.00, 1698, 'DRE8DLTFNX0MLCE8DLTFNX0MLC', '2023-07-18 02:20:58', 'drink'),
    ('cocoa', 2.00, 1652, 'DR1UNFHET81UNFHET8', '2024-08-26 11:36:48', 'drink'),
    ('espresso', 2.00, 1037, 'DRBFZUJWPZ9SRABFZUJWPZ9SRA', '2024-04-19 12:17:22', 'drink'),

/* SELECT * FROM db.sales_iceberg の出力結果
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| product_name  | price  | customer_id  |          order_id           |        datetime        | category  |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| tomato juice  | 2.0    | 1698         | DRE8DLTFNX0MLCE8DLTFNX0MLC  | 2023-07-18 02:20:58.0  | drink     |
| cocoa         | 2.0    | 1652         | DR1UNFHET81UNFHET8          | 2024-08-26 11:36:48.0  | drink     |
| espresso      | 2.0    | 1037         | DRBFZUJWPZ9SRABFZUJWPZ9SRA  | 2024-04-19 12:17:22.0  | drink     |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
*/

-- 更新用のソーステーブル
CREATE EXTERNAL TABLE db.sales_logs (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string) 
STORED BY ICEBERG 
TBLPROPERTIES('iceberg.catalog'='hive_catalog');

INSERT INTO db.sales_logs VALUES 
    ('white mocha', 4.00, 1037, 'DRBFZUJWPZ9SRABFZUJWPZ9SRA', '2024-04-19 21:19:33', 'drink'),
    ('eggplant', 1.00, 2492, 'GRK0L8ZQK0L8ZQ', '2024-04-19 21:22:04', 'grocery'),
    ('almond', 2.00, 8912, 'GR15U0LKA15U0LKA', '2024-04-20 11:15:31', 'grocery');

/* SELECT * FROM db.sales_logs の出力結果
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| product_name  | price  | customer_id  |          order_id           |        datetime        | category  |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| eggplant      | 1.0    | 2492         | GRK0L8ZQK0L8ZQ              | 2024-04-19 21:22:04.0  | grocery   |
| almond        | 2.0    | 8912         | GR15U0LKA15U0LKA            | 2024-04-20 11:15:31.0  | grocery   |
| white mocha   | 4.0    | 1037         | DRBFZUJWPZ9SRABFZUJWPZ9SRA  | 2024-04-19 21:19:33.0  | drink     |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
*/
```

MERGE INTO を使って Upsert 処理を実行します。注意点として、Spark のように `INSERT *` の記法は利用できません。

```sql
MERGE INTO db.sales_iceberg t
USING db.sales_logs AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN 
    UPDATE SET 
        product_name=s.product_name, 
        price=s.price, 
        datetime=s.datetime
WHEN NOT MATCHED THEN 
    INSERT VALUES (
        s.product_name,
        s.price,
        s.customer_id,
        s.order_id,
        s.datetime,
        s.category);
```

`MERGE INTO` 実行後、元のテーブルデータを確認すると、`product_name=espresso` が `white mocha` に更新され、`category=grocery` を持つ 2 レコードが追加されています。

```sql
SELECT product_name, price, customer_id, order_id, datetime, category 
FROM db.sales_icenerg
/*
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| product_name  | price  | customer_id  |          order_id           |        datetime        | category  |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
| tomato juice  | 2.0    | 1698         | DRE8DLTFNX0MLCE8DLTFNX0MLC  | 2023-07-18 02:20:58.0  | drink     |
| cocoa         | 2.0    | 1652         | DR1UNFHET81UNFHET8          | 2024-08-26 11:36:48.0  | drink     |
| white mocha   | 4.0    | 1037         | DRBFZUJWPZ9SRABFZUJWPZ9SRA  | 2024-04-19 21:19:33.0  | drink     |
| eggplant      | 1.0    | 2492         | GRK0L8ZQK0L8ZQ              | 2024-04-19 21:22:04.0  | grocery   |
| almond        | 2.0    | 8912         | GR15U0LKA15U0LKA            | 2024-04-20 11:15:31.0  | grocery   |
+---------------+--------+--------------+-----------------------------+------------------------+-----------+
*/
```
