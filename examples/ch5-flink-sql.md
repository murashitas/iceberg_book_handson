# 第 5 章: Apache Flink

本資料では、本書第 5 章の内容に沿って、ターミナル上で Flink SQL をインタラクティブに実行するためのクエリサンプルを紹介しています。ハンズオン環境で Flinki SQL を実行するにあたって、最初に**Flink 環境のセットアップ**を完了させる必要があります。**Flink でデータ処理を実行する**以降の節では、本書の節名に対応したサンプルクエリを紹介しており、本書を参照しながら Iceberg テーブルを操作するための様々な Flink SQL を実行できます。

## Flink 環境のセットアップ

ハンズオン環境のコンテナを起動した後、Flink SQL クライアントを起動します。まず、ターミナルのカレントディレクトリが `iceberg_book_handson` であることを確認してから、以下の手順を実行してください。

### Flink SQL クライアントの起動

Flink SQL クライアントを利用することで、Flink SQL を利用し様々な処理を実行できます。以下のコマンドをターミナル上で実行し、Flink SQL クライアントを起動します。

```sh
# Podman を利用している場合
$ podman compose run flink-sql-client

# Docker を利用している場合
$ docker compose run flink-sql-client
```

Flink SQL クライアントが正常に起動すると、以下のような出力が表示されます。

```txt
$ podman compose run flink-sql-client

[+] Creating 1/1
 ✔ Container flink-jobmanager  Running

Successfully initialized from sql script: file:/opt/flink/init.sql

                                   ▒▓██▓██▒
                               ▓████▒▒█▓▒▓███▓▒
                            ▓███▓░░        ▒▒▒▓██▒  ▒
                          ░██▒   ▒▒▓▓█▓▓▒░      ▒████
                          ██▒         ░▒▓███▒    ▒█▒█▒
                            ░▓█            ███   ▓░▒██
                              ▓█       ▒▒▒▒▒▓██▓░▒░▓▓█
                            █░ █   ▒▒░       ███▓▓█ ▒█▒▒▒
                            ████░   ▒▓█▓      ██▒▒▒ ▓███▒
                         ░▒█▓▓██       ▓█▒    ▓█▒▓██▓ ░█░
                   ▓░▒▓████▒ ██         ▒█    █▓░▒█▒░▒█▒
                  ███▓░██▓  ▓█           █   █▓ ▒▓█▓▓█▒
                ░██▓  ░█░            █  █▒ ▒█████▓▒ ██▓░▒
               ███░ ░ █░          ▓ ░█ █████▒░░    ░█░▓  ▓░
              ██▓█ ▒▒▓▒          ▓███████▓░       ▒█▒ ▒▓ ▓██▓
           ▒██▓ ▓█ █▓█       ░▒█████▓▓▒░         ██▒▒  █ ▒  ▓█▒
           ▓█▓  ▓█ ██▓ ░▓▓▓▓▓▓▓▒              ▒██▓           ░█▒
           ▓█    █ ▓███▓▒░              ░▓▓▓███▓          ░▒░ ▓█
           ██▓    ██▒    ░▒▓▓███▓▓▓▓▓██████▓▒            ▓███  █
          ▓███▒ ███   ░▓▓▒░░   ░▓████▓░                  ░▒▓▒  █▓
          █▓▒▒▓▓██  ░▒▒░░░▒▒▒▒▓██▓░                            █▓
          ██ ▓░▒█   ▓▓▓▓▒░░  ▒█▓       ▒▓▓██▓    ▓▒          ▒▒▓
          ▓█▓ ▓▒█  █▓░  ░▒▓▓██▒            ░▓█▒   ▒▒▒░▒▒▓█████▒
           ██░ ▓█▒█▒  ▒▓▓▒  ▓█                █░      ░░░░   ░█▒
           ▓█   ▒█▓   ░     █░                ▒█              █▓
            █▓   ██         █░                 ▓▓        ▒█▓▓▓▒█░
             █▓ ░▓██░       ▓▒                  ▓█▓▒░░░▒▓█░    ▒█
              ██   ▓█▓░      ▒                    ░▒█▒██▒      ▓▓
               ▓█▒   ▒█▓▒░                         ▒▒ █▒█▓▒▒░░▒██
                ░██▒    ▒▓▓▒                     ▓██▓▒█▒ ░▓▓▓▓▒█▓
                  ░▓██▒                          ▓░  ▒█▓█  ░░▒▒▒
                      ▒▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒░░▓▓  ▓░▒█░

    ______ _ _       _       _____  ____  _         _____ _ _            _  BETA
   |  ____| (_)     | |     / ____|/ __ \| |       / ____| (_)          | |
   | |__  | |_ _ __ | | __ | (___ | |  | | |      | |    | |_  ___ _ __ | |_
   |  __| | | | '_ \| |/ /  \___ \| |  | | |      | |    | | |/ _ \ '_ \| __|
   | |    | | | | | |   <   ____) | |__| | |____  | |____| | |  __/ | | | |_
   |_|    |_|_|_| |_|_|\_\ |_____/ \___\_\______|  \_____|_|_|\___|_| |_|\__|

        Welcome! Enter 'HELP;' to list all available commands. 'QUIT;' to exit.

Command history file path: /opt/flink/.flink-sql-history
```

なお、本ハンズオン環境においては、上記コマンドを実行した際に、`flink/setup` 配下における `init.sql` が実行され、Iceberg REST カタログを利用するようセッションが自動で設定されます。

### 実行モードをバッチモードあるいはストリーミングモードに設定する

Flink SQL により Iceberg テーブルを操作する際に、実行するコマンドによってはバッチモードでのみ実行可能な場合があります。このような場合において、コマンドを実行する前に、`SET` コマンドを利用して実行モードを変更します。`execution.runtime-mode` に対して `batch` (バッチモード)あるいは `streaming` (ストリーミングモード)を設定できます。

```sql
-- バッチモードで実行する
SET 'execution.runtime-mode' = 'batch';

-- ストリーミングモードで実行する
SET 'execution.runtime-mode' = 'streaming';
```

## 事前準備

### (Optional) データベースの作成

以下のコマンドを実行することで、データベースを作成します。

```sql
CREATE DATABASE db;
```

## Flink でデータ処理を実行する

事前にテーブルを作成します。

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

### シンプルなデータの読み出し

以下のクエリを実行すると、`TABLE`モードで画面が起動します。

```sql
SELECT * FROM db.sales_data /*+ OPTIONS('source.monitor-interval' = '10s') */;
```

`flink-streaming-data_0.json`, `flink-streaming-data_1.json`, `flink-streaming-data_2.json`を本書の内容に従いそれぞれ順番に`s3://amzn-s3-demo-bucket/flink/data` にアップロードします。この時の出力が本書の内容と合うか確認してください。

### ウィンドウ集計処理を行う

以下のクエリを実行し、`TABLE`モードの画面が起動することを確認します。

```sql
-- カテゴリ別に 1 分間隔のタイムウィンドウで売上集計を行う
SELECT
    category,
    round(sum(price)) as total_sales,
    count(*) as record_count,
    window_start,
    window_end
FROM TABLE(
    TUMBLE(
        (SELECT * FROM db.sales_data /*+ OPTIONS('source.monitor-interval' = '10s') */),
        DESCRIPTOR(recorded_at),
        INTERVAL '1' MINUTES))
GROUP BY category, window_start, window_end
```

`flink-streaming-data_0.json`, `flink-streaming-data_1.json`, `flink-streaming-data_2.json`を本書の内容に従いそれぞれ順番に`s3://amzn-s3-demo-bucket/flink/data` にアップロードします。この時の出力が本書の内容と合うか確認してください。

### バッチモードでの集計処理を行う

バッチモードに変更します。

```sql
SET 'execution.runtime-mode' = 'batch;
```

集計クエリを実行し、テーブル全体の集計結果を取得します。

```sql
SELECT category, round(sum(price)) as total_sales, count(*) as record_count
FROM db.sales_data GROUP BY category;

/* 実行結果
+----------+-------------+--------------+
| category | total_sales | record_count |
+----------+-------------+--------------+
|    drink |          34 |            8 |
|  grocery |          27 |            7 |
|  kitchen |         174 |            8 |
+----------+-------------+--------------+
*/
```

## Iceberg の利用を開始する - テーブルの作成と基本的な操作

Flink SQL で Iceberg テーブルを作成し、データの読み書きを行います。事前に実行モードを**バッチモード**にセットしてください。

```sql
-- 商品の売上データを格納するテーブル sales_iceberg_example を作成する
CREATE TABLE db.sales_iceberg_example (
    product_name string,
    price decimal(10, 2),
    customer_id bigint,
    order_id string,
    record_at timestamp,
    category string);

-- 作成したテーブルにレコードを追加する
INSERT INTO db.sales_iceberg_example VALUES
    ('orange juice', 3.50, 1756, 'OR78FNGH45TY92', TIMESTAMP '2025-04-27 14:23:18', 'drink'),
    ('cutting board', 12.99, 1842, 'KB93HDRT56UJ21', TIMESTAMP '2025-04-26 09:45:32', 'kitchen'),
    ('pasta', 1.75, 1533, 'GR67LKJU23RT98', TIMESTAMP '2025-04-27 18:12:05', 'grocery'),
    ('mixing bowl', 8.25, 1695, 'KB45PLMN78RT34', TIMESTAMP '2025-04-25 11:36:47', 'kitchen'),
    ('green tea', 4.25, 1921, 'DR56NBVC34RT67', TIMESTAMP '2025-04-26 16:58:23', 'drink');

-- 追加したデータを読む
SELECT product_name, price, customer_id, order_id, record_at, category
FROM db.sales_iceberg_example;

/* 出力されるテーブルレコード
+---------------+-------+-------------+----------------+----------------------------+----------+
|  product_name | price | customer_id |       order_id |                  record_at | category |
+---------------+-------+-------------+----------------+----------------------------+----------+
|  orange juice |  3.50 |        1756 | OR78FNGH45TY92 | 2025-04-27 14:23:18.000000 |    drink |
| cutting board | 12.99 |        1842 | KB93HDRT56UJ21 | 2025-04-26 09:45:32.000000 |  kitchen |
|         pasta |  1.75 |        1533 | GR67LKJU23RT98 | 2025-04-27 18:12:05.000000 |  grocery |
|   mixing bowl |  8.25 |        1695 | KB45PLMN78RT34 | 2025-04-25 11:36:47.000000 |  kitchen |
|     green tea |  4.25 |        1921 | DR56NBVC34RT67 | 2025-04-26 16:58:23.000000 |    drink |
+---------------+-------+-------------+----------------+----------------------------+----------+
*/
```

Iceberg テーブルに対して、ストリーミング読み込みを行う際は、以下のようなオプションを追加することでも可能です。

```sql
-- ストリーミング処理でIcebergテーブルデータを読む
SELECT product_name, price, customer_id, order_id, record_at, category
FROM db.sales_iceberg_example /*+ OPTIONS('streaming'='true') */;
```

## 基本的な Iceberg 機能の利用

### テーブルの作成

```sql
-- 商品の売上データを格納するIcebergテーブルsales_icebergを作成する
CREATE TABLE db.sales_iceberg (
    product_name string,
    price decimal(10, 2),
    customer_id bigint,
    order_id string,
    record_at timestamp,
    category string
);
```

#### テーブルプロパティの設定

以下の例では、テーブルロケーションをカスタマイズしています。

```sql
CREATE TABLE db.sales_iceberg (
    product_name string,
    price double,
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string)
WITH ('location'='s3://amzn-s3-demo-bucket/custom-path');
```

以下の例は、テーブルロケーションをカスタマイズし、Icebergテーブルのメタデータファイルをgzip形式で圧縮するように設定しています。

```sql
CREATE TABLE db.sales_iceberg (
    product_name string,
    price decimal(10, 2),
    customer_id bigint,
    order_id string,
    record_at timestamp,
    category string)
WITH (
    'location'='s3://amzn-s3-demo-bucket/custom-path',
    'write.metadata.compression-codec'='gzip'
);
```

#### テーブルパーティションの設定

以下の例では、`category` カラムでパーティションニングした `sales_iceberg` テーブルを作成しています。

```sql
CREATE TABLE db.sales_iceberg (
    product_name string,
    price decimal(10, 2),
    customer_id bigint,
    order_id string,
    record_at timestamp,
    category string)
PARTITIONED BY (category);
```

#### プライマリーキーの設定

以下の例では、`order_id` カラムをプライマリーキーとして指定しています。

```sql
CREATE TABLE db.sales_iceberg (
    product_name string,
    price decimal(10, 2),
    customer_id bigint,
    order_id string,
    record_at timestamp,
    category string,
    PRIMARY KEY(`order_id`) NOT ENFORCED
);
```

### データの読み込み

```sql
-- バッチクエリ
SELECT product_name, price, customer_id, order_id, record_at, category
FROM db.sales_iceberg;

-- ストリーミングクエリ
SELECT product_name, price, customer_id, order_id, record_at, category
FROM db.sales_iceberg /*+ OPTIONS('streaming'='true') */;
```

以下の例は、10 秒間隔でソースを監視するストリーミングクエリです。

```sql
SELECT product_name, price, customer_id, order_id, record_at, category
FROM db.sales_iceberg /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s') */;
```

### データの書き込み

#### INSERT INTO によるデータの書き込み

```sql
-- 作成したテーブルにレコードを追加する
INSERT INTO db.sales_iceberg VALUES
    ('orange juice', 3.50, 1756, 'OR78FNGH45TY92', TIMESTAMP '2025-04-27 14:23:18', 'drink'),
    ('cutting board', 12.99, 1842, 'KB93HDRT56UJ21', TIMESTAMP '2025-04-26 09:45:32', 'kitchen'),
    ('pasta', 1.75, 1533, 'GR67LKJU23RT98', TIMESTAMP '2025-04-27 18:12:05', 'grocery'),
    ('mixing bowl', 8.25, 1695, 'KB45PLMN78RT34', TIMESTAMP '2025-04-25 11:36:47', 'kitchen'),
    ('green tea', 4.25, 1921, 'DR56NBVC34RT67', TIMESTAMP '2025-04-26 16:58:23', 'drink');
```

#### INSERT OVERWRITE によるデータの上書き

```sql
/* 最初の Iceberg テーブルデータ (SELECT * FROM db.sales_iceberg の出力結果)
+---------------+-------+-------------+----------------+----------------------------+----------+
|  product_name | price | customer_id |       order_id |                  record_at | category |
+---------------+-------+-------------+----------------+----------------------------+----------+
|  orange juice |  3.50 |        1756 | OR78FNGH45TY92 | 2025-04-27 14:23:18.000000 |    drink |
| cutting board | 12.99 |        1842 | KB93HDRT56UJ21 | 2025-04-26 09:45:32.000000 |  kitchen |
|         pasta |  1.75 |        1533 | GR67LKJU23RT98 | 2025-04-27 18:12:05.000000 |  grocery |
|   mixing bowl |  8.25 |        1695 | KB45PLMN78RT34 | 2025-04-25 11:36:47.000000 |  kitchen |
|     green tea |  4.25 |        1921 | DR56NBVC34RT67 | 2025-04-26 16:58:23.000000 |    drink |
+---------------+-------+-------------+----------------+----------------------------+----------+
*/

-- INSERT OVERWRITE によるテーブルデータの上書き
INSERT OVERWRITE db.sales_iceberg VALUES
  ('sparkling water', 1.25, 1876, 'DR67QWER12TY45', TIMESTAMP '2025-04-25 15:41:08', 'drink');

/* INSERT OVERWRITE 実行後のテーブルデータ (SELECT * FROM db.sales_iceberg の出力結果)
+-----------------+-------+-------------+----------------+----------------------------+----------+
|    product_name | price | customer_id |       order_id |                  record_at | category |
+-----------------+-------+-------------+----------------+----------------------------+----------+
| sparkling water |  1.25 |        1876 | DR67QWER12TY45 | 2025-04-25 15:41:08.000000 |    drink |
+-----------------+-------+-------------+----------------+----------------------------+----------+
*/
```

以下の例は、テーブルがパーティショニングされている場合における**動的パーティション書き込み**の実行結果を示しています。

```sql
/* INSERT OVERWRITE 実行前のテーブルデータ (SELECT * FROM db.sales_iceberg の出力結果)
+---------------+-------+-------------+----------------+----------------------------+----------+
|  product_name | price | customer_id |       order_id |                  record_at | category |
+---------------+-------+-------------+----------------+----------------------------+----------+
|  orange juice |  3.50 |        1756 | OR78FNGH45TY92 | 2025-04-27 14:23:18.000000 |    drink |
|     green tea |  4.25 |        1921 | DR56NBVC34RT67 | 2025-04-26 16:58:23.000000 |    drink |
| cutting board | 12.99 |        1842 | KB93HDRT56UJ21 | 2025-04-26 09:45:32.000000 |  kitchen |
|   mixing bowl |  8.25 |        1695 | KB45PLMN78RT34 | 2025-04-25 11:36:47.000000 |  kitchen |
|         pasta |  1.75 |        1533 | GR67LKJU23RT98 | 2025-04-27 18:12:05.000000 |  grocery |
+---------------+-------+-------------+----------------+----------------------------+----------+
*/

-- 動的パーティションモードでの実行
INSERT OVERWRITE db.sales_iceberg
    SELECT * FROM (
    VALUES
        ('sparkling water', 1.25, 1876, 'DR67QWER12TY45', TIMESTAMP '2025-04-25 15:41:08', 'drink')
    ) AS t (product_name, price, customer_id, order_id, record_at, category);

/* INSERT OVERWRITE 実行後のテーブルデータ (SELECT * FROM db.sales_iceberg の出力結果)
+-----------------+-------+-------------+----------------+----------------------------+----------+
|    product_name | price | customer_id |       order_id |                  record_at | category |
+-----------------+-------+-------------+----------------+----------------------------+----------+
| sparkling water |  1.25 |        1876 | DR67QWER12TY45 | 2025-04-25 15:41:08.000000 |    drink |
|           pasta |  1.75 |        1533 | GR67LKJU23RT98 | 2025-04-27 18:12:05.000000 |  grocery |
|   cutting board | 12.99 |        1842 | KB93HDRT56UJ21 | 2025-04-26 09:45:32.000000 |  kitchen |
|     mixing bowl |  8.25 |        1695 | KB45PLMN78RT34 | 2025-04-25 11:36:47.000000 |  kitchen |
+-----------------+-------+-------------+----------------+----------------------------+----------+
*/
```

## 高度な Iceberg 機能の利用