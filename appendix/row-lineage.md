# Iceberg フォーマットバージョン 3: Row Lineage (行レベルリネージ)

行レベルリネージ (Row Linage)は、Iceberg テーブル内の各行の変更を追跡し、バージョン管理を行うために仕組みです。この仕様はプルリクエスト [#11130](https://github.com/apache/iceberg/pull/11130)で確定され、バージョン 3 以降で各エンジンがサポートすべき必須機能となっています。本機能により、Iceberg テーブルに対する CDC (Change Data Capture)や増分データ処理などの実現が期待されています。Iceberg (Java) 1.8.0 以上で利用可能です。なお行レベルリネージの詳細については Iceberg 公式ドキュメントの [Row Lineage](https://iceberg.apache.org/spec/#row-lineage)も参照してください。

## 行レベルリネージの仕組み

行レベルリネージは以下のフィールドによって実現されています:

* メタデータファイルに保存される行 ID
  * `next-row-id` フィールド: 次に割り当てられる行IDで、コミット時に更新される
  * `first-row-id` フィールド: 各スナップショットで最初に割り当てられる行 ID を示す
* データファイルに保存されるメタデータカラム
  * `_row_id`: テーブル内の各行に割り当てられる一意の整数識別子で、行が最初にテーブルに追加された際に**継承**により割り当てられる
  * `_last_updated_sequence_number`: 行を最後に更新したコミットのシーケンス番号で、行の追加や変更時に**継承**により割り当てられる

以下では、継承の仕組みについて以下で解説します。

### 継承による各メタデータフィールド割り当ての仕組み

読み書きに応じて、それぞれ以下のような仕組みで各メタデータカラムの値が利用されます:

* **書き込み時**: `_row_id` と `_last_updated_sequence_number` は `null` として書き込み
* **読み込み時**: `null` 値を実際の値に置き換える (置き換え方法については以下で解説)

特に**読み込み時**において、(各フィールドが`null`の場合) 以下のような継承が行われます。それ以外は保存されているカラムの値を取得します。

* `_row_id`の継承: `_row_id` が `null` の場合、`_row_id = データファイルの first_row_id + ファイル内の行位置(_pos)` という計算で割り当てられる
* シーケンス番号の継承: `_last_updated_sequence_number` が `null` の場合、データファイルに関連するマニフェストファイルの `sequence_number` を割り当てる

補足: 既存行がコンパクションなどで別のデータファイルに移動される場合は、以下のルールに従います

* 既存の`null` でない `_row_id` を新しいデータファイルにコピー
* 行が変更された場合は、`_last_updated_sequence_number` を `null` に設定
* 行が変更されていない場合は、既存の`null`でない`_last_updated_sequence_number` の値をコピー

## 行レベルリネージの利用方法

行レベルリネージを利用するには、以下の 3 つの条件を満たす必要があります。

1. Iceberg テーブルのフォーマットバージョンを 3 に設定する
2. 使用するエンジンが行レベルリネージ機能をサポートしている
3. 行レベルリネージ機能を事前に有効化する (ただし、Iceberg 1.9.0 以降では[本機能の実装と有効化が必須](https://github.com/apache/iceberg/pull/12593)になったため、追加設定は必要ありません)

### 行レベルリネージの設定

Spark SQL を使用した設定例を示します:

```sql
CREATE TABLE my_catalog.db.sales_iceberg (
    product_name string,
    price decimal(10, 2),
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string)
USING iceberg
TBLPROPERTIES (
    'format-version'='3',
    'enable-row-lineage'='true' -- 行レベルリネージの有効化。Iceberg 1.9.0 より前で必要
)

-- 既存テーブルのフォーマットバージョンを 2 から 3 に設定し、行レベルリネージ機能を有効化する
ALTER TABLE my_catalog.db.sales_iceberg
SET TBLPROPERTIES (
    'format-version'='3',
    'enable-row-lineage'='true' -- 行レベルリネージの有効化。Iceberg 1.9.0 より前で必要
)
```

### 行レベルリネージの動作確認

実際に行レベルリネージがどのように動作するか確認します。まず、テストテーブルを作成します。

```sql
CREATE TABLE my_catalog.db.sales_iceberg (
    product_name string,
    price decimal(10, 2),
    customer_id bigint,
    order_id string,
    datetime timestamp,
    category string)
USING iceberg
TBLPROPERTIES ('format-version'='3')
```

空のテーブルに、以下の 3 レコードを追加します。

```sql
INSERT INTO my_catalog.db.sales_iceberg VALUES
    ('tomato juice', 2.00, 1698, 'DRE8DLTFNX0MLCE8DLTFNX0MLC', TIMESTAMP '2023-07-18T02:20:58Z', 'drink'),
    ('cocoa', 2.00, 1652, 'DR1UNFHET81UNFHET8', TIMESTAMP '2024-08-26T11:36:48Z', 'drink'),
    ('espresso', 2.00, 1037, 'DRBFZUJWPZ9SRABFZUJWPZ9SRA', TIMESTAMP '2024-04-19T12:17:22Z', 'drink'),
```

この時、Iceberg テーブルのメタデータ を確認しましょう。以下のように、`next-row-id` が `3` (3 レコードを追加したことによるもので、次の行 ID は 3 からを意味する)に、`first-row-id` が `0` (本スナップショットの最初の行 ID は 0 であることを意味する)に設定されていることを確認できます。

```json
// cat 00001-6fb7d714-e094-4500-96ef-fd266ba05985.metadata.json
{
  "format-version": 3,
  "table-uuid": "66895b5f-4c93-4d69-8890-f1de551dc77d",
  "location": "s3://amzn-s3-demo-bucket/db/sales_iceberg",
  "last-sequence-number": 1,
  // 省略 ...
  "current-snapshot-id": 5925759178589561078,
  "next-row-id": 3, // <= 次の行 ID は 3 から
  // 省略 ...
  "snapshots": [
    {
      "sequence-number": 1,
      "snapshot-id": 5925759178589561078,
  // 省略 ...
      "first-row-id": 0, // <= 本スナップショットの最初の行 ID は 0
      "added-rows": 3
    }
  ],
  // 省略 ...
```

追加でメタデータカラムの値についても確認してみましょう。以下の出力が得られます。

```sql
SELECT _row_id, _last_updated_sequence_number, * FROM my_catalog.db.sales_iceberg
/*
+-------+-----------------------------+------------+-----+-----------+--------------------+-------------------+--------+
|_row_id|_last_updated_sequence_number|product_name|price|customer_id|            order_id|           datetime|category|
+-------+-----------------------------+------------+-----+-----------+--------------------+-------------------+--------+
|      0|                            1|tomato juice| 2.00|       1698|DRE8DLTFNX0MLCE8D...|2023-07-18 02:20:58|   drink|
|      0|                            1|       cocoa| 2.00|       1652|  DR1UNFHET81UNFHET8|2024-08-26 11:36:48|   drink|
|      0|                            1|    espresso| 2.00|       1037|DRBFZUJWPZ9SRABFZ...|2024-04-19 12:17:22|   drink|
+-------+-----------------------------+------------+-----+-----------+--------------------+-------------------+--------+
*/
```

さらに、以下の`category=grocery`の 2 レコードを追加してみましょう。

```sql
INSERT INTO my_catalog.db.sales_iceberg VALUES
    ('broccoli', 1.00, 3092, 'GRK0L8ZQK0L8ZQ', TIMESTAMP '2023-03-22T18:48:04Z', 'grocery'),
    ('nutmeg', 1.00, 3512, 'GR15U0LKA15U0LKA', TIMESTAMP '2024-02-27T15:13:31Z', 'grocery')
```

同様に Iceberg テーブルメタデータを確認すると、`next-row-id` が `5` (2 レコードを追加したことによるもので、次の行 ID は 5 からを意味する)に、`first-row-id` が `3` (本スナップショットの最初の行 ID は 3 であることを意味する)に設定されています。

```json
// cat 00002-b877667e-dbd7-4e10-9897-dc81f6dda530.metadata.json
{
  "format-version": 3,
  "table-uuid": "66895b5f-4c93-4d69-8890-f1de551dc77d",
  "location": "s3://amzn-s3-demo-bucket/db/sales_iceberg",
  "last-sequence-number": 2,
  // 省略 ...
  "current-snapshot-id": 4937439540234442517,
  "next-row-id": 5, // <= 次の行 ID は 5 から
  // 省略 ...
  "snapshots": [
  // 省略 ...
    {
      "sequence-number": 2,
      "snapshot-id": 4937439540234442517,
      "parent-snapshot-id": 5925759178589561078,
  // 省略 ...
      "first-row-id": 3, // <= 本スナップショットの最初の行 ID は 3
      "added-rows": 2
    }
  ],
  // 省略 ...
```

この時のメタデータカラムの値については以下です。新たに追加された`category=grocery`の 2 レコードが前のスナップショットにおける `next-row-id` と同じ行 ID をもち、`_last_updated_sequence_number` は関連するスナップショットのものと一致します。

```sql
SELECT _row_id, _last_updated_sequence_number, * FROM my_catalog.db.sales_iceberg
/*
+-------+-----------------------------+------------+-----+-----------+--------------------+-------------------+--------+
|_row_id|_last_updated_sequence_number|product_name|price|customer_id|            order_id|           datetime|category|
+-------+-----------------------------+------------+-----+-----------+--------------------+-------------------+--------+
|      3|                            2|    broccoli| 1.00|       3092|      GRK0L8ZQK0L8ZQ|2023-03-22 18:48:04| grocery|
|      3|                            2|      nutmeg| 1.00|       3512|    GR15U0LKA15U0LKA|2024-02-27 15:13:31| grocery|
|      0|                            1|tomato juice| 2.00|       1698|DRE8DLTFNX0MLCE8D...|2023-07-18 02:20:58|   drink|
|      0|                            1|       cocoa| 2.00|       1652|  DR1UNFHET81UNFHET8|2024-08-26 11:36:48|   drink|
|      0|                            1|    espresso| 2.00|       1037|DRBFZUJWPZ9SRABFZ...|2024-04-19 12:17:22|   drink|
+-------+-----------------------------+------------+-----+-----------+--------------------+-------------------+--------+
*/
```

最後に、`product_name=cocoa`の金額を更新してみましょう。

```sql
UPDATE my_catalog.db.sales_iceberg SET price = 4.00
WHERE product_name = 'cocoa'
/* 更新後
+------------+-----+-----------+--------------------+-------------------+--------+
|product_name|price|customer_id|            order_id|           datetime|category|
+------------+-----+-----------+--------------------+-------------------+--------+
|    broccoli| 1.00|       3092|      GRK0L8ZQK0L8ZQ|2023-03-22 18:48:04| grocery|
|      nutmeg| 1.00|       3512|    GR15U0LKA15U0LKA|2024-02-27 15:13:31| grocery|
|tomato juice| 2.00|       1698|DRE8DLTFNX0MLCE8D...|2023-07-18 02:20:58|   drink|
|       cocoa| 4.00|       1652|  DR1UNFHET81UNFHET8|2024-08-26 11:36:48|   drink| <= 2.00 から 4.00 に更新された
|    espresso| 2.00|       1037|DRBFZUJWPZ9SRABFZ...|2024-04-19 12:17:22|   drink|
+------------+-----+-----------+--------------------+-------------------+--------+
*/
```

更新が完了後、Iceberg テーブルメタデータを確認してみましょう。1 行のみの更新ですが、`category=drink`の合計 3 レコードが含まれるファイルが新たに作成されたため、`next-row-id` が `8` (= 前回のRowID 5 + 今回追加レコード分3) に設定されます。

```json
// cat 00003-7b5e4d3e-c278-4bdb-83ec-b54117452a8c.metadata.json
{
  "format-version": 3,
  "table-uuid": "66895b5f-4c93-4d69-8890-f1de551dc77d",
  "location": "s3://amzn-s3-demo-bucket/db/sales_iceberg",
  "last-sequence-number": 3,
  // 省略 ...
  "current-snapshot-id": 8209300062153198183,
  "next-row-id": 8, // <= 次の行 ID は 8 から
  // 省略 ...
  "snapshots": [
  // 省略 ...
    {
      "sequence-number": 3,
      "snapshot-id": 8209300062153198183,
      "parent-snapshot-id": 4937439540234442517,
  // 省略 ...
      "first-row-id": 5, // <= 本スナップショットの最初の行 ID は 5
      "added-rows": 3
    }
  ],
  // 省略 ...
```

この時のメタデータカラムの値については以下です。変更後の値を含む`category=drink`に関するファイルが新たに作成されたため、`_row_id` が新たなファイルの `_row_id` に変更されています。

```sql
SELECT _row_id, _last_updated_sequence_number, * FROM my_catalog.db.sales_iceberg
/*
+-------+-----------------------------+------------+-----+-----------+--------------------+-------------------+--------+
|_row_id|_last_updated_sequence_number|product_name|price|customer_id|            order_id|           datetime|category|
+-------+-----------------------------+------------+-----+-----------+--------------------+-------------------+--------+
|      3|                            2|    broccoli| 1.00|       3092|      GRK0L8ZQK0L8ZQ|2023-03-22 18:48:04| grocery|
|      3|                            2|      nutmeg| 1.00|       3512|    GR15U0LKA15U0LKA|2024-02-27 15:13:31| grocery|
|      5|                            3|tomato juice| 2.00|       1698|DRE8DLTFNX0MLCE8D...|2023-07-18 02:20:58|   drink|
|      5|                            3|       cocoa| 4.00|       1652|  DR1UNFHET81UNFHET8|2024-08-26 11:36:48|   drink|
|      5|                            3|    espresso| 2.00|       1037|DRBFZUJWPZ9SRABFZ...|2024-04-19 12:17:22|   drink|
+-------+-----------------------------+------------+-----+-----------+--------------------+-------------------+--------+
*/
```

このようにテーブルメタデータにおける `snapshots` の部分と、メタデータカラムを照らし合わせることで、各行に対してどのような変更があったかを追跡することができます。

## 考慮事項

* 等価条件削除 (Equality Delete)については行レベルリネージを利用できません。これは、エンジンが既存データを読まずに変更を書き込むため、元の行 ID を作成できないためです
* 2025年8月18日時点において、Flink では行レベルリネージを有効化した場合のコンパクションをサポートしていません
