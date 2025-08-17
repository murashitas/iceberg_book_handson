# Iceberg フォーマットバージョン 3: Row Lineage (行レベルリネージ)

行レベルリネージ (Row Linage)は、Iceberg テーブル内の各行の変更を追跡し、バージョン管理を行うために仕組みです。この仕様はプルリクエスト [#11130](https://github.com/apache/iceberg/pull/11130)で確定され、バージョン 3 以降で各エンジンがサポートすべき必須機能となっています。本機能により、Iceberg テーブルに対する CDC (Change Data Capture)や増分データ処理などの実現が期待されています。Iceberg (Java) 1.8.0 以上で利用可能です。なお行レベルリネージの詳細については Iceberg 公式ドキュメントの [Row Lineage](https://iceberg.apache.org/spec/#row-lineage)も参照してください。

## 行レベルリネージの仕組み

行レベルリネージは以下 2 つのメタデータフィールドで構成され、それぞれを **Inheritance (継承)**という仕組みで管理します:

1. `_row_id`: テーブル内の各行に割り当てられる一意の整数識別子で、行が最初にテーブルに追加された際に継承により割り当てられる
2. `_last_updated_sequence_number`: 行を最後に更新したコミットのシーケンス番号で、行の追加や変更時に継承により割り当てられる

### 各メタデータフィールド割り当ての仕組み

書き込み時 (行が追加または変更される場合):

1. `_last_updated_sequence_number` フィールドは `null` に設定（読み込み時に継承)
2. 新規行の `_row_id` フィールドは `null` に設定（読み込み時に割り当て)
3. 新規行のみのデータファイルでは、両フィールドを省略可能

読み込み時: 各フィールドが`null`の場合継承が行われます。それ以外は保存されているカラムの値を取得します

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

## 考慮事項

* 等価条件削除 (Equality Delete)については行レベルリネージを利用できません。これは、エンジンが既存データを読まずに変更を書き込むため、元の Row ID を作成できないためです
* 2025年8月18日時点において、Flink では Row Linage を有効化した場合のコンパクションをサポートしていません
