# メタデータテーブルクエリ例

## 過去数日間のテーブル履歴の確認

シンプルな例として1つのメタデータテーブルに対するクエリ例を確認しましょう。以下の例では、`history`メタデータテーブルに対してクエリを実行し、過去7日間のテーブル操作履歴を取得しています。

```sql
-- 過去7日間のテーブル操作履歴を取得
SELECT 
  made_current_at,
  snapshot_id,
  parent_id,
  is_current_ancestor
FROM my_catalog.db.sales_iceberg.history
WHERE made_current_at > current_timestamp - INTERVAL 7 DAYS
ORDER BY made_current_at DESC;
```

以下に上記クエリを実行した出力例を示します。

```
-RECORD 0--------------------------------------
 made_current_at     | 2025-07-19 08:24:03.602
 snapshot_id         | 5718193762120521294
 parent_id           | 4505228428385620397
 is_current_ancestor | true
-RECORD 1--------------------------------------
 made_current_at     | 2025-07-19 08:18:10.878
 snapshot_id         | 589802713479450036
 parent_id           | 5718193762120521294
 is_current_ancestor | false  # ロールバックされたスナップショット
...
```

本クエリを実行することで7日間におけるテーブルの更新履歴を確認できる上、`is_current_ancestor`を確認することでテーブルロールバックが行われたかどうか確認できます。具体的には、本カラムが`false`にセットされているスナップショットは、ロールバックされたものであることを確認できます。

## パーティションごとのファイルサイズの状況とコンパクション必要性の判定

`files`と`partitions`メタデータテーブルを活用し、各パーティションごとのファイルサイズの状況を確認します。これによって、コンパクションが必要なパーティションを判断できるようにします。本クエリを定期的に実行することで、どのタイミングで、どのパーティションを対象にコンパクションを実行すべきか判断する指標を得ることができます。

```sql
WITH file_analysis AS (
  SELECT 
    f.partition,
    COUNT(*) as file_count,
    AVG(f.file_size_in_bytes) / 1048576 as avg_file_size_mb,
    MIN(f.file_size_in_bytes) / 1048576 as min_file_size_mb,
    MAX(f.file_size_in_bytes) / 1048576 as max_file_size_mb,
    SUM(f.file_size_in_bytes) / 1073741824 as total_size_gb,
    p.record_count,
    p.position_delete_record_count,
    p.position_delete_record_count::float / NULLIF(p.record_count, 0) as delete_ratio
  FROM my_catalog.db.sales_iceberg.files f
  JOIN my_catalog.db.sales_iceberg.partitions p
    ON f.partition = p.partition
  WHERE f.content = 0  -- データファイルのみを確認する
  GROUP BY f.partition, p.record_count, p.position_delete_record_count
)
SELECT 
  partition,
  file_count,
  avg_file_size_mb,
  total_size_gb,
  delete_ratio,
  CASE 
    WHEN avg_file_size_mb < 100 AND file_count > 10 THEN 'Recommmended (data file)'
    WHEN delete_ratio > 0.3 THEN 'Recommended (delete file)'
    ELSE 'No action needed'
  END as recommendation
FROM file_analysis
ORDER BY avg_file_size_mb ASC, file_count DESC;
```

以下に上記クエリを実行した結果を示します。

```
+----------------+----------+--------------------+---------------------+------------+----------------+
|partition       |file_count|avg_file_size_mb    |total_size_gb        |delete_ratio|recommendation  |
+----------------+----------+--------------------+---------------------+------------+----------------+
|{2023, 11, 26}  |2         |0.010484695434570312|2.047792077064514E-5 |0.0         |No action needed|
|{2023, 12, 2}   |1         |0.010575294494628906|1.0327436029911041E-5|0.0         |No action needed|
|{2024, 5, 8}    |2         |0.010732650756835938|2.096220850944519E-5 |0.0         |No action needed|
|{2023, 11, 29}  |2         |0.010815620422363281|2.1124258637428284E-5|0.0         |No action needed|
|{2024, 10, 10}  |1         |0.010943412780761719|1.0686926543712616E-5|0.0         |No action needed|
+----------------+----------+--------------------+---------------------+------------+----------------+
```