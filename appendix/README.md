# 実践 Apache Iceberg —— 付録

本文の内容を補完する付録を用意しました。本書の内容と併せてご覧ください。

- [Icebergテーブルのサンプルメタデータ](./sample-iceberg-table/)
  - メタデータファイル
    - [00000-c27acb38-1835-4c8d-b6ad-38a09d8d5151.metadata.json](./sample-iceberg-table/simple_table/metadata/00000-c27acb38-1835-4c8d-b6ad-38a09d8d5151.metadata.json)
    - [00001-df0aafb6-54e8-451e-92c6-363e55e43bdc.metadata.json](./sample-iceberg-table/simple_table/metadata/00001-df0aafb6-54e8-451e-92c6-363e55e43bdc.metadata.json)
  - マニフェストリスト
    - [snap-2170363704577455857-1-18e47ad9-6557-47f0-ba53-f855647ab401.avro](./sample-iceberg-table/simple_table/metadata/snap-2170363704577455857-1-18e47ad9-6557-47f0-ba53-f855647ab401.avro)
  - マニフェストファイル
    - [18e47ad9-6557-47f0-ba53-f855647ab401-m0.avro](./sample-iceberg-table/simple_table/metadata/18e47ad9-6557-47f0-ba53-f855647ab401-m0.avro)
- [Icebergビューのサンプルデータ](./sample-iceberg-view/sample_view/metadata/00000-bcbc7253-437c-447b-8c75-fba0d6313eac.gz.metadata.json)
- [Iceberg テーブルのフィールドリスト](appendix/iceberg-table-spec-detail/table_field_list.md)
- [Iceberg フォーマットバージョン V3: Row Lineage (行レベルリネージ)](./row-lineage.md)
- [Iceberg フォーマットバージョン V3: Deletion Vectors (削除ベクトル)](./deletion-vectors.md)
- [メタデータテーブルクエリ例](./metadata-table-query-examples.md)
