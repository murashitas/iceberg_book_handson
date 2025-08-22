# 実践 Apache Iceberg —— ハンズオン環境と付録

<a href=https://gihyo.jp/book/2025/978-4-297-15074-7><img src="https://gihyo.jp/assets/images/cover/2025/9784297150747.jpg" alt="実践 Apache Iceberg" height="256px"></a>

『実践 Apache Iceberg』のハンズオン環境と、付録コンテンツを提供するリポジトリです。

- ハンズオン環境：以下のREADME.mdを参照してください
- 付録コンテンツ：[appendix](./appendix/README.md)を参照してください

## ハンズオン環境について

本書では、Apache Iceberg の機能や特性を実際に操作しながら理解できるように、Icebergを簡単に試すことができるコンテナベースのハンズオン環境を用意しています。この環境を使って、Iceberg テーブルの作成や操作、複数エンジンからのクエリ実行などを体験できます。この環境はコンテナが動作するマシンと、関連するライブラリをダウンロードできるネットワークさえあれば、簡単なコマンドで立ち上げることができます。  

各章では、適宜このハンズオン環境を使ってIcebergの機能を実際に体験するパートを設けています。
ぜひ実際に手を動かしながら理解を深めてみてください。すべての手順は外部のサービスに依存せずに実行可能です。

> [!WARNING]
> 本環境はあくまでも実験用で、最小限のマシンリソースで簡易的な検証を実現することに主眼を置いています。本番環境での運用を想定した設計ではないため、本番向けにはパフォーマンスやセキュリティ、信頼性などの非機能面を考慮した追加の設計の検討が必要である点に注意してください。

## ハンズオンコンテンツ一覧

各章とハンズオンコンテンツの対応は以下の通りです。

#### 2章 Apache Icebergの仕組みと機能
- [examples/ch2-query-lifecycle.ipynb](examples/ch2-query-lifecycle.ipynb)
#### 3章 Icebergカタログとストレージ
- [examples/ch3-rest-catalog.ipynb](examples/ch3-rest-catalog.ipynb)
#### 4章 Apache Spark
- [examples/ch4-spark-1.ipynb](examples/ch4-spark-1.ipynb)
- [examples/ch4-spark-2-iceberg-example.ipynb](examples/ch4-spark-2-iceberg-example.ipynb)
- [examples/ch4-spark-3-iceberg.ipynb](examples/ch4-spark-3-iceberg.ipynb)
#### 5章 Apache Flink
- [examples/ch5-flink-sql.md](examples/ch5-flink-sql.md)
#### 6章 Trino
- [examples/ch6-trino.ipynb](examples/ch6-trino.ipynb)
#### 7章 Apache Hive
- [examples/ch7-hive.md](examples/ch7-hive.md)
#### 8章 PyIceberg
- [examples/ch8-pyiceberg.ipynb](examples/ch8-pyiceberg.ipynb)
#### 9章 ユースケースとソリューションパターン
- [examples/ch9-basic-pipeline.ipynb](examples/ch9-basic-pipeline.ipynb)
- [examples/ch9-cdc.ipynb](examples/ch9-cdc.ipynb)
- [examples/ch9-scd-type2.ipynb](examples/ch9-scd-type2.ipynb)
- [examples/ch9-wap.ipynb](examples/ch9-wap.ipynb)
- [examples/ch9-streaming.ipynb](examples/ch9-streaming.ipynb)
#### 11章 運用管理
- [examples/ch11-data-life-cycle.ipynb](./examples/ch11-data-life-cycle.ipynb)
- [examples/ch11-iceberg-metrics.ipynb](./examples/ch11-iceberg-metrics.ipynb)

## ハンズオン環境の構成

Icebergの実験に必要なコンポーネントが`docker-compose.yml`に纏まっています。
主に以下のコンテナによって構成されています。  

* **jupyter**
   * PySpark、PyIcebergとJupyter Notebookを動かすコンテナです
   * 8888番ポートでJupyter Notebookが起動します
   * examplesディレクトリとdataディレクトリがマウントされ、ハンズオン用のノートブックを実行できます

* **iceberg-rest**
   * IcebergのRESTカタログを起動するコンテナです。Icebergのメタデータ管理をREST APIを介して行うカタログ機能を提供します
   * 8181番ポートで起動し、各種エンジンからIceberg Catalogとして参照されます

* **minio**
   * オブジェクトストレージを提供するコンテナです
   * ローカル環境でAmazon S3互換のストレージとして動作します
   * 9000番ポートでS3 APIのエンドポイント、amzn-s3-demo-bucketがデフォルトで作成されます

* **minio-console**
   * MinIOのWebコンソールUIを提供するコンテナです
   * 9001番ポートでMinIOのWeb管理画面にアクセスできます

* **flink-jobmanager / flink-taskmanager / flink-sql-client**
   * Apache Flinkのクラスタを構成するコンテナ群です
   * SQL Clientを通じてIcebergテーブルへのストリーミング読み書きが可能です

* **trino**
   * 分散SQLクエリエンジンTrinoを起動するコンテナです
   * 8085番ポートでクエリ用のHTTPインターフェースにアクセスできます
   * 動的カタログ管理が有効化されています

* **hive**
   * Apache Hive, Hive メタストアサービスを提供するコンテナです
   * 10000番ポートでHiveServer2、10002番ポートでWeb UIにアクセスできます
   * Icebergテーブルの操作やメタデータ管理に利用されます

* **superset**
   * Apache Supersetを起動し、データの可視化とダッシュボード機能を提供します
   * 8088番ポートでWeb UIにアクセスできます（admin/adminでログイン）
   * Trinoと連携してIcebergテーブルのデータを可視化できます

* **kafka**
   * 分散メッセージングシステムApache Kafkaを起動するコンテナです
   * 9092番ポートでKafkaブローカーにアクセスできます
   * リアルタイムデータパイプラインの構築に利用されます

これらのコンテナは同一のネットワーク(iceberg_handson_net)上で連携し、Icebergテーブルを複数のエンジンから操作できる包括的な実行環境を提供します。

## ハンズオン環境のセットアップ
### 前提条件
- Docker Engine または Podman などのコンテナランタイムがインストールされていること
- Docker Composeまたは Podman Composeが利用可能であること
- コンテナイメージのダウンロードのためインターネット接続が可能であること

### リポジトリのクローン
GitHub リポジトリをクローンする
```bash
git clone https://github.com/murashitas/iceberg_book_handson.git && cd iceberg_book_handson
```

### コンテナ立ち上げ

```bash
docker-compose up --build
```

> [!WARNING]
> docker-compose.yml内にAWSのクレデンシャルを指定するように見える箇所がありますが、これはminioをローカルで稼働させるために必要な設定で、編集は不要です(本ハンズオンではAmazon S3を使用しません)

### Jupyterにアクセス
ブラウザから`localhost:8888`にアクセスしてください。無事に環境が構築できていれば、JupyterのUIが表示されるはずです。 ハンズオンの手順はNotebookフォルダに配置されており、ハンズオンではこれらを利用していきます。

### MinIO object browser
本環境で扱うIcebergテーブルのデータは、S3互換のオブジェクトストレージであるMinIOに保存されます。MinIOはオブジェクトをUIから確認できる「オブジェクトブラウザ」を提供しています。  

`localhost:9001`にアクセスするとログイン画面が表示されます。  

UsernameとPasswordはdocker-compose.ymlに記載の通り、Username=admin, Password=passwordです。(適宜変更してください)

## MinIO にファイルをアップロードする

本書におけるいくつかの例において、事前にサンプルファイルをMinIO object browser経由でアップロードする必要があります。以下のステップの通りにパスの作成およびファイルのアップロードを行ってください。

1. ローカルのWebブラウザ経由で`localhost:9001`にアクセスし、ログインします
2. ログイン後、画面右上に表示されている**Create new path**より、バケット配下にパスを作成できます
3. 画面右上に表示されている**Upload**を選択し、**Upload File**を選択します。その後アップロードするファイルを選択することで、現在のパス配下にファイルをアップロードできます

## QA
### テスト環境が正常に動作しない
何らかの理由で、Icebergカタログ等に不整合が発生している可能性があります。以下のコマンドでコンテナを停止し、再構築を行ってください。

```bash
docker-compose down
docker-compose up --build
```

不明点や、問題が発生した場合は、GitHubのIssuesを起票してください。