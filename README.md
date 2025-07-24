# 実践 Apache Iceberg —— ハンズオン環境

## ハンズオン環境について

本書では、Apache Iceberg の機能や特性を実際に操作しながら理解できるように、Icebergを簡単に試すことができるコンテナベースのハンズオン環境を用意しています。この環境を使って、Iceberg テーブルの作成や操作、複数エンジンからのクエリ実行などを体験できます。この環境はコンテナが動作するマシンと、関連するライブラリをダウンロードできるネットワークさえあれば、簡単なコマンドで立ち上げることができます。  

以降の各章では、適宜このハンズオン環境を使って実際の操作を行う手順を解説するので、ぜひ実際に手を動かしながら理解を深めてみてください。

【注意】この環境はあくまでも実験用で、最小限のマシンリソースで簡易的な検証を実現することに主眼を置いています。本番環境での運用を想定した設計ではないため、本番向けには非機能面を考慮した適切な設計の検討が必要です

## ハンズオン環境のリポジトリ
本書で使用するハンズオン環境は、GitHub 上のリポジトリに格納されています。リポジトリの URL は以下の通りです。
TODO: リポジトリの URL を記載する

## ハンズオン環境の構成
Icebergの実験に必要なコンポーネントがdocker-compose.ymlに纏まっており、簡単に実験環境を構築できます。  
以下のコンテナによって構成されています。  

- spark-iceberg
  - Spark（Iceberg対応のビルド）と Jupyter Notebook を動かすコンテナです。
  - Jupyter Notebookを使って、Spark 上で Iceberg テーブルを扱う実行例を確認できます。
  - 8888 番ポートで Jupyter Notebook が起動し、8080 で Spark の Web UI にアクセスできます。
- iceberg-rest
  - Iceberg のRESTカタログを起動するコンテナです。Iceberg のメタデータ管理を REST API を介して行うカタログ機能を提供します。
  - 8181 番ポートで起動し、Spark や Trino などから Iceberg Catalog として参照されます。
- minio
  - オブジェクトストレージを提供するコンテナです。
  - ローカル環境で Amazon S3 互換のストレージとして動作します。
  - 9000 番ポートが S3 API のエンドポイント、9001 番ポートで MinIO の管理用コンソール (Web UI) にアクセスできます。
  - データの永続化先として /data を用い、**amzn-s3-demo-bucket** というバケットに Iceberg テーブルのファイルを保存します。
- trino
  - 分散SQLクエリエンジン Trino を起動するコンテナです。
  - Iceberg の REST Catalog を参照しながら、Trino 経由でも Iceberg テーブルに対してクエリを実行することができます。
  - 8085 番ポートでクエリ用の HTTP インターフェースにアクセスできます (CLI や JDBCドライバからの利用を想定)。
- kafka
  - 分散方メッセージングシステム Apache Kafka を起動するコンテナです。
  - ローカル環境で Kafka クラスターが起動します。

これらのコンテナは同一のDockerネットワーク(handson_net)上で連携し、Iceberg テーブルを操作できる実行環境を提供します。

全体としては、以下のような構成になっています。

<!-- TODO: Add a figure to describe the system -->

## ハンズオン環境のセットアップ
### 前提条件
- Docker や Podman などのコンテナランタイムが利用可能であること
- Docker Compose や Podman Compose などのツールが利用可能であること
- インターネットに接続可能であること

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
> docker-compose.yml内にAWSのクレデンシャルを指定するように見える箇所がありますが、これはminioのために必要な設定で、編集は不要です(本ハンズオンではS3を使用しません)

### JupyterのUIを開いてみよう

ブラウザからlocalhost:8888にアクセスしてください。無事に環境が構築できていれば、JupyterのUIが表示されるはずです。 ハンズオンの手順はNotebookとして配置されており、ハンズオンではこれらを利用していきます。

### MinIOのUIを開いてみよう

localhost:9001にアクセスするとMinIOのログイン画面が表示されます。  

UsernameとPasswordはdocker-compose.ymlに記載の通り、Username=admin, Password=passwordです。(適宜変更してください)  

ログインすると以下の画面が表示されます。  

<!-- TODO: Add a screenshot of the login view -->

### まとめ

以上が本書で利用するハンズオン環境の概要です。本書の全体を通じて、この環境を使ってIceberg の機能を体験していきます。  
