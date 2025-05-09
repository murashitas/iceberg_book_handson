FROM flink:1.20.1-scala_2.12

WORKDIR /opt/flink

RUN apt-get update && apt-get install -y wget curl

# Put the iceberg catalog initialization file
COPY ./setup/init.sql /opt/flink/init.sql

# Iceberg dependencies
RUN mkdir -p ./lib/iceberg && \
    curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.8.1/iceberg-flink-runtime-1.20-1.8.1.jar -o ./lib/iceberg/iceberg-flink-runtime-1.20-1.8.1.jar && \
    curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.8.1/iceberg-aws-bundle-1.8.1.jar -o ./lib/iceberg/iceberg-aws-bundle-1.8.1.jar

# Hadoop dependencies
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -o ./lib/hadoop-aws-3.3.2.jar
RUN mkdir -p ./lib/hive && \
    curl https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/5.3.0/woodstox-core-5.3.0.jar -o ./lib/hive/woodstox-core-5.3.0.jar && \
    curl https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar -o ./lib/hive/commons-logging-1.1.3.jar && \
    curl https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.1.1/commons-configuration2-2.1.1.jar -o ./lib/hive/commons-configuration2-2.1.1.jar && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.2/hadoop-auth-3.3.2.jar -o ./lib/hive/hadoop-auth-3.3.2.jar && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.2/hadoop-common-3.3.2.jar -o ./lib/hive/hadoop-common-3.3.2.jar && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.2/hadoop-hdfs-client-3.3.2.jar -o ./lib/hive/hadoop-hdfs-client-3.3.2.jar && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.2/hadoop-mapreduce-client-core-3.3.2.jar -o ./lib/hive/hadoop-mapreduce-client-core-3.3.2.jar && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar -o ./lib/hive/hadoop-shaded-guava-1.1.1.jar && \
    curl https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar -o ./lib/hive/stax2-api-4.2.1.jar

# S3 dependencies
RUN curl https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.1/flink-s3-fs-hadoop-1.20.1.jar -o ./lib/flink-s3-fs-hadoop-1.20.1.jar

# hadoop-aws
RUN mkdir -p /opt/flink/conf
RUN echo "<configuration>\
    <property>\
        <name>fs.s3a.endpoint</name>\
        <value>http://minio:9000</value>\
    </property>\
    <property>\
        <name>fs.s3a.access.key</name>\
        <value>admin</value>\
    </property>\
    <property>\
        <name>fs.s3a.secret.key</name>\
        <value>password</value>\
    </property>\
    <property>\
        <name>fs.s3a.path.style.access</name>\
        <value>true</value>\
    </property>\
    <property>\
        <name>fs.s3a.impl</name>\
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>\
    </property>\
    <property>\
        <name>fs.s3a.aws.credentials.provider</name>\
        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>\
    </property>\
</configuration>" > /opt/flink/conf/core-site.xml
