FROM apache/hive:4.0.1

USER root

RUN apt-get update && apt-get install -y curl

RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar -o /opt/hive/lib/hadoop-aws-3.3.6.jar && \
    curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.4.3/iceberg-hive-runtime-1.4.3.jar -o /opt/hive/lib/iceberg-hive-runtime-1.4.3.jar && \
    curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar -o /opt/hive/lib/iceberg-aws-bundle-1.4.3.jar && \
    curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.31.3/bundle-2.31.3.jar -o /opt/hive/lib/bundle-2.31.3.jar && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar -o /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar

USER hive
