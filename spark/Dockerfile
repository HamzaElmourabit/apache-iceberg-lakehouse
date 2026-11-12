FROM apache/spark:3.5.3

USER root

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    && chmod 644 /opt/spark/jars/hadoop-aws-3.3.4.jar \
    && chmod 644 /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER spark