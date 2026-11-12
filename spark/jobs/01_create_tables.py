
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("IcebergMinIOTest")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        "spark.sql.catalog.local.type",
        "hadoop",
    )
    .config(
        "spark.sql.catalog.local.warehouse",
        "s3a://warehouse",
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000",
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "minioadmin",
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "minioadmin",
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true",
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false",
    )
    .getOrCreate()
)


print("========================================")
print("Spark + Iceberg + MinIO")
print("========================================")


# Create Iceberg table
spark.sql("""
CREATE TABLE IF NOT EXISTS local.tickets (
    id INT,
    title STRING,
    priority STRING,
    status STRING
)
USING iceberg
""")


print("========================================")
print("ICEBERG TABLE CREATED SUCCESSFULLY")
print("========================================")


# Verify the table structure
print("========================================")
print("ICEBERG TABLE STRUCTURE")
print("========================================")

spark.sql("DESCRIBE TABLE local.tickets").show(truncate=False)


# Verify that the table can be queried
print("========================================")
print("ICEBERG TABLE CONTENT")
print("========================================")

spark.sql("SELECT * FROM local.tickets").show()


spark.stop()

