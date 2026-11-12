from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


spark = (
    SparkSession.builder
    .appName("OnlineRetailBronze")
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


print("=" * 70)
print("ONLINE RETAIL II - BRONZE INGESTION")
print("=" * 70)


# ---------------------------------------------------------
# 1. Read CSV
# ---------------------------------------------------------

input_path = "/opt/spark/data/online_retail_II.csv"

print("\nReading source dataset:")
print(input_path)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)


print("\nSource schema:")
df.printSchema()


print("\nNumber of rows:")
print(f"{df.count():,}")


# ---------------------------------------------------------
# 2. Add ingestion metadata
# ---------------------------------------------------------

bronze_df = (
    df
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    .withColumn(
        "source_file",
        lit("online_retail_II.csv")
    )
)


# ---------------------------------------------------------
# 3. Create Bronze Iceberg table
# ---------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS local.online_retail_bronze (
    Invoice STRING,
    StockCode STRING,
    Description STRING,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    Price DOUBLE,
    `Customer ID` DOUBLE,
    Country STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
USING iceberg
""")

print("\nBronze Iceberg table created/verified.")


# ---------------------------------------------------------
# 4. Insert data into Iceberg
# ---------------------------------------------------------

bronze_df.writeTo(
    "local.online_retail_bronze"
).overwrite(lit(True))


# ---------------------------------------------------------
# 5. Verification
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("BRONZE TABLE VERIFICATION")
print("=" * 70)

count = spark.sql("""
SELECT COUNT(*)
FROM local.online_retail_bronze
""").collect()[0][0]

print(f"\nBronze rows: {count:,}")


print("\nSample records:")

spark.sql("""
SELECT *
FROM local.online_retail_bronze
LIMIT 10
""").show(
    truncate=False
)


print("\nBronze schema:")

spark.sql("""
DESCRIBE TABLE local.online_retail_bronze
""").show(
    truncate=False
)


print("\n" + "=" * 70)
print("BRONZE INGESTION COMPLETED SUCCESSFULLY")
print("=" * 70)


spark.stop()