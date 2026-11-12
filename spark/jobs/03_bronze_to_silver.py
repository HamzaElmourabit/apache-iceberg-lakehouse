from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    regexp_replace,
    count,
    sum,
    lit,
)


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("OnlineRetailBronzeToSilver")
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


spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("ONLINE RETAIL II - BRONZE → SILVER")
print("=" * 70)


# =========================================================
# 1. READ BRONZE
# =========================================================

print("\nReading Bronze table...")

bronze_df = spark.table(
    "local.online_retail_bronze"
)


print("\nBronze schema:")

bronze_df.printSchema()


bronze_count = bronze_df.count()

print(f"\nBronze rows: {bronze_count:,}")


# =========================================================
# 2. CLEAN COLUMN NAMES / BASIC CLEANING
# =========================================================

print("\nCleaning data...")


silver_df = (
    bronze_df

    # -----------------------------------------------------
    # Description
    # -----------------------------------------------------
    .withColumn(
        "Description",
        trim(col("Description"))
    )

    # -----------------------------------------------------
    # Country
    # -----------------------------------------------------
    .withColumn(
        "Country",
        trim(col("Country"))
    )

    # -----------------------------------------------------
    # StockCode
    # -----------------------------------------------------
    .withColumn(
        "StockCode",
        trim(upper(col("StockCode")))
    )

    # -----------------------------------------------------
    # Invoice
    # -----------------------------------------------------
    .withColumn(
        "Invoice",
        trim(col("Invoice"))
    )

    # -----------------------------------------------------
    # Customer ID
    # Convert DOUBLE → BIGINT
    # -----------------------------------------------------
    .withColumn(
        "Customer_ID",
        col("Customer ID").cast("BIGINT")
    )

    # -----------------------------------------------------
    # Transaction type
    # -----------------------------------------------------
    .withColumn(
        "transaction_type",
        when(
            col("Quantity") < 0,
            "RETURN"
        ).otherwise(
            "SALE"
        )
    )

    # -----------------------------------------------------
    # Revenue
    # -----------------------------------------------------
    .withColumn(
        "Revenue",
        col("Quantity") * col("Price")
    )
)


# =========================================================
# 3. REMOVE INVALID RECORDS
# =========================================================

print("\nRemoving invalid records...")


silver_df = silver_df.filter(
    col("Invoice").isNotNull()
    & col("StockCode").isNotNull()
    & col("InvoiceDate").isNotNull()
    & col("Quantity").isNotNull()
    & col("Price").isNotNull()
)


# =========================================================
# 4. REMOVE INVALID PRICES
# =========================================================

silver_df = silver_df.filter(
    col("Price") >= 0
)


# =========================================================
# 5. REMOVE ZERO QUANTITY
# =========================================================

silver_df = silver_df.filter(
    col("Quantity") != 0
)


# =========================================================
# 6. REMOVE DUPLICATES
# =========================================================

silver_df = silver_df.dropDuplicates(
    [
        "Invoice",
        "StockCode",
        "InvoiceDate",
        "Quantity",
        "Price",
        "Customer_ID",
    ]
)


# =========================================================
# 7. SELECT FINAL SILVER COLUMNS
# =========================================================

silver_df = silver_df.select(
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer_ID",
    "Country",
    "Revenue",
    "transaction_type",
    "ingestion_timestamp",
    "source_file",
)


# =========================================================
# 8. CREATE SILVER ICEBERG TABLE
# =========================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS local.online_retail_silver (
    Invoice STRING,
    StockCode STRING,
    Description STRING,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    Price DOUBLE,
    Customer_ID BIGINT,
    Country STRING,
    Revenue DOUBLE,
    transaction_type STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
USING iceberg
""")


print("\nSilver Iceberg table created/verified.")


# =========================================================
# 9. WRITE SILVER
# =========================================================

silver_df.writeTo(
    "local.online_retail_silver"
).overwrite(lit(True))


# =========================================================
# 10. VERIFICATION
# =========================================================

print("\n" + "=" * 70)
print("SILVER TABLE VERIFICATION")
print("=" * 70)


silver_count = spark.sql("""
SELECT COUNT(*)
FROM local.online_retail_silver
""").collect()[0][0]


print(f"\nSilver rows: {silver_count:,}")


print("\nSilver schema:")

spark.sql("""
DESCRIBE TABLE local.online_retail_silver
""").show(
    truncate=False
)


print("\nSample Silver records:")

spark.sql("""
SELECT *
FROM local.online_retail_silver
LIMIT 10
""").show(
    truncate=False
)


# =========================================================
# 11. DATA QUALITY CHECKS
# =========================================================

print("\n" + "=" * 70)
print("SILVER DATA QUALITY")
print("=" * 70)


print("\nTransaction types:")

spark.sql("""
SELECT
    transaction_type,
    COUNT(*) AS records,
    ROUND(SUM(Revenue), 2) AS total_revenue
FROM local.online_retail_silver
GROUP BY transaction_type
ORDER BY transaction_type
""").show(
    truncate=False
)


print("\nCountries:")

spark.sql("""
SELECT
    Country,
    COUNT(*) AS transactions,
    ROUND(SUM(Revenue), 2) AS revenue
FROM local.online_retail_silver
GROUP BY Country
ORDER BY revenue DESC
LIMIT 10
""").show(
    truncate=False
)


print("\nNull checks:")

spark.sql("""
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Invoice IS NULL THEN 1 ELSE 0 END) AS null_invoice,
    SUM(CASE WHEN StockCode IS NULL THEN 1 ELSE 0 END) AS null_stockcode,
    SUM(CASE WHEN InvoiceDate IS NULL THEN 1 ELSE 0 END) AS null_invoice_date,
    SUM(CASE WHEN Quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity,
    SUM(CASE WHEN Price IS NULL THEN 1 ELSE 0 END) AS null_price,
    SUM(CASE WHEN Country IS NULL THEN 1 ELSE 0 END) AS null_country
FROM local.online_retail_silver
""").show(
    truncate=False
)


print("\n" + "=" * 70)
print("BRONZE → SILVER COMPLETED SUCCESSFULLY")
print("=" * 70)


spark.stop()