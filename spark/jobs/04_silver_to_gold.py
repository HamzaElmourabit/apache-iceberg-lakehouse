import os
import csv
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("OnlineRetailSilverToGold")
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
print("ONLINE RETAIL II - SILVER → GOLD")
print("=" * 70)


# =========================================================
# 1. READ SILVER
# =========================================================

print("\nReading Silver table...")

silver_df = spark.table(
    "local.online_retail_silver"
)


print("\nSilver schema:")

silver_df.printSchema()


silver_count = silver_df.count()

print(f"\nSilver rows: {silver_count:,}")


# =========================================================
# 2. CREATE GOLD TABLES
# =========================================================

print("\nCreating Gold Iceberg tables...")


# ---------------------------------------------------------
# GOLD 1 - SALES BY COUNTRY
# ---------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS local.gold_sales_by_country (
    Country STRING,
    total_quantity BIGINT,
    total_revenue DOUBLE,
    invoice_count BIGINT,
    customer_count BIGINT
)
USING iceberg
""")


# ---------------------------------------------------------
# GOLD 2 - SALES BY MONTH
# ---------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS local.gold_sales_by_month (
    year INT,
    month INT,
    total_quantity BIGINT,
    total_revenue DOUBLE,
    invoice_count BIGINT,
    customer_count BIGINT
)
USING iceberg
""")


# ---------------------------------------------------------
# GOLD 3 - TOP PRODUCTS
# ---------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS local.gold_top_products (
    StockCode STRING,
    Description STRING,
    total_quantity BIGINT,
    total_revenue DOUBLE,
    invoice_count BIGINT
)
USING iceberg
""")


print("Gold tables created/verified.")


# =========================================================
# 3. GOLD - SALES BY COUNTRY
# =========================================================

print("\n" + "=" * 70)
print("BUILDING GOLD SALES BY COUNTRY")
print("=" * 70)


gold_country_df = spark.sql("""
SELECT
    Country,

    SUM(Quantity) AS total_quantity,

    ROUND(
        SUM(Revenue),
        2
    ) AS total_revenue,

    COUNT(DISTINCT Invoice) AS invoice_count,

    COUNT(DISTINCT Customer_ID) AS customer_count

FROM local.online_retail_silver

WHERE Country IS NOT NULL

GROUP BY Country

ORDER BY total_revenue DESC
""")


print("\nTop countries:")

gold_country_df.show(
    10,
    truncate=False
)


# =========================================================
# WRITE GOLD COUNTRY
# =========================================================

gold_country_df.writeTo(
    "local.gold_sales_by_country"
).overwrite(lit(True))


# =========================================================
# 4. GOLD - SALES BY MONTH
# =========================================================

print("\n" + "=" * 70)
print("BUILDING GOLD SALES BY MONTH")
print("=" * 70)


gold_month_df = spark.sql("""
SELECT
    YEAR(InvoiceDate) AS year,

    MONTH(InvoiceDate) AS month,

    SUM(Quantity) AS total_quantity,

    ROUND(
        SUM(Revenue),
        2
    ) AS total_revenue,

    COUNT(DISTINCT Invoice) AS invoice_count,

    COUNT(DISTINCT Customer_ID) AS customer_count

FROM local.online_retail_silver

WHERE InvoiceDate IS NOT NULL

GROUP BY
    YEAR(InvoiceDate),
    MONTH(InvoiceDate)

ORDER BY
    year,
    month
""")


print("\nMonthly sales:")

gold_month_df.show(
    24,
    truncate=False
)


# =========================================================
# WRITE GOLD MONTH
# =========================================================

gold_month_df.writeTo(
    "local.gold_sales_by_month"
).overwrite(lit(True))


# =========================================================
# 5. GOLD - TOP PRODUCTS
# =========================================================

print("\n" + "=" * 70)
print("BUILDING GOLD TOP PRODUCTS")
print("=" * 70)


gold_products_df = spark.sql("""
SELECT
    StockCode,

    MAX(Description) AS Description,

    SUM(Quantity) AS total_quantity,

    ROUND(
        SUM(Revenue),
        2
    ) AS total_revenue,

    COUNT(DISTINCT Invoice) AS invoice_count

FROM local.online_retail_silver

WHERE
    StockCode IS NOT NULL
    AND Description IS NOT NULL

GROUP BY StockCode

ORDER BY total_revenue DESC
""")


print("\nTop products:")

gold_products_df.show(
    20,
    truncate=False
)


# =========================================================
# WRITE GOLD PRODUCTS
# =========================================================

gold_products_df.writeTo(
    "local.gold_top_products"
).overwrite(lit(True))


# =========================================================
# 6. EXPORT GOLD TABLES FOR STREAMLIT
# =========================================================

export_dir = Path(
    os.getenv("ICEBERG_DASHBOARD_EXPORT_DIR", "/opt/spark/exports")
)
export_dir.mkdir(parents=True, exist_ok=True)

for table_name, dataframe in (
    ("gold_sales_by_country", gold_country_df),
    ("gold_sales_by_month", gold_month_df),
    ("gold_top_products", gold_products_df),
):
    table_export_dir = export_dir / table_name
    table_export_dir.mkdir(parents=True, exist_ok=True)
    export_path = table_export_dir / "part-00000.csv"

    with export_path.open("w", newline="", encoding="utf-8") as export_file:
        writer = csv.writer(export_file)
        writer.writerow(dataframe.columns)
        for row in dataframe.toLocalIterator():
            writer.writerow(row)

(export_dir / "_dashboard_exported_at.txt").write_text(
    datetime.now(timezone.utc).isoformat(),
    encoding="utf-8",
)

print(f"\nStreamlit dashboard exports written to {export_dir}")


# =========================================================
# 7. GOLD VERIFICATION
# =========================================================

print("\n" + "=" * 70)
print("GOLD TABLE VERIFICATION")
print("=" * 70)


# ---------------------------------------------------------
# Country
# ---------------------------------------------------------

country_count = spark.sql("""
SELECT COUNT(*)
FROM local.gold_sales_by_country
""").collect()[0][0]


print(
    f"\ngold_sales_by_country rows: {country_count:,}"
)


# ---------------------------------------------------------
# Month
# ---------------------------------------------------------

month_count = spark.sql("""
SELECT COUNT(*)
FROM local.gold_sales_by_month
""").collect()[0][0]


print(
    f"gold_sales_by_month rows: {month_count:,}"
)


# ---------------------------------------------------------
# Products
# ---------------------------------------------------------

product_count = spark.sql("""
SELECT COUNT(*)
FROM local.gold_top_products
""").collect()[0][0]


print(
    f"gold_top_products rows: {product_count:,}"
)


# =========================================================
# 7. DISPLAY GOLD TABLES
# =========================================================

print("\n" + "=" * 70)
print("GOLD - SALES BY COUNTRY")
print("=" * 70)


spark.sql("""
SELECT *
FROM local.gold_sales_by_country
ORDER BY total_revenue DESC
LIMIT 10
""").show(
    truncate=False
)


print("\n" + "=" * 70)
print("GOLD - SALES BY MONTH")
print("=" * 70)


spark.sql("""
SELECT *
FROM local.gold_sales_by_month
ORDER BY year, month
""").show(
    truncate=False
)


print("\n" + "=" * 70)
print("GOLD - TOP PRODUCTS")
print("=" * 70)


spark.sql("""
SELECT *
FROM local.gold_top_products
ORDER BY total_revenue DESC
LIMIT 10
""").show(
    truncate=False
)


# =========================================================
# 8. GOLD SCHEMAS
# =========================================================

print("\n" + "=" * 70)
print("GOLD TABLE SCHEMAS")
print("=" * 70)


print("\n--- gold_sales_by_country ---")

spark.sql("""
DESCRIBE TABLE local.gold_sales_by_country
""").show(
    truncate=False
)


print("\n--- gold_sales_by_month ---")

spark.sql("""
DESCRIBE TABLE local.gold_sales_by_month
""").show(
    truncate=False
)


print("\n--- gold_top_products ---")

spark.sql("""
DESCRIBE TABLE local.gold_top_products
""").show(
    truncate=False
)


# =========================================================
# FINAL
# =========================================================

print("\n" + "=" * 70)
print("SILVER → GOLD COMPLETED SUCCESSFULLY")
print("=" * 70)


spark.stop()