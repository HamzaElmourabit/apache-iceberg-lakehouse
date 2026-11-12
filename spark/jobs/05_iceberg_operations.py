from pyspark.sql import SparkSession


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("IcebergOperationsDemo")
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


print("=" * 75)
print("APACHE ICEBERG - INSERT / UPDATE / DELETE / SNAPSHOTS")
print("=" * 75)


# =========================================================
# 1. VERIFY EXISTING LAKEHOUSE TABLES
# =========================================================

print("\n" + "=" * 75)
print("1. VERIFY EXISTING LAKEHOUSE TABLES")
print("=" * 75)


print("\nSilver table:")

spark.sql("""
SELECT COUNT(*) AS row_count
FROM local.online_retail_silver
""").show()


print("Gold - sales by country:")

spark.sql("""
SELECT COUNT(*) AS row_count
FROM local.gold_sales_by_country
""").show()


print("Gold - sales by month:")

spark.sql("""
SELECT COUNT(*) AS row_count
FROM local.gold_sales_by_month
""").show()


print("Gold - top products:")

spark.sql("""
SELECT COUNT(*) AS row_count
FROM local.gold_top_products
""").show()


# =========================================================
# 2. CREATE ICEBERG DEMO TABLE
# =========================================================

print("\n" + "=" * 75)
print("2. CREATE ICEBERG DEMO TABLE")
print("=" * 75)


spark.sql("""
DROP TABLE IF EXISTS local.iceberg_operations_demo
""")


spark.sql("""
CREATE TABLE local.iceberg_operations_demo (
    id INT,
    product STRING,
    category STRING,
    quantity INT,
    price DOUBLE,
    status STRING
)
USING iceberg
""")


print("\nIceberg demonstration table created.")


# =========================================================
# 3. INITIAL INSERT
# =========================================================

print("\n" + "=" * 75)
print("3. INITIAL INSERT")
print("=" * 75)


spark.sql("""
INSERT INTO local.iceberg_operations_demo
VALUES
    (1, 'Product A', 'Electronics', 10, 25.00, 'ACTIVE'),
    (2, 'Product B', 'Home',        20, 15.00, 'ACTIVE'),
    (3, 'Product C', 'Garden',       5, 40.00, 'ACTIVE')
""")


print("\nInitial data:")

spark.sql("""
SELECT *
FROM local.iceberg_operations_demo
ORDER BY id
""").show(
    truncate=False
)


# =========================================================
# 4. INSERT NEW RECORD
# =========================================================

print("\n" + "=" * 75)
print("4. INSERT NEW RECORD")
print("=" * 75)


spark.sql("""
INSERT INTO local.iceberg_operations_demo
VALUES
    (4, 'Product D', 'Office', 12, 30.00, 'ACTIVE')
""")


print("\nAfter INSERT:")

spark.sql("""
SELECT *
FROM local.iceberg_operations_demo
ORDER BY id
""").show(
    truncate=False
)


# =========================================================
# 5. UPDATE RECORD
# =========================================================

print("\n" + "=" * 75)
print("5. UPDATE RECORD")
print("=" * 75)


spark.sql("""
UPDATE local.iceberg_operations_demo
SET
    quantity = 50,
    price = 18.00
WHERE id = 2
""")


print("\nAfter UPDATE:")

spark.sql("""
SELECT *
FROM local.iceberg_operations_demo
ORDER BY id
""").show(
    truncate=False
)


# =========================================================
# 6. DELETE RECORD
# =========================================================

print("\n" + "=" * 75)
print("6. DELETE RECORD")
print("=" * 75)


spark.sql("""
DELETE FROM local.iceberg_operations_demo
WHERE id = 3
""")


print("\nAfter DELETE:")

spark.sql("""
SELECT *
FROM local.iceberg_operations_demo
ORDER BY id
""").show(
    truncate=False
)


# =========================================================
# 7. CURRENT TABLE STATE
# =========================================================

print("\n" + "=" * 75)
print("7. CURRENT TABLE STATE")
print("=" * 75)


current_count = spark.sql("""
SELECT COUNT(*)
FROM local.iceberg_operations_demo
""").collect()[0][0]


print(f"\nCurrent number of rows: {current_count}")


spark.sql("""
SELECT *
FROM local.iceberg_operations_demo
ORDER BY id
""").show(
    truncate=False
)


# =========================================================
# 8. ICEBERG HISTORY
# =========================================================

print("\n" + "=" * 75)
print("8. ICEBERG HISTORY")
print("=" * 75)


print("""
Iceberg history:
- made_current_at
- snapshot_id
- parent_id
- is_current_ancestor
""")


spark.sql("""
SELECT
    made_current_at,
    snapshot_id,
    parent_id,
    is_current_ancestor
FROM local.iceberg_operations_demo.history
ORDER BY made_current_at
""").show(
    truncate=False
)


# =========================================================
# 9. ICEBERG SNAPSHOTS
# =========================================================

print("\n" + "=" * 75)
print("9. ICEBERG SNAPSHOTS")
print("=" * 75)


print("\nAvailable snapshots:")


spark.sql("""
SELECT
    committed_at,
    snapshot_id,
    parent_id,
    operation,
    summary
FROM local.iceberg_operations_demo.snapshots
ORDER BY committed_at
""").show(
    truncate=False
)


# =========================================================
# 10. SNAPSHOT IDs
# =========================================================

print("\n" + "=" * 75)
print("10. SNAPSHOT IDENTIFIERS")
print("=" * 75)


snapshots_df = spark.sql("""
SELECT
    committed_at,
    snapshot_id,
    parent_id,
    operation
FROM local.iceberg_operations_demo.snapshots
ORDER BY committed_at
""")


snapshots_df.show(
    truncate=False
)


# =========================================================
# 11. FINAL TABLE
# =========================================================

print("\n" + "=" * 75)
print("11. FINAL TABLE CONTENT")
print("=" * 75)


spark.sql("""
SELECT *
FROM local.iceberg_operations_demo
ORDER BY id
""").show(
    truncate=False
)


# =========================================================
# 12. TABLE METADATA
# =========================================================

print("\n" + "=" * 75)
print("12. ICEBERG TABLE METADATA")
print("=" * 75)


spark.sql("""
DESCRIBE TABLE local.iceberg_operations_demo
""").show(
    truncate=False
)


# =========================================================
# FINAL
# =========================================================

print("\n" + "=" * 75)
print("ICEBERG OPERATIONS DEMONSTRATION COMPLETED SUCCESSFULLY")
print("=" * 75)


spark.stop()