from pyspark.sql import SparkSession


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("IcebergSchemaEvolution")
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


TABLE = "local.iceberg_operations_demo"


print("=" * 75)
print("APACHE ICEBERG - SCHEMA EVOLUTION")
print("=" * 75)


# =========================================================
# 1. CURRENT SCHEMA
# =========================================================

print("\n" + "=" * 75)
print("1. CURRENT SCHEMA")
print("=" * 75)

spark.sql(f"""
DESCRIBE TABLE {TABLE}
""").show(truncate=False)


# =========================================================
# 2. CURRENT DATA
# =========================================================

print("\n" + "=" * 75)
print("2. CURRENT DATA")
print("=" * 75)

spark.sql(f"""
SELECT *
FROM {TABLE}
ORDER BY id
""").show(truncate=False)


# =========================================================
# 3. ADD NEW COLUMN
# =========================================================

print("\n" + "=" * 75)
print("3. ADDING NEW COLUMN: discount")
print("=" * 75)

spark.sql(f"""
ALTER TABLE {TABLE}
ADD COLUMN discount DOUBLE
""")

print("Column 'discount' added successfully.")


# =========================================================
# 4. NEW SCHEMA
# =========================================================

print("\n" + "=" * 75)
print("4. SCHEMA AFTER EVOLUTION")
print("=" * 75)

spark.sql(f"""
DESCRIBE TABLE {TABLE}
""").show(truncate=False)


# =========================================================
# 5. OLD DATA AFTER SCHEMA EVOLUTION
# =========================================================

print("\n" + "=" * 75)
print("5. OLD DATA AFTER SCHEMA EVOLUTION")
print("=" * 75)

spark.sql(f"""
SELECT *
FROM {TABLE}
ORDER BY id
""").show(truncate=False)


# =========================================================
# 6. INSERT NEW DATA WITH DISCOUNT
# =========================================================

print("\n" + "=" * 75)
print("6. INSERT NEW DATA")
print("=" * 75)

spark.sql(f"""
INSERT INTO {TABLE}
VALUES
(
    5,
    'Product E',
    'Electronics',
    15,
    100.0,
    'ACTIVE',
    10.0
)
""")

print("Product E inserted successfully.")


# =========================================================
# 7. VERIFY NEW DATA
# =========================================================

print("\n" + "=" * 75)
print("7. DATA AFTER INSERT")
print("=" * 75)

spark.sql(f"""
SELECT *
FROM {TABLE}
ORDER BY id
""").show(truncate=False)


# =========================================================
# 8. UPDATE EXISTING PRODUCT
# =========================================================

print("\n" + "=" * 75)
print("8. UPDATE PRODUCT A WITH DISCOUNT")
print("=" * 75)

spark.sql(f"""
UPDATE {TABLE}
SET discount = 5.0
WHERE id = 1
""")

spark.sql(f"""
SELECT *
FROM {TABLE}
WHERE id = 1
""").show(truncate=False)


# =========================================================
# 9. SHOW SNAPSHOTS
# =========================================================

print("\n" + "=" * 75)
print("9. SNAPSHOTS AFTER SCHEMA EVOLUTION")
print("=" * 75)

spark.sql(f"""
SELECT
    committed_at,
    snapshot_id,
    parent_id,
    operation,
    summary
FROM {TABLE}.snapshots
ORDER BY committed_at
""").show(truncate=False)


# =========================================================
# 10. FINAL RESULT
# =========================================================

print("\n" + "=" * 75)
print("10. FINAL RESULT")
print("=" * 75)

print("""
Schema Evolution successfully demonstrated.

The table originally contained:

id
product
category
quantity
price
status

A new column was added:

discount

Existing records were preserved.
New records can use the new column.
""")

print("=" * 75)
print("SCHEMA EVOLUTION COMPLETED SUCCESSFULLY")
print("=" * 75)


spark.stop()