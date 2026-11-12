from pyspark.sql import SparkSession


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("IcebergRollback")
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
print("APACHE ICEBERG - ROLLBACK / RESTORATION")
print("=" * 75)


# =========================================================
# 1. CURRENT STATE
# =========================================================

print("\n" + "=" * 75)
print("1. CURRENT STATE BEFORE ROLLBACK")
print("=" * 75)

current_df = spark.sql(f"""
SELECT *
FROM {TABLE}
ORDER BY id
""")

current_df.show(truncate=False)

print(f"Current row count: {current_df.count()}")


# =========================================================
# 2. GET SNAPSHOTS
# =========================================================

print("\n" + "=" * 75)
print("2. AVAILABLE SNAPSHOTS")
print("=" * 75)

snapshots_df = spark.sql(f"""
SELECT
    committed_at,
    snapshot_id,
    parent_id,
    operation,
    summary
FROM {TABLE}.snapshots
ORDER BY committed_at
""")

snapshots_df.show(truncate=False)

snapshots = snapshots_df.collect()

print(f"\nNumber of snapshots: {len(snapshots)}")


if len(snapshots) < 2:
    raise RuntimeError(
        "Not enough snapshots available for rollback."
    )


# =========================================================
# 3. SELECT SNAPSHOT BEFORE DELETE
# =========================================================

target_snapshot = snapshots[-2]

target_snapshot_id = target_snapshot["snapshot_id"]


print("\n" + "=" * 75)
print("3. TARGET SNAPSHOT")
print("=" * 75)

print(f"Snapshot ID : {target_snapshot_id}")
print(f"Committed at: {target_snapshot['committed_at']}")
print(f"Operation   : {target_snapshot['operation']}")


# =========================================================
# 4. SHOW HISTORICAL STATE
# =========================================================

print("\n" + "=" * 75)
print("4. HISTORICAL STATE TO RESTORE")
print("=" * 75)

historical_df = spark.sql(f"""
SELECT *
FROM {TABLE}
VERSION AS OF {target_snapshot_id}
ORDER BY id
""")

historical_df.show(truncate=False)

print(
    f"Historical row count: {historical_df.count()}"
)


# =========================================================
# 5. SHOW PRODUCT C
# =========================================================

print("\n" + "=" * 75)
print("5. PRODUCT C BEFORE ROLLBACK")
print("=" * 75)

print("Current table:")

spark.sql(f"""
SELECT *
FROM {TABLE}
WHERE id = 3
""").show(truncate=False)

print("Historical snapshot:")

spark.sql(f"""
SELECT *
FROM {TABLE}
VERSION AS OF {target_snapshot_id}
WHERE id = 3
""").show(truncate=False)


# =========================================================
# 6. EXECUTE ROLLBACK
# =========================================================

print("\n" + "=" * 75)
print("6. EXECUTING ROLLBACK")
print("=" * 75)

print(
    f"Restoring table to snapshot {target_snapshot_id}..."
)

spark.sql(f"""
CALL local.system.rollback_to_snapshot(
    '{TABLE}',
    {target_snapshot_id}
)
""")

print("\nRollback completed successfully.")


# =========================================================
# 7. VERIFY RESTORED TABLE
# =========================================================

print("\n" + "=" * 75)
print("7. RESTORED TABLE")
print("=" * 75)

restored_df = spark.sql(f"""
SELECT *
FROM {TABLE}
ORDER BY id
""")

restored_df.show(truncate=False)

restored_count = restored_df.count()

print(
    f"Restored row count: {restored_count}"
)


# =========================================================
# 8. VERIFY PRODUCT C
# =========================================================

print("\n" + "=" * 75)
print("8. PRODUCT C AFTER ROLLBACK")
print("=" * 75)

product_c = spark.sql(f"""
SELECT *
FROM {TABLE}
WHERE id = 3
""")

product_c.show(truncate=False)


# =========================================================
# 9. FINAL VERIFICATION
# =========================================================

print("\n" + "=" * 75)
print("9. FINAL VERIFICATION")
print("=" * 75)

if product_c.count() == 1:
    print("SUCCESS: Product C has been restored.")
else:
    print("ERROR: Product C was not restored.")


print("""
Expected restored state:

Product A -> present
Product B -> present
Product C -> RESTORED
Product D -> present

Total rows -> 4
""")


print("=" * 75)
print("ICEBERG ROLLBACK COMPLETED")
print("=" * 75)


spark.stop()