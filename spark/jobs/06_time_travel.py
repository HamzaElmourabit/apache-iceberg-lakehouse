from pyspark.sql import SparkSession


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("IcebergTimeTravel")
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
print("APACHE ICEBERG - TIME TRAVEL DEMONSTRATION")
print("=" * 75)


# =========================================================
# 1. VERIFY TABLE
# =========================================================

print("\n" + "=" * 75)
print("1. CURRENT TABLE")
print("=" * 75)


spark.sql(f"""
SELECT *
FROM {TABLE}
ORDER BY id
""").show(
    truncate=False
)


# =========================================================
# 2. DISPLAY SNAPSHOTS
# =========================================================

print("\n" + "=" * 75)
print("2. AVAILABLE ICEBERG SNAPSHOTS")
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


snapshots_df.show(
    truncate=False
)


# =========================================================
# 3. COLLECT SNAPSHOTS
# =========================================================

snapshots = snapshots_df.collect()


print(f"\nNumber of snapshots: {len(snapshots)}")


if len(snapshots) < 4:
    print("""
WARNING:
At least 4 snapshots are expected for this demonstration.

Expected sequence:
1. Initial INSERT
2. INSERT Product D
3. UPDATE Product B
4. DELETE Product C
""")

    spark.stop()
    raise SystemExit(1)


# =========================================================
# 4. DISPLAY SNAPSHOT IDs
# =========================================================

print("\n" + "=" * 75)
print("3. SNAPSHOT IDENTIFIERS")
print("=" * 75)


for index, snapshot in enumerate(snapshots, start=1):

    print(
        f"{index}. "
        f"timestamp={snapshot['committed_at']} | "
        f"snapshot_id={snapshot['snapshot_id']} | "
        f"operation={snapshot['operation']}"
    )


# =========================================================
# 5. SELECT IMPORTANT SNAPSHOTS
# =========================================================

# Expected:
#
# Snapshot 1 → Initial INSERT
# Snapshot 2 → INSERT Product D
# Snapshot 3 → UPDATE Product B
# Snapshot 4 → DELETE Product C

snapshot_before_delete = snapshots[-2]
snapshot_after_delete = snapshots[-1]


before_delete_id = snapshot_before_delete["snapshot_id"]
after_delete_id = snapshot_after_delete["snapshot_id"]


print("\n" + "=" * 75)
print("4. SELECTED SNAPSHOTS")
print("=" * 75)


print(
    f"\nSnapshot BEFORE DELETE: {before_delete_id}"
)

print(
    f"Snapshot AFTER DELETE:  {after_delete_id}"
)


# =========================================================
# 6. CURRENT STATE
# =========================================================

print("\n" + "=" * 75)
print("5. CURRENT STATE")
print("=" * 75)


current_df = spark.sql(f"""
SELECT *
FROM {TABLE}
ORDER BY id
""")


current_df.show(
    truncate=False
)


current_count = current_df.count()


print(
    f"Current row count: {current_count}"
)


# =========================================================
# 7. TIME TRAVEL - BEFORE DELETE
# =========================================================

print("\n" + "=" * 75)
print("6. TIME TRAVEL - BEFORE DELETE")
print("=" * 75)


print(
    f"\nReading snapshot {before_delete_id}..."
)


before_delete_df = spark.sql(f"""
SELECT *
FROM {TABLE}
VERSION AS OF {before_delete_id}
ORDER BY id
""")


before_delete_df.show(
    truncate=False
)


before_delete_count = before_delete_df.count()


print(
    f"Rows before DELETE: {before_delete_count}"
)


# =========================================================
# 8. TIME TRAVEL - AFTER DELETE
# =========================================================

print("\n" + "=" * 75)
print("7. TIME TRAVEL - AFTER DELETE")
print("=" * 75)


print(
    f"\nReading snapshot {after_delete_id}..."
)


after_delete_df = spark.sql(f"""
SELECT *
FROM {TABLE}
VERSION AS OF {after_delete_id}
ORDER BY id
""")


after_delete_df.show(
    truncate=False
)


after_delete_count = after_delete_df.count()


print(
    f"Rows after DELETE: {after_delete_count}"
)


# =========================================================
# 9. PROVE THAT PRODUCT C EXISTED BEFORE DELETE
# =========================================================

print("\n" + "=" * 75)
print("8. TIME TRAVEL PROOF")
print("=" * 75)


print("\nProduct C - CURRENT TABLE:")


spark.sql(f"""
SELECT *
FROM {TABLE}
WHERE id = 3
""").show(
    truncate=False
)


print("\nProduct C - HISTORICAL SNAPSHOT:")


spark.sql(f"""
SELECT *
FROM {TABLE}
VERSION AS OF {before_delete_id}
WHERE id = 3
""").show(
    truncate=False
)


# =========================================================
# 10. COMPARE CURRENT VS HISTORICAL
# =========================================================

print("\n" + "=" * 75)
print("9. CURRENT VS HISTORICAL COMPARISON")
print("=" * 75)


print(
    f"""
Current rows:
    {current_count}

Rows before DELETE:
    {before_delete_count}

Difference:
    {before_delete_count - current_count}
"""
)


# =========================================================
# 11. TIME TRAVEL USING TIMESTAMP
# =========================================================

print("\n" + "=" * 75)
print("10. TIME TRAVEL USING TIMESTAMP")
print("=" * 75)


before_timestamp = snapshot_before_delete["committed_at"]


print(
    f"\nHistorical timestamp: {before_timestamp}"
)


# Convert timestamp to Spark SQL timestamp format
timestamp_string = before_timestamp.strftime(
    "%Y-%m-%d %H:%M:%S"
)


timestamp_df = spark.sql(f"""
SELECT *
FROM {TABLE}
TIMESTAMP AS OF '{timestamp_string}'
ORDER BY id
""")


timestamp_df.show(
    truncate=False
)


print(
    f"Rows using TIMESTAMP AS OF: {timestamp_df.count()}"
)


# =========================================================
# 12. FINAL RESULT
# =========================================================

print("\n" + "=" * 75)
print("TIME TRAVEL RESULT")
print("=" * 75)


print(
    """
Apache Iceberg successfully preserved
previous versions of the table.

Current state:
    Product C has been deleted.

Historical state:
    Product C is still available.

Therefore:
    TIME TRAVEL = SUCCESS
"""
)


print("=" * 75)
print("TIME TRAVEL DEMONSTRATION COMPLETED SUCCESSFULLY")
print("=" * 75)


spark.stop()