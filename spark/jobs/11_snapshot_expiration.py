
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("IcebergSnapshotExpirationDemo")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

table = "local.snapshot_expiration_demo"

try:
    # 1. Creer une table de demonstration independante
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    spark.sql(f"""
        CREATE TABLE {table} (
            id BIGINT,
            product STRING,
            amount DOUBLE
        )
        USING iceberg
    """)

    # 2. Creer plusieurs snapshots par des insertions successives
    spark.sql(f"""
        INSERT INTO {table}
        VALUES (1, 'Product A', 100.0)
    """)

    spark.sql(f"""
        INSERT INTO {table}
        VALUES (2, 'Product B', 200.0)
    """)

    spark.sql(f"""
        INSERT INTO {table}
        VALUES (3, 'Product C', 300.0)
    """)

    print("\n=== SNAPSHOTS AVANT EXPIRATION ===")
    spark.sql(f"""
        SELECT
            snapshot_id,
            committed_at,
            operation
        FROM {table}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)

    # 3. Expirer les snapshots precedant un seuil recent.
    # Les snapshots sont crees pendant ce script : on utilise
    # un seuil futur pour rendre les anciens snapshots eligibles.
    from datetime import datetime, timedelta, timezone

    cutoff = (
        datetime.now(timezone.utc) + timedelta(minutes=1)
    ).strftime("%Y-%m-%d %H:%M:%S")

    print(f"\nExpiration avant le seuil UTC : {cutoff}")

    spark.sql(f"""
        CALL local.system.expire_snapshots(
            table => 'snapshot_expiration_demo',
            older_than => TIMESTAMP '{cutoff}',
            retain_last => 1
        )
    """).show(truncate=False)

    print("\n=== SNAPSHOTS APRES EXPIRATION ===")
    spark.sql(f"""
        SELECT
            snapshot_id,
            committed_at,
            operation
        FROM {table}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)

    print("\n=== DONNEES ACTUELLES ===")
    spark.sql(f"SELECT * FROM {table} ORDER BY id").show()

finally:
    spark.stop()
