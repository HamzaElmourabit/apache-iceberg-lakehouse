
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("IcebergCompactionDemo")
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

table = "local.compaction_demo"

try:
    # Creer une table de demonstration independante
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    spark.sql(f"""
        CREATE TABLE {table} (
            id BIGINT,
            product STRING,
            amount DOUBLE
        )
        USING iceberg
    """)

    # Effectuer plusieurs petites insertions
    for i in range(1, 7):
        spark.sql(f"""
            INSERT INTO {table}
            VALUES ({i}, 'Product_{i}', {i * 10.0})
        """)

    print("\n=== AVANT COMPACTION ===")

    spark.sql(f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT product) AS distinct_products
        FROM {table}
    """).show()

    spark.sql(f"""
        SELECT
            COUNT(*) AS data_files,
            ROUND(SUM(file_size_in_bytes) / 1024.0, 2)
                AS total_size_kb
        FROM {table}.files
    """).show()

    # Compacter les fichiers de donnees
    spark.sql("""
        CALL local.system.rewrite_data_files(
            table => 'compaction_demo',
            options => map(
                'target-file-size-bytes', '134217728'
            )
        )
    """).show(truncate=False)

    print("\n=== APRES COMPACTION ===")

    spark.sql(f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT product) AS distinct_products
        FROM {table}
    """).show()

    spark.sql(f"""
        SELECT
            COUNT(*) AS data_files,
            ROUND(SUM(file_size_in_bytes) / 1024.0, 2)
                AS total_size_kb
        FROM {table}.files
    """).show()

    print("\n=== DONNEES FINALES ===")
    spark.sql(f"SELECT * FROM {table} ORDER BY id").show()

finally:
    spark.stop()
