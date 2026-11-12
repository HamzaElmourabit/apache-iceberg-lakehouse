
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestIcebergS3ACatalog")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.s3test",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.s3test.type", "hadoop")
    .config(
        "spark.sql.catalog.s3test.warehouse",
        "s3a://warehouse/iceberg_catalog_test",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


try:
    print("\n=== 1. CRÉATION EXPLICITE DU NAMESPACE ===")

    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS s3test.diagnostic
    """)

    print("\n=== 2. LISTE DES NAMESPACES ===")
    spark.sql("SHOW NAMESPACES IN s3test").show(truncate=False)

    print("\n=== 3. CRÉATION DE LA TABLE ===")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS s3test.diagnostic.sample (
            id INT,
            message STRING
        )
        USING iceberg
    """)

    print("\n=== 4. INSERTION ET LECTURE ===")

    spark.sql("""
        INSERT INTO s3test.diagnostic.sample
        VALUES (1, 'S3A fonctionne')
    """)

    spark.sql("""
        SELECT * FROM s3test.diagnostic.sample
    """).show(truncate=False)

    print("\n=== TEST S3A CATALOGUE RÉUSSI ===")

finally:
    spark.stop()
