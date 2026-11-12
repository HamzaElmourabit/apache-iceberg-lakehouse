from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --------------------------------------------------
# 1. Spark + Apache Iceberg configuration
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("IcebergPartitioningDemo")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.local.type", "hadoop")
    .config(
        "spark.sql.catalog.local.warehouse",
        "s3a://warehouse/",
    )
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

try:
    # --------------------------------------------------
    # 2. Source and demo table
    # --------------------------------------------------
    source_table = "local.online_retail_silver"
    demo_table = "local.partition_demo"

    # --------------------------------------------------
    # 3. Read a limited sample from the Silver table
    # --------------------------------------------------
    sample_df = (
        spark.table(source_table)
        .select("InvoiceDate", "Country", "Revenue")
        .filter(
            col("InvoiceDate").isNotNull()
            & col("Country").isNotNull()
        )
        .limit(10000)
    )

    sample_count = sample_df.count()

    if sample_count == 0:
        raise RuntimeError(
            f"No data found in source table: {source_table}"
        )

    print(f"Sample rows selected: {sample_count}")

    # --------------------------------------------------
    # 4. Recreate only the dedicated demo table
    # --------------------------------------------------
    spark.sql(f"DROP TABLE IF EXISTS {demo_table}")

    spark.sql(
        f"""
        CREATE TABLE {demo_table} (
            InvoiceDate TIMESTAMP,
            Country STRING,
            Revenue DOUBLE
        )
        USING iceberg
        PARTITIONED BY (months(InvoiceDate))
        """
    )

    # --------------------------------------------------
    # 5. Write the sample into the partitioned table
    # --------------------------------------------------
    sample_df.writeTo(demo_table).append()

    print("\nPartitioned table created successfully.")

    # --------------------------------------------------
    # 6. Verify the data
    # --------------------------------------------------
    print("\nSample rows:")
    spark.sql(
        f"SELECT * FROM {demo_table} LIMIT 10"
    ).show(truncate=False)

    print("Rows in demo table:")
    spark.sql(
        f"SELECT COUNT(*) AS total_rows FROM {demo_table}"
    ).show()

    print("Monthly revenue:")
    spark.sql(
        f"""
        SELECT
            DATE_FORMAT(InvoiceDate, 'yyyy-MM') AS month,
            COUNT(*) AS transaction_count,
            ROUND(SUM(Revenue), 2) AS total_revenue
        FROM {demo_table}
        GROUP BY DATE_FORMAT(InvoiceDate, 'yyyy-MM')
        ORDER BY month
        """
    ).show(50, truncate=False)

    # --------------------------------------------------
    # 7. Inspect Iceberg partition metadata
    # --------------------------------------------------
    print("Iceberg partition metadata:")
    spark.sql(
        f"SELECT * FROM {demo_table}.partitions"
    ).show(50, truncate=False)

finally:
    spark.stop()