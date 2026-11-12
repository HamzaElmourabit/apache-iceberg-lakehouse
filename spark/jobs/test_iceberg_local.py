
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

temp_dir = tempfile.mkdtemp(prefix="iceberg_diagnostic_")
warehouse = Path(temp_dir).resolve().as_uri()

print("Warehouse temporaire :", warehouse)

spark = (
    SparkSession.builder
    .appName("TestIcebergLocal")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.test",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.test.type", "hadoop")
    .config("spark.sql.catalog.test.warehouse", warehouse)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    print("\n=== 1. TEST DU CATALOGUE LOCAL ===")
    spark.sql("SHOW NAMESPACES IN test").show(truncate=False)

    print("\n=== 2. CRÉATION D'UN NAMESPACE TEMPORAIRE ===")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS test.diagnostic")
    spark.sql("SHOW NAMESPACES IN test").show(truncate=False)

    print("\n=== 3. CRÉATION D'UNE TABLE TEMPORAIRE ===")
    spark.sql("""
        CREATE TABLE test.diagnostic.sample (
            id INT,
            message STRING
        )
        USING iceberg
    """)

    spark.sql("""
        INSERT INTO test.diagnostic.sample
        VALUES (1, 'test')
    """)

    spark.sql(
        "SELECT * FROM test.diagnostic.sample"
    ).show(truncate=False)

    print("\n=== TEST LOCAL RÉUSSI ===")

finally:
    spark.stop()

