
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("DiagnoseIcebergCatalog")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.local.type", "hadoop")
    # Warehouse sans barre oblique finale
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def print_exception(exc):
    """Affiche les informations utiles sur une exception."""
    print("Type Python :", type(exc).__name__)
    print("Message :", repr(exc))

    java_exception = getattr(exc, "java_exception", None)

    if java_exception is not None:
        print("\n=== EXCEPTION JAVA ===")
        print(java_exception.toString())
        java_exception.printStackTrace()


try:
    print("\n=== 1. CONFIGURATION ===")
    print(
        "Warehouse :",
        spark.conf.get("spark.sql.catalog.local.warehouse"),
    )

    print("\n=== 2. CHARGEMENT DU CATALOGUE ICEBERG ===")

    catalog_manager = (
        spark._jsparkSession
        .sessionState()
        .catalogManager()
    )

    print(
        "Gestionnaire :",
        catalog_manager.getClass().getName(),
    )

    iceberg_catalog = catalog_manager.catalog("local")

    print(
        "Catalogue chargé :",
        iceberg_catalog.getClass().getName(),
    )

    print("\n=== 3. CRÉATION DU NAMESPACE DE DIAGNOSTIC ===")

    try:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS local.diagnostic"
        )
        print("Création du namespace : OK")

    except Exception as exc:
        print("Échec de création du namespace.")
        print_exception(exc)
        raise

    print("\n=== 4. TEST DE LISTE DES NAMESPACES ===")

    try:
        spark.sql("SHOW NAMESPACES IN local").show(
            truncate=False
        )

    except Exception as exc:
        print("Échec de la liste des namespaces.")
        print_exception(exc)
        raise

    print("\n=== 5. TEST DES TABLES EXISTANTES ===")

    try:
        spark.sql("SHOW TABLES IN local").show(
            50,
            truncate=False,
        )

    except Exception as exc:
        print("Échec de la liste des tables.")
        print_exception(exc)
        raise

    print("\n=== 6. TEST DE LECTURE DE LA TABLE GOLD ===")

    try:
        spark.sql(
            "SELECT * FROM local.gold_sales_by_country LIMIT 5"
        ).show(truncate=False)

    except Exception as exc:
        print("Échec de lecture de gold_sales_by_country.")
        print_exception(exc)
        raise

    print("\n=== DIAGNOSTIC TERMINÉ AVEC SUCCÈS ===")

finally:
    spark.stop()
