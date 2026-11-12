
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestIcebergPath")
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
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    jvm = spark._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    print("\n=== 1. TEST DU CHEMIN ===")

    path_string = "s3a://warehouse/"
    path = jvm.org.apache.hadoop.fs.Path(path_string)

    print("Chemin :", path.toString())
    print("URI :", path.toUri().toString())
    print("Chemin absolu :", path.isAbsolute())
    print("Chemin URI :", path.toUri().getPath())

    print("\n=== 2. TEST DU FILESYSTEM S3A ===")

    try:
        fs = path.getFileSystem(hadoop_conf)

        print("Filesystem :", fs.getClass().getName())
        print("URI du filesystem :", fs.getUri().toString())
        print("Répertoire de travail :", fs.getWorkingDirectory().toString())
        print("Existe :", fs.exists(path))

    except Exception as exc:
        print("Erreur filesystem :", type(exc).__name__, repr(exc))
        raise

    print("\n=== 3. TEST DU CHEMIN DE LA TABLE EXISTANTE ===")

    table_path = jvm.org.apache.hadoop.fs.Path(
        "s3a://warehouse/online_retail_silver"
    )

    print("Chemin table :", table_path.toString())
    print("Chemin absolu :", table_path.isAbsolute())

    try:
        fs = table_path.getFileSystem(hadoop_conf)
        print("Table accessible :", fs.exists(table_path))
        metadata_path = jvm.org.apache.hadoop.fs.Path(
            "s3a://warehouse/online_retail_silver/metadata/v2.metadata.json"
        )
        print("Métadonnées v2 accessibles :", fs.exists(metadata_path))

    except Exception as exc:
        print("Erreur table :", type(exc).__name__, repr(exc))
        raise

    print("\n=== TEST TERMINÉ ===")

finally:
    spark.stop()