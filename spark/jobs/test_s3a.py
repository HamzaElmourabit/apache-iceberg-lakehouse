from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestS3A")
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

try:
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path("s3a://warehouse/")
    fs = path.getFileSystem(conf)

    print("Filesystem URI:", fs.getUri())
    print("Warehouse path:", path.toString())
    print("Warehouse exists:", fs.exists(path))

    print("Listing warehouse:")
    for item in fs.listStatus(path):
        print(item.getPath().toString())

except Exception as exc:
    print("S3A TEST FAILED:", repr(exc))
    raise

finally:
    spark.stop()
