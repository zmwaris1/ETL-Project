import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *

KAFKA_TOPIC_NAME = "sparkTopic2"
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092,localhost:49092"

os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password"

CATALOG_URI = "http://172.19.0.3:19120/api/v1"
WAREHOUSE = "s3a://datalakehouse/"
STORAGE_URI = "http://172.19.0.2:9000"

conf = (
    pyspark.SparkConf()
    .setAppName("sales_data_app")
    .set(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.17.257,software.amazon.awssdk:url-connection-client:2.17.257,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.1",
    )
    .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )
    .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.nessie.uri", CATALOG_URI)
    .set("spark.sql.catalog.nessie.ref", "main")
    .set("spark.sql.catalog.nessie.authentication.type", "NONE")
    .set(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )
    .set("spark.sql.catalog.nessie.s3.endpoint", STORAGE_URI)
    .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
    .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Session Started")

df = spark.read.format("parquet").load(
    "file:///home/zmwaris1/Documents/my_files/DVL-High-Throughput-Internal-Project/datasets/*.parquet"
)

spark.sql("create schema if not exists nessie.db")

df.writeTo("nessie.db.taxi_data").createOrReplace()