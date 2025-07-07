# import dependencies

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
    LongType,
)
import os
from pyspark.sql.functions import *

# define variables
KAFKA_TOPIC_NAME = "sparkTopic2"
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092,localhost:49092"

os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password"

CATALOG_URI = "http://172.19.0.2:19120/api/v1"
WAREHOUSE = "s3a://user-events-store"
STORAGE_URI = "http://172.19.0.3:9000"

# set spark configurations
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
    .set("spark.sql.adaptive.enabled", "false")
)

# define event schema
schema = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", LongType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True),
    ]
)

# create spark session
def spark_session(conf):
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("Spark Session Started")
    return spark

# create schema/namespaece in nessie catalog
def create_schema(spark, schema_query):
    try:
        spark.sql(schema_query)
        return True, None
    except Exception as err:
        return False, err

# Extract - read events from Kafka
def read_stream(spark):
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )
    return df

# Transform - transform events to columns
def transform_df(df):
    parsed_df = (
        df.selectExpr("CAST(value AS STRING)")
    .select(from_json("value", schema).alias("data"))
    .select("data.*")
    )
    return parsed_df

# Load - load events to iceberg tables
def write_data(df):
    try:
        query = (df.writeStream.format("iceberg")
            .outputMode("append")
            .option(
                "checkpointLocation",
                "file:///home/zmwaris1/Documents/my_files/ETL-Project/checkpoint",
            ).trigger(once=True)
            .toTable("nessie.db.raw_user_events")
        )

        query.awaitTermination()
        return True, None
    except Exception as e:
        return False, e
    

if __name__ == "__main__":
    spark = spark_session(conf=conf)
    schema_query = "create schema if not exists nessie.db"
    bool_value, err = create_schema(spark=spark, schema_query=schema_query)
    if err != None:
        print(f"Error while creating schema \n {err}")
    else:
        df = read_stream(spark=spark)
        transformed_df = transform_df(df)
        val, err = write_data(transformed_df)
        if err != None:
            print(f"Error while writing data \n {err}")
    