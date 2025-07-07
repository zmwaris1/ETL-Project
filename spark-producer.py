from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

spark = SparkSession.builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5").appName("SparkKafkaProducer").getOrCreate()

# Example DataFrame to send to Kafka
df = spark.readStream.format("parquet").schema(schema).load(
    "file:///home/zmwaris1/Documents/my_files/DVL-High-Throughput-Internal-Project/datasets"
)

df = df.withColumn("value", to_json(struct(col("*"))))
df = df.withColumn(
    "key", concat(col("PULocationID"), col("DOLocationID"), col("VendorID"))
).select("key", "value")

# Write DataFrame to Kafka
query = (
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream.format("kafka")
    .option(
        "kafka.bootstrap.servers", "localhost:29092,localhost:39092,localhost:49092"
    )
    .option("topic", "sparkTopic2")
    .option(
        "checkpointLocation",
        "file:///home/zmwaris1/Documents/my_files/DVL-High-Throughput-Internal-Project/checkpoints/yellow",
    ).trigger(once=True)
    .start()
)

query.awaitTermination()
