import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
import os

os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

## DEFINE SENSITIVE VARIABLES
CATALOG_URI = "http://172.18.0.2:19120/api/v1"  # Nessie Server URI
WAREHOUSE = "s3://warehouse/"  # Minio Address to Write to
STORAGE_URI = "http://172.18.0.3:9000"  # Minio IP address from docker inspect

# Configure Spark with necessary packages and Iceberg/Nessie settings
conf = (
    pyspark.SparkConf()
    .setAppName("sales_data_app")
    # Include necessary packages
    .set(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.17.257,software.amazon.awssdk:url-connection-client:2.17.257,org.apache.iceberg:iceberg-aws-bundle:1.5.2",
    )
    # Enable Iceberg and Nessie extensions
    .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )
    # Configure Nessie catalog
    .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.nessie.uri", CATALOG_URI)
    .set("spark.sql.catalog.nessie.ref", "main")
    .set("spark.sql.catalog.nessie.authentication.type", "NONE")
    .set(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )
    # Set Minio as the S3 endpoint for Iceberg storage
    .set("spark.sql.catalog.nessie.s3.endpoint", STORAGE_URI)
    .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
    .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
)

# Start Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Session Started")

schema = StructType(
    [
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("order_date", StringType(), True),
    ]
)

# Create a DataFrame with messy sales data (including duplicates and errors)
sales_data = [
    (1, 101, "Laptop", 1, 1000.00, "2023-08-01"),
    (2, 102, "Mouse", 2, 25.50, "2023-08-01"),
    (3, 103, "Keyboard", 1, 45.00, "2023-08-01"),
    (1, 101, "Laptop", 1, 1000.00, "2023-08-01"),  # Duplicate
    (4, 104, "Monitor", None, 200.00, "2023-08-02"),  # Missing quantity
    (5, None, "Mouse", 1, 25.50, "2023-08-02"),  # Missing customer_id
]

# Convert the data into a DataFrame
sales_df = spark.createDataFrame(sales_data, schema)

# Create the "sales" namespace
spark.sql("CREATE NAMESPACE if not exists nessie.sales;").show()

# Write the DataFrame to an Iceberg table in the Nessie catalog
sales_df.writeTo("nessie.sales.sales_data_raw").createOrReplace()

# Verify by reading from the Iceberg table
spark.read.table("nessie.sales.sales_data_raw").show()

# Stop the Spark session
spark.stop()
