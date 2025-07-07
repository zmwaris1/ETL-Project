### Introduction 

__Data Warehouse__

Data Warehouse have been state of the art for analytic workloads since the 1980's with the emergence of Ralph Kimball's dimensional modeling. These were built to aggreagte structured data from different sources and run analytic queries on them to generate insights. 

Up until the emergence of cloud dwh's like Aws Redshift most data warehouses had stoarge and compute tightly coupled together. This made scaling inflexible since you could not independently scale storage from compute and vice versa. 

In general, Data Warehouse however is quite expensive and do not support unstructured data. Data Warehouses are also a single locked system, which means your data can just be accessed by the compute engine of the warehouse. 

__Data Lake__

They offer cheap storage and flexibility with data format and file type. In addition to that, they can be accessed by a wide variety of computational engines.

Individual datasets within data lakes are often organized as collections of files within directory structures, often with multiple files in one directory representing a single table. The benefits of this approach are that data is highly accessible and flexible. 
However, several concepts provided by traditional databases and data warehouses are not addressed solely by directories of files and require additional tooling. This includes:

- What is the schema of a dataset, including columns and data types
- Which files comprise the dataset and how are they organized (e.g., partitions)
- How different applications coordinate changes to the dataset, including both changes to the definition of the dataset and changes to data
- Making transactions ACID compliant 
- Enabling, Inserts, merges and timetravel etc. 

A solution are __Data Lakehouse__

Data Lakehouse still uses the cheap and flexible storage of Data Lake, but they add a layer of structure and governance to the data. They use open table formats like Apache Iceberg to provide schema enforcement, organization of files, and other traditional database-like features.

This enables Data Lakehouses to use traditional SQL-based tools and techniques to query and analyze the data, which makes it easier for both business analysts and data scientists to work with the data and find insights.

By combining the best aspects of Data Lakes and traditional Data Warehouses, Data Lakehouses provide a way to store and manage data that is both flexible and structured. This makes it easier for teams to collaborate, find insights, and make informed decisions.

In this repository I have built a sample Lakehouse architecture with a working Streaming Spark pipelinee. It can be used for local testing purposes or as a backend to your own data analysis projects, as a more real-life like solution compared to files on your own disk.

### Architecture of this lakehouse:

![image](arch/Open_Data_Lakehouse.png)

**Data Source:**

Since this pipeline is a streaming pipeline I have used **Kafka** as the data source. Apache Kafka is a distributed event streaming platform used for building real-time data pipelines and streaming applications. It allows high-throughput, fault-tolerant, and scalable publishing and subscribing of messages. Kafka stores streams of records in categories called topics, enabling decoupled data communication between producers and consumers. It’s widely used for log aggregation, stream processing, and real-time analytics in modern data architectures.

**Data Processing:**

For Data Processing I have used **Apache Spark** because of it's distributed processing and multiple integrations. Apache Spark is an open-source distributed computing framework designed for fast, large-scale data processing. It supports in-memory computation, which makes it significantly faster than traditional big data tools like Hadoop MapReduce. It's widely used for ETL, real-time analytics, and complex data workflows.

**Storage:**

As for the storage layer, I have chosen **Minio**, which is an open source, S3 compliant Object storage. Such an storage has less operational cost than a traditional data warehouses and enables us to store all types of data and formats. Moreover since it is decoupled from our query engine we can scale it independently, if we need additional storage but not more processing power. 

**Table formats**

Table formats are a way to organize data files. They try to bring database-like features to the Data lake. This is one of the key differences between a Data Lake and Lakehouse. 

I have chosen Apache Iceberg, which in particular has the following advantages over Hudi and Delta tables:  

Iceberg is faster regarding insert and update operations as well as requiring less storage. 
Furthermore it lets you change partitions easily while data evolves, as well as making schema evolution possible with a multitude of query engines. 

**Schema Metastore**

The schemas, partitions and other metadata of the created tables needs to be stored in a central repository. This is where **Nessie** Catalog comes in place for. It consists of an internal server which is used to connect with other services when querying data. (The catalog data is not persisted since I decided to use rocksDB(default) which is an in-memory database. To persist data you can choose any database like MySQL, PostgreSQL etc.) 

**Query engine**

**Trino** is a Query engine which lets you create and query tables based on flat files via. Iceberg. Moreover it support query federation, meaning you can join data from multiple different sources. Like all other services, Trino has been deployed in a Docker container.  

Trino itself works like a massively parallel processing databases query engine, meaning it scales verticaly instead of increasing processing power of one node. Moreover it is a connector based architecture. As explained before Trino itself is only a query engine, thus relying on connectors to connect to all types of data sources. A database on the other hand has both the query engine and storage coupled together. The connector will translate the Sql statement to a set of API calls which will return so called pages: A collection of rows in columnar format. The users Sql query will be translated into a query or execution plan on the data. Such a plan consists of multiple stages that process these pages and which will be assigned to the worker nodes by the coordinator. 

There is a really good explanation on it in the O'Reilly Trino book, which can be found [here](https://www.oreilly.com/library/view/trino-the-definitive/9781098107703/ch04.html#fig-task-split)

In this project the computational service (Trino) is decoupled from the storage and thus would allow for easy scalability independent of the data storage.

### How to setup the project

To clone the repo run:

```bash
git clone https://github.com/zmwaris1/ETL-Project.git
```

To setup Kafka cluster(source) run:

```bash
docker compose -f kafka_clsuter.yaml up -d
```

To setup minio and nessie run:

```bash
docker compose -f docker-compose.yml up -d
```
To setup python virtual environment and install libraries:

```bash
python -m venv .venv
```
```bash
source ./.venv/bin/activate
```
Install dependent libraries:

```bash
pip install -r requirements.txt
```

To setup required Kafka topic, run:
```bash
python kafka_topic.py
```

To produce Kafka messages:
```bash
python spark-producer.py
```

To consume and run ETL process:
```bash
python spark_kafka_stream.py
```
To start trino server:

```bash
docker compose -f trino-compose.yml up -d
```
You will need to add connection properties in ```catalog/iceberg.properties``` file to setup the iceberg tables in Trino.

**Note:**
Spark and Docker setup can be done using the given links:
Spark ---> [Spark Setup(Ubuntu)](https://phoenixnap.com/kb/install-spark-on-ubuntu)

Docker ---> [Docker Setup](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)

Thank you for your time. Please raise an issue if you have something to contribute.