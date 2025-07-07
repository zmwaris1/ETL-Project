from confluent_kafka.admin import AdminClient, NewTopic

a = AdminClient(
    {"bootstrap.servers": "localhost:29092,localhost:39092,localhost:49092"}
)

new_topics = [
    NewTopic(topic, num_partitions=3, replication_factor=3) for topic in ["sparkTopic2"]
]
# Note: In a multi-cluster production scenario, it is more typical to use a replication_factor of 3 for durability.

fs = a.create_topics(new_topics)

# Wait for each operation to finish.
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))
