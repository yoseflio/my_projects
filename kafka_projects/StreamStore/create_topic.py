from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

TOPIC = "orders"
PARTITIONS = 3

admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})

topics = admin.list_topics(timeout=10).topics

if TOPIC not in topics:
    # Replication factor 1 because we only run a single broker.
    admin.create_topics([NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)])[TOPIC].result()
    print(f"Created '{TOPIC}' with {PARTITIONS} partitions")
else:
    current = len(topics[TOPIC].partitions)
    if current < PARTITIONS:
        # Kafka can add partitions to a topic but never remove them.
        admin.create_partitions([NewPartitions(TOPIC, PARTITIONS)])[TOPIC].result()
        print(f"Expanded '{TOPIC}' from {current} to {PARTITIONS} partitions")
    else:
        print(f"'{TOPIC}' already has {current} partitions")
