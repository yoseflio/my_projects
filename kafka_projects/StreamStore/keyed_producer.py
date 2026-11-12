from confluent_kafka import Producer
import json
import random
import time
import uuid

producer_config = {
    "bootstrap.servers": "127.0.0.1:9092"
}

producer = Producer(producer_config)

USERS = ["lior", "adi", "nana", "dan", "maya"]
ITEMS = ["mushroom pizza", "pasta", "shrimps", "salad", "burger"]

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"key={msg.key().decode('utf-8'):<5} -> partition {msg.partition()} offset {msg.offset()}")

for _ in range(30):
    order = {
        "order_id": str(uuid.uuid4()),
        "user": random.choice(USERS),
        "item": random.choice(ITEMS),
        "quantity": random.randint(1, 5)
    }

    # The key decides the partition: the same user always lands on the same partition,
    # so each user's orders stay in order.
    producer.produce(
        topic="orders",
        key=order["user"],
        value=json.dumps(order).encode("utf-8"),
        callback=delivery_report
    )
    producer.poll(0)
    time.sleep(0.3)

remaining = producer.flush(10)
if remaining:
    print(f"{remaining} message(s) were not delivered")
