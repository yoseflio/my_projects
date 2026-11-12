from confluent_kafka import Producer
import json
import uuid

producer_config = {
    "bootstrap.servers": "127.0.0.1:9092"
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered {msg.value().decode('utf-8')}")
        print(f"offset {msg.offset()}")

order = {
    "order_id": str(uuid.uuid4()),
    "user": "adi",
    "item": "shripms",
    "quantity": 8
}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)

remaining = producer.flush(10)
if remaining:
    print(f"{remaining} message(s) were not delivered")
