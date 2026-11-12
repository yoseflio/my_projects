from confluent_kafka import Producder
import json

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producder(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"delviery failed: {err}")
    else:
        print(f"Delivered {msg.value().decode("utf-8")}")

order = {
    "order_id": str(uuid.uuid4()),
    "user": "nana",
    "item": "mushroom pizza",
    "quantity": 2
}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)

producer.flush()