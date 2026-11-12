from confluent_kafka import Consumer
import json

consumer_config = {
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("Consumer is running")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error", msg.error())
            continue

        value = msg.value().decode("utf-8")
        try:
            order = json.loads(value)
            print(f"package received: {order['quantity']} x {order['item']} from {order['user']}")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Skipping bad message at offset {msg.offset()}: {e}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
