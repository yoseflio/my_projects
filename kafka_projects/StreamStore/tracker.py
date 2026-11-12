from confluent_kafka import Consumer
import json
import os

consumer_config = {
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

# PID tells apart several trackers running side by side.
name = f"tracker-{os.getpid()}"


def on_assign(consumer, partitions):
    print(f"[{name}] assigned partitions: {[p.partition for p in partitions]}")


def on_revoke(consumer, partitions):
    print(f"[{name}] revoked partitions: {[p.partition for p in partitions]}")


consumer.subscribe(["orders"], on_assign=on_assign, on_revoke=on_revoke)

print(f"[{name}] Consumer is running")

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
            print(f"[{name}] partition {msg.partition()}: {order['quantity']} x {order['item']} from {order['user']}")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Skipping bad message at offset {msg.offset()}: {e}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
