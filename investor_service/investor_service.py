from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "subscription-requests",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    group_id="investor-service-group",
    key_deserializer=lambda k: k.decode("utf-8"),
    value_deserializer=lambda v: v.decode("utf-8"),
)

announced = False

try:
    while True:
        records = consumer.poll(timeout_ms=1000)
        if consumer.assignment() and not announced:
            print(
                "assigned",
                sorted(partition.partition for partition in consumer.assignment()),
                flush=True,
            )
            announced = True
        for messages in records.values():
            for message in messages:
                event = json.loads(message.value)
                print(message.partition, message.key,
                      event["investor_id"], flush=True)
                print(event["subscription_amount"])
finally:
    consumer.close()
