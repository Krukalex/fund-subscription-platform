from kafka import KafkaConsumer
import json
from aggregate import aggregate


investor = {
    "investor_id": 4,
    "status": "active",
    "commitment_amount": 1_000_000,
    "subscribed_amount": 900_000,
    "currency": "USD",
}

subscription = {
    "investor_id": 4,
    "fund_id": 3,
    "subscription_amount": 200_000,
    "currency": "USD",
}

decision = aggregate(investor, subscription)

consumer = KafkaConsumer(
    "subscription-requests",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    group_id="investor-service-group",
    key_deserializer=lambda k: k.decode("utf-8"),
    value_deserializer=lambda v: v.decode("utf-8"),
)

# decision = aggregate(investor, subscription)

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
                subscription = json.loads(message.value)
                print(message.partition, message.key,
                      subscription["investor_id"], flush=True)
                print(subscription["subscription_amount"])
                decision = aggregate(investor, subscription)
                print(decision)
finally:
    consumer.close()
