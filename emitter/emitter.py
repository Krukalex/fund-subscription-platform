from datetime import datetime, timedelta
import uuid
import random
import time
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import json

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

future = producer.send("smoke", {"ok": True})
metadata = future.get(timeout=10)
producer.flush()
print(metadata.topic, metadata.partition, metadata.offset)


consumer = KafkaConsumer(
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    group_id="smoke_check",
    consumer_timeout_ms=10000,
    value_deserializer=lambda v: v.decode("utf-8")
)

partition = TopicPartition("smoke", 0)
consumer.assign([partition])
consumer.seek(partition, 0)

for message in consumer:
    print(message.offset, message.value)

consumer.close()
