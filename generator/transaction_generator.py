from datetime import datetime, timezone
import uuid
import random
import time
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

funds = [
    {"fund_id": 1, "fund_name": "Fund A", "investors": [
        {"investor_id": 1, "investor_name": "investor A"}, {"investor_id": 2, "investor_name": "investor B"}]},
    {"fund_id": 2, "fund_name": "Fund B", "investors": [
        {"investor_id": 3, "investor_name": "investor C"}]},
    {"fund_id": 3, "fund_name": "Fund C", "investors": [
        {"investor_id": 4, "investor_name": "investor D"}]},
    {"fund_id": 4, "fund_name": "Fund D", "investors": [
        {"investor_id": 5, "investor_name": "investor E"}, {"investor_id": 6, "investor_name": "investor F"}]},
]


def generate_transaction() -> dict:
    """
    Generate a random transaction for a fund
    """

    fund = random.choice(funds)
    investor = random.choice(fund['investors'])

    subscription_timestamp = datetime.now(timezone.utc)
    transaction = {
        "request_id": str(uuid.uuid4()),
        "request_source": "investor_portal",
        "subscription_id": f"SUB-{str(uuid.uuid4())}",
        "investor_id": investor["investor_id"],
        "fund_id": fund["fund_id"],
        "subscription_amount": round(random.uniform(0, 1000000), 2),
        "currency": "USD",
        "requested_at": subscription_timestamp.isoformat(),
    }

    return transaction


while True:
    transaction = generate_transaction()
    producer.send("transactions", transaction)
    print(f"Produced: {transaction}")
    time.sleep(10)
