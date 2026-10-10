# Fund Subscription Platform

A distributed fund subscription pipeline built to learn how message state, the inbox/outbox pattern, and state-transition transactions keep a system idempotent under replay and safe from lost events.

A subscription is a contribution to a fund for a particular investor. The original business event is checked against business rules, then committed to stored state. Those rules run inside a saga: an investor service owns the investor aggregate, and a fund service owns the fund aggregate.

Kafka is the transport. Python processors will run the saga and the projection. Postgres is the planned state store.

## Status

Kafka is running locally and a Python client can produce and consume on it. The saga, projection service, and Postgres store are not built yet.

| Piece | State |
| --- | --- |
| Kafka (single broker, Zookeeper) | Running in Docker |
| Python producer and consumer | Connectivity checked against the `smoke` topic |
| Subscription saga | Designed, not implemented |
| Projection service | Designed, not implemented |
| Postgres state store | Planned |

## Flow

```text
Transaction generator
        |
        v
      Kafka
        |
        v
Subscription saga
  investor service  ->  fund service
        |
        v
      Kafka
        |
        v
Projection service
        |
        v
    Postgres
```

The transaction generator emits the original subscription. The saga validates the investor, then attempts to reserve fund capacity. The projection service reads the saga outcome and writes the stored state.

## What this project is practicing

- **Inbox.** Record a message as received in the same transaction as the state change it causes, so a redelivery is recognized and not applied twice.
- **Outbox.** Record the outgoing message in the same transaction as the state change, then publish from that record, so a successful write is not lost if the process dies before the Kafka send.
- **State-transition transactions.** Commit the aggregate change and the inbox or outbox row together, so a step cannot half-succeed.
- **Idempotent replay.** `message_id` deduplicates a single command. `saga_id`, `correlation_id`, and `causation_id` make a replayed history reconstructable.

## Business event

A subscription request entering the system:

| Field | Type | Meaning |
| --- | --- | --- |
| `request_id` | uuid | Id of this incoming request |
| `request_source` | varchar | System that submitted the request |
| `subscription_id` | integer | Business id of the subscription |
| `investor_id` | integer | Investor making the contribution |
| `fund_id` | integer | Fund receiving the contribution |
| `subscription_amount` | integer | Amount contributed |
| `currency` | varchar | Currency of the amount |
| `requested_at` | timestamp | When the subscription was requested |

```json
{
  "request_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "request_source": "investor-portal",
  "subscription_id": 1001,
  "investor_id": 42,
  "fund_id": 7,
  "subscription_amount": 250000,
  "currency": "USD",
  "requested_at": "2026-10-10T15:04:05Z"
}
```

## Saga command

Services communicate with this command envelope. The payload carries the subscription facts for the current step.

| Field | Type | Meaning |
| --- | --- | --- |
| `saga_id` | uuid | Identifies the saga this message belongs to |
| `aggregate_id` | uuid | Aggregate the command is addressed to |
| `aggregate_type` | varchar | Kind of aggregate, such as investor or fund |
| `message_id` | uuid | Unique id of this message |
| `message_type` | varchar | Step in the subscription workflow |
| `correlation_id` | varchar | Subscription id, used to tie messages in the same saga together |
| `causation_id` | varchar | `message_id` of the previous message, used to follow the flow |
| `created_at` | timestamp | When this message was created |
| `schema_version` | integer | Envelope version, so later changes can be told apart |
| `payload` | object | Facts from the original business event |

Payload fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `subscription_id` | integer | Business id of the subscription |
| `investor_id` | integer | Investor making the contribution |
| `fund_id` | integer | Fund receiving the contribution |
| `amount` | float | Subscription amount carried on the command |
| `subscription_timestamp` | timestamp | When the original subscription event was created |

```json
{
  "saga_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "aggregate_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "aggregate_type": "investor",
  "message_id": "16fd2706-8baf-433b-82eb-8c7fada847da",
  "message_type": "ValidateInvestor",
  "correlation_id": "1001",
  "causation_id": "6fa459ea-ee8a-3ca4-894e-db77e160355e",
  "created_at": "2026-10-10T15:04:06Z",
  "schema_version": 1,
  "payload": {
    "subscription_id": 1001,
    "investor_id": 42,
    "fund_id": 7,
    "amount": 250000.0,
    "subscription_timestamp": "2026-10-10T15:04:05Z"
  }
}
```

`created_at` is when this command was produced. `subscription_timestamp` is when the original subscription happened. `schema_version` stays on the envelope so a consumer can detect a change in shape.

### Message types

`message_type` names the step:

| Message type | Role |
| --- | --- |
| `CreateSubscription` | Start the saga from the business event |
| `ValidateInvestor` | Ask the investor service to check the investor aggregate |
| `InvestorValidated` | Investor passed the business rules |
| `InvestorRejected` | Investor failed the business rules |
| `ReserveFundCapacity` | Ask the fund service to reserve capacity on the fund aggregate |
| `FundCapacityReserved` | Capacity was reserved |
| `FundCapacityRejected` | Capacity could not be reserved |

## Layout

```text
docker/          Kafka and Zookeeper (Confluent 7.4.0)
emitter/         Python producer and consumer used to verify Kafka
requirements.txt kafka-python
```

The broker advertises `localhost:9092` to clients on the host and `kafka:29092` to other containers on the `fund-pipeline` network.

## Run Kafka locally

From `docker/`:

```powershell
docker compose up -d
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

From the repo root, with the virtualenv active:

```powershell
python emitter/emitter.py
```

That script writes one record to the `smoke` topic and then reads the topic from offset 0. It is a connectivity check, not the subscription flow.
