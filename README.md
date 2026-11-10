# outbox-relay

An implementation of the transactional outbox pattern in Go: a relay that reads events from a PostgreSQL table and publishes them to NATS JetStream. A learning project to understand how services can send events without losing them and without distributed transactions.

The idea: your app writes the event into an `outbox` table **in the same transaction** as the business data. The relay picks up new rows and publishes them. If the transaction rolled back, there is no event; if it committed, the event will be published sooner or later, even if NATS was down at that moment.

- Rows are taken with `FOR UPDATE SKIP LOCKED`, so a second relay instance doesn't publish the same rows
- Published in `id` order, a failed message stops the batch so the order is kept
- Every message has `Nats-Msg-Id` = outbox id, so JetStream drops duplicates if the relay crashes between publishing and marking the row
- Graceful shutdown on `SIGTERM`

How it works and why it's needed is described (in Russian) in [docs/how-it-works.md](docs/how-it-works.md), known limitations in [docs/limitations.md](docs/limitations.md).

## Running

```bash
docker compose up -d --build
```

This starts PostgreSQL (port 5433), NATS with JetStream (port 4222) and the relay.

```bash
go run ./examples/consumer
```

In another terminal:

```bash
go run ./examples/producer -customer alice -total 1500
```

The producer creates an order and an `order.created` event in one transaction; the consumer prints it a moment later:

```
events.order.created (outbox id 1): {"total": 1500, "customer": "alice", "order_id": 1}
```

## Writing to the outbox from your app

```sql
BEGIN;
INSERT INTO orders (customer, total) VALUES ('alice', 1500) RETURNING id;
INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
VALUES ('order', '1', 'order.created', '{"order_id": 1, "total": 1500}');
COMMIT;
```

The event is published to the subject `events.<event_type>`, headers carry `Outbox-Id`, `Aggregate-Type`, `Aggregate-Id` and `Event-Type`. The app can be written in any language, it only needs the table.

## Configuration

| Variable | Default |
|---|---|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5433/shop?sslmode=disable` |
| `NATS_URL` | `nats://127.0.0.1:4222` |
| `BATCH_SIZE` | `100` |
| `POLL_INTERVAL` | `1s` |

## Tests

```bash
make test
```

Starts PostgreSQL in Docker; NATS runs inside the test process.
