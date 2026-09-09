# Toll Payment System — Microservices Demo

## Architecture

Services:

- Vehicle Service - service for receiving car recognition data
- Payment Service - service for handling payment transactions
- Account Service - stub service for future implementation of user authentication
- RabbitMQ - event bus for communication between microservices
- Vehicle PG - database for tracking outbox events (owned by Vehicle Service)
- Payment PG - database for keeping records of transactions (owned by Payment Service)

Each service owns its own PostgreSQL instance (database-per-service) rather than sharing a single database.

```mermaid
graph LR
    Client([Client]) -->|POST /vehicle/detect| VS[Vehicle Service<br/>:4000]

    VS -->|writes outbox row| VPG[(Vehicle PG<br/>:5433)]
    VS -->|publish: vehicle.detected| MQ{{RabbitMQ<br/>:5672 / UI :15672}}
    VS -.cron republish.-> VPG

    MQ -->|consume: vehicle.detected| PS[Payment Service<br/>:4005]
    PS -->|idempotency check + write transaction| PPG[(Payment PG<br/>:5432)]

    AS[Account Service<br/>:4010<br/><i>stub, not implemented</i>]
```

## Key Patterns

- **Outbox Pattern** — guaranteed event delivery. `Vehicle Service` writes the event to an `outbox` table in the same transaction as the business action, then a cron job republishes any `pending` rows that failed to publish to RabbitMQ.
- **Idempotency** — duplicate event protection. `Payment Service` checks the incoming message's `eventId` against existing `transaction` rows before processing, and acks/discards duplicates.
- **Dead Letter Queue** — failed message handling. **Planned, not yet implemented** — no DLQ/`x-dead-letter-exchange` is currently configured on the RabbitMQ queues.

### Outbox + Idempotency flow

```mermaid
sequenceDiagram
    participant C as Client
    participant VS as Vehicle Service
    participant VPG as Vehicle PG (outbox)
    participant MQ as RabbitMQ
    participant PS as Payment Service
    participant PPG as Payment PG (transactions)

    C->>VS: POST /vehicle/detect
    VS->>VPG: save outbox row (status: pending)
    VS->>MQ: publish vehicle.detected
    VS->>VPG: update outbox row (status: processed)

    Note over VS,VPG: Cron job (every second) republishes<br/>any rows still "pending"

    MQ->>PS: deliver vehicle.detected
    PS->>PPG: check transaction by eventId
    alt eventId already processed
        PS->>MQ: ack (skip duplicate)
    else new event
        PS->>PPG: save transaction
        PS->>MQ: ack
    end
```

## Tech Stack

NestJS, TypeScript, RabbitMQ, PostgreSQL (TypeORM), Docker Compose

## How to Run

The compose file lives under `infrastructure/dev/`, not the repo root:

```bash
docker compose -f infrastructure/dev/docker-compose.yml up -d --build
```

To (re)build and refresh a single service after changing its dependencies (npm install doesn't propagate into the container's node_modules volume automatically):

```bash
docker compose -f infrastructure/dev/docker-compose.yml up -d --build --force-recreate --renew-anon-volumes <service-name>
```

### Required environment variables (set in `infrastructure/dev/docker-compose.yml`)

| Variable | Used by | Default (local dev) |
|---|---|---|
| `RABBITMQ_URL` | vehicle-service, payment-service | `amqp://admin:admin@localhost:5672` |
| `DB_HOST` | vehicle-service, payment-service | `localhost` |
| `DB_PORT` | vehicle-service, payment-service | `5432` |
| `DB_USERNAME` / `DB_PASSWORD` / `DB_NAME` | vehicle-service, payment-service | `postgres` / `postgres` / `postgres` |

## Services

- Vehicle Service: http://localhost:4000
- Payment Service: http://localhost:4005
- Account Service: http://localhost:4010
- RabbitMQ UI: http://localhost:15672 (admin / admin)
- Vehicle PG: `localhost:5433` (mapped from container port 5432)
- Payment PG: `localhost:5432`
