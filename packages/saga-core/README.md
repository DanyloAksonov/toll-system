# saga-core

A generic, embeddable saga orchestration engine for NestJS services. Implements the
saga/outbox/inbox pattern: a `sagas` table tracks each saga's current status, an
`outbox` table holds commands to publish over RabbitMQ (relayed by a background job),
and an `inbox` table deduplicates incoming replies.

This package contains no business logic of its own — it only knows how to walk a
state machine you define (a `SagaDefinitions` map) and reliably publish/consume
messages around each transition. Each consuming service supplies its own saga
definitions, its own RabbitMQ exchange/routing-key names, and its own database
connection.

## Installing

Not published to a registry. Each consumer depends on it via a local path:

```json
"dependencies": {
  "saga-core": "file:../../packages/saga-core"
}
```

Note: Yarn Classic **copies** `file:` dependencies rather than symlinking them. After
changing this package, run `yarn build` here, then `yarn install` (and rebuild the
Docker image, if containerized) in every consuming service to pick up the change.

## Peer dependencies

The consuming app must already have these installed, at compatible versions:

- `@nestjs/common`, `@nestjs/core`
- `@nestjs/typeorm`, `typeorm`
- `@nestjs/schedule` (the outbox relay runs via `@Cron`)
- `amqp-connection-manager`
- `reflect-metadata`

These are peer dependencies, not bundled dependencies, specifically so the consumer's
own instances are what get used — bundling separate copies risks two different
`typeorm`/`@nestjs/*` module instances existing in one process, which can break
decorator metadata resolution (`@InjectRepository`, etc.) in hard-to-diagnose ways.

## Setup

### 1. Register the entities with your DataSource

`SagaCoreModule` only calls `TypeOrmModule.forFeature(...)` — it does **not** register
entity metadata with your connection. Your own `TypeOrmModule.forRoot(...)` call must
include this package's entities explicitly (a glob pattern won't find them, since
they live in `node_modules`, not your own compiled output):

```ts
import { InboxEntity, OutboxEntity, SagaEntity } from 'saga-core';

TypeOrmModule.forRoot({
  // ...your connection config...
  entities: [
    join(__dirname, '..', '**', '*.entity{.ts,.js}'), // your own entities
    SagaEntity,
    OutboxEntity,
    InboxEntity,
  ],
  synchronize: true,
});
```

### 2. Define your saga(s)

A `SagaDefinition` is keyed by **status**, not by saga type — each saga type has
exactly one definition, but that definition needs one entry per step it can be in.
`successStatus`/`failureStatus` say where to go next; `command` is the side effect to
emit upon *entering* that status; `compensate` is how to undo it.

```ts
import { SagaDefinition, SagaStatus } from 'saga-core';

export const REGISTER_VEHICLE_TRANSITIONS: SagaDefinition = {
  [SagaStatus.NEW]: {
    successStatus: 'AccountVerification',
    failureStatus: SagaStatus.FAILED,
    command: {
      exchange: 'account.commands',
      routingKey: 'account.verify',
      // Optional: send only what this participant needs. Without it, the
      // participant receives the saga's whole accumulated payload.
      buildPayload: (p) => ({ plateNumber: p.plateNumber }),
    },
  },
  AccountVerification: {
    successStatus: 'ChargingWallet',
    failureStatus: SagaStatus.FAILED,
  },
  ChargingWallet: {
    successStatus: SagaStatus.COMPLETED,
    failureStatus: 'CompensatingWallet',
    compensate: { exchange: 'payment.commands', routingKey: 'wallet.refund' },
  },
};
```

### 3. Register the module

Call `SagaCoreModule.forRoot(...)` once, from your `AppModule` — it's marked `global`,
so `SagaCoreService` becomes injectable anywhere in the app without re-importing the
module. Also make sure `ScheduleModule.forRoot()` is imported somewhere (the outbox
relay's `@Cron` needs it; this package does not register it itself, since a shared
library shouldn't assume it owns that app-wide concern).

```ts
import { ScheduleModule } from '@nestjs/schedule';
import { SagaCoreModule } from 'saga-core';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    SagaCoreModule.forRoot({
      RegisterVehicle: REGISTER_VEHICLE_TRANSITIONS,
    }),
    // ...
  ],
})
export class AppModule {}
```

## Using `SagaCoreService`

- **`startSaga({ type, payload, idempotencyKey })`** — creates a saga (idempotent on
  `idempotencyKey`: calling it again with the same key returns the existing saga
  instead of creating a duplicate), and enqueues the `NEW` status's command, if any,
  in the same transaction.
- **`forwardSaga(sagaId, outcome, context?)`** — advances a saga from its current
  status based on `'success' | 'failure'`, using optimistic concurrency (throws if the
  saga's `version` changed concurrently). Enqueues the next status's command, if any,
  in the same transaction as the status update.
- **`compensateSaga(sagaId, reason)`** — emits the *current* status's `compensate`
  command (if defined) and marks the saga `FAILED`. **Does not** walk backward through
  every previously-completed step — only the current one. A full rollback chain needs
  a `prev` pointer per step, which isn't implemented yet.
- **`markProcessed(messageId, consumer, messageType)`** — inbox dedup guard for
  incoming reply consumers. Returns `false` if this `(messageId, consumer)` pair was
  already recorded (meaning: skip it, it's a redelivery); `true` if newly recorded.

## Command message format

Every command (and compensation) is published as a `SagaCommandMessage`:

```ts
{
  sagaId: string;    // echo this back in the reply so the orchestrator can forwardSaga()
  messageId: string; // stable across relay retries - use it with markProcessed()
  type: string;      // the command's routingKey
  data: object;      // command.buildPayload(sagaPayload), or the whole payload if omitted
}
```

`saga.payload` itself stays the orchestrator's private state: replies passed to
`forwardSaga(sagaId, outcome, context)` are merged into it, and each step's
`buildPayload` picks what its participant should see.

## Outbox relay

A `@Cron('*/5 * * * * *')` job polls `outbox` for unpublished rows and publishes them.
Each row carries its own `exchange`/`routingKey`/`payload` — the relay asserts the
exchange on demand (as `direct`, cached after the first time) and publishes, with no
knowledge of what exchanges exist ahead of time. On failure, `attempts`/`last_error`
are recorded and the row is retried on the next tick.

## Known limitations

- `compensateSaga` only compensates the current step (see above).
- The outbox relay asserts every exchange as `type: 'direct'` — a saga needing to
  publish to a `fanout`/`topic` exchange isn't supported yet.
- No admin/inspection HTTP surface is exposed by default beyond an empty
  `SagaCoreController` (mounted at `/saga-core` in whatever app imports this module).
