import * as amqp from 'amqp-connection-manager';
import { randomUUID } from 'crypto';
import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { Cron } from '@nestjs/schedule';
import { DataSource, EntityManager, IsNull, Repository } from 'typeorm';
import { InboxEntity } from './entities/inbox.entity';
import { OutboxEntity } from './entities/outbox.entity';
import { SagaEntity } from './entities/saga.entity';
import {
  SAGA_CORE_OPTIONS,
  SAGA_DEFINITIONS,
  SAGA_REPLIES_EXCHANGE,
  SagaCommand,
  SagaCoreOptions,
  SagaCommandMessage,
  SagaDefinitions,
  SagaReply,
  SagaStatus,
  StartSagaParams,
} from './types';

@Injectable()
export class SagaCoreService implements OnModuleInit, OnModuleDestroy {
  private ampqConnection: amqp.AmqpConnectionManager;
  private ampqChannelWrapper: amqp.ChannelWrapper;
  private ampqRepliesChannelWrapper: amqp.ChannelWrapper;
  private readonly assertedExchanges = new Set<string>();

  constructor(
    @InjectRepository(OutboxEntity)
    private outboxRepo: Repository<OutboxEntity>,
    @InjectRepository(SagaEntity)
    private sagaRepo: Repository<SagaEntity>,
    @InjectDataSource()
    private dataSource: DataSource,
    @Inject(SAGA_DEFINITIONS)
    private readonly definitions: SagaDefinitions,
    @Inject(SAGA_CORE_OPTIONS)
    private readonly options: SagaCoreOptions,
  ) {}

  onModuleInit() {
    this.ampqConnection = amqp.connect([
      process.env.RABBITMQ_URL || 'amqp://admin:admin@localhost:5672',
    ]);

    // No exchanges are asserted here. Every outbox row names its own
    // exchange/routingKey, so the relay asserts on demand (see `publish`)
    // instead of needing to know what any saga definition will ever emit.
    this.ampqChannelWrapper = this.ampqConnection.createChannel();

    // Separate channel so the reply consumer's prefetch doesn't affect the relay.
    const { queue, routingKeys } = this.options.replies;
    this.ampqRepliesChannelWrapper = this.ampqConnection.createChannel({
      setup: async (channel) => {
        // Every participant replies here, whichever step it handled.
        await channel.assertExchange(SAGA_REPLIES_EXCHANGE, 'direct', {
          durable: true,
        });

        await channel.assertQueue(queue, { durable: true });

        for (const routingKey of routingKeys) {
          await channel.bindQueue(queue, SAGA_REPLIES_EXCHANGE, routingKey);
        }

        //TODO: add DLX/DLQ for the replies queue, like payment-service does

        await channel.prefetch(1);
        await channel.consume(queue, (message) =>
          this.consumeReply(message, channel),
        );
      },
    });

    this.ampqConnection.on('error', (err, info) => {
      console.error('SagaCore RabbitMQ connection error:', err, info);
    });
  }

  async onModuleDestroy() {
    await this.ampqRepliesChannelWrapper.close();
    await this.ampqChannelWrapper.close();
    await this.ampqConnection.close();
  }

  // Handles replies from every participant of every saga this app runs.
  // Which step comes next is decided by the saga definitions, not here.
  private async consumeReply(message: any, channel: any) {
    if (!message) {
      return;
    }

    try {
      const reply: SagaReply = JSON.parse(message.content.toString());

      //TODO: validate the reply shape and nack malformed ones

      //TODO: check that the reply belongs to the saga's current step (reply.type vs saga.status),
      // so a late reply can't advance a saga that has already moved on

      //TODO: compensate all previously completed steps in reverse order -
      // compensateSaga currently only compensates the current step (see README)

      // Dedup + forward/compensate in one transaction: a reply is only
      // recorded as processed if the saga update committed too.
      await this.handleReply(reply, this.options.consumer);

      channel.ack(message);
    } catch (error) {
      //TODO: requeue retryable errors (e.g. concurrent modification) - now safe, since a failed
      // handleReply leaves no inbox record. Keep dropping/dead-lettering permanent ones (malformed reply)
      console.error(error);
      channel.nack(message, false, false);
    }
  }

  // ---- saga lifecycle ----

  async startSaga({ type, payload, idempotencyKey }: StartSagaParams) {
    const existedSaga = await this.sagaRepo.findOneBy({ idempotencyKey });
    if (existedSaga) {
      console.log(`Saga already started for idempotencyKey: ${idempotencyKey}`);
      return existedSaga;
    }

    const definition = this.definitions[type];
    if (!definition) {
      throw new Error(`No saga definition registered for type: ${type}`);
    }

    const entryTransition = definition[SagaStatus.NEW];
    const parsedPayload = JSON.parse(payload);

    return this.dataSource.transaction(async (manager) => {
      const saga = manager.create(SagaEntity, {
        idempotencyKey,
        type,
        payload: parsedPayload,
        status: SagaStatus.NEW,
      });
      await manager.save(saga);

      if (entryTransition?.command) {
        await this.enqueueCommand(
          manager,
          saga.id,
          SagaStatus.NEW,
          parsedPayload,
          entryTransition.command,
        );
      }

      return saga;
    });
  }

  // Advances a saga from its current status, based on the outcome of
  // whatever that status's command was waiting on (e.g. a reply message).
  // Pass `manager` to run inside a caller's transaction (see `handleReply`).
  async forwardSaga(
    sagaId: string,
    outcome: 'success' | 'failure',
    context: Record<string, unknown> = {},
    manager?: EntityManager,
  ) {
    if (!manager) {
      return this.dataSource.transaction((m) =>
        this.forwardSaga(sagaId, outcome, context, m),
      );
    }

    const saga = await manager.findOneBy(SagaEntity, { id: sagaId });
    if (!saga) {
      console.error(`Saga does not exist with id: ${sagaId}`);
      return null;
    }

    const definition = this.definitions[saga.type];
    if (!definition) {
      throw new Error(`No saga definition registered for type: ${saga.type}`);
    }

    const currentTransition = definition[saga.status];
    if (!currentTransition) {
      throw new Error(
        `No transition defined for saga type "${saga.type}" at status "${saga.status}"`,
      );
    }

    const nextStatus =
      outcome === 'success'
        ? currentTransition.successStatus
        : currentTransition.failureStatus;

    if (!nextStatus) {
      throw new Error(
        `Saga type "${saga.type}" at status "${saga.status}" has no ${outcome} transition defined`,
      );
    }

    const nextTransition = definition[nextStatus];
    const isTerminal =
      nextStatus === SagaStatus.COMPLETED || nextStatus === SagaStatus.FAILED;
    const mergedPayload = { ...saga.payload, ...context };

    const result = await manager.update(
      SagaEntity,
      { id: sagaId, version: saga.version },
      {
        status: nextStatus,
        payload: mergedPayload,
        version: saga.version + 1,
        ...(isTerminal ? { completedAt: new Date() } : {}),
        ...(outcome === 'failure' && context.failureReason
          ? { failureReason: String(context.failureReason) }
          : {}),
      },
    );

    if (result.affected === 0) {
      throw new Error(
        `Concurrent modification detected for saga ${sagaId} (expected version ${saga.version})`,
      );
    }

    if (nextTransition?.command) {
      await this.enqueueCommand(
        manager,
        sagaId,
        nextStatus,
        mergedPayload,
        nextTransition.command,
      );
    }
  }

  // Compensates the saga's *current* step only (emits that step's
  // `compensate` command, if any) and marks the saga FAILED.
  // Pass `manager` to run inside a caller's transaction (see `handleReply`).
  //
  // NOTE: this does not walk backward through every previously-completed
  // step. A full rollback chain needs a `prev` pointer per step, which
  // isn't implemented yet.
  async compensateSaga(
    sagaId: string,
    reason: string,
    manager?: EntityManager,
  ) {
    if (!manager) {
      return this.dataSource.transaction((m) =>
        this.compensateSaga(sagaId, reason, m),
      );
    }

    const saga = await manager.findOneBy(SagaEntity, { id: sagaId });
    if (!saga) {
      console.error(`Saga does not exist with id: ${sagaId}`);
      return null;
    }

    const definition = this.definitions[saga.type];
    const currentTransition = definition?.[saga.status];

    if (currentTransition?.compensate) {
      await this.enqueueCommand(
        manager,
        sagaId,
        saga.status,
        saga.payload,
        currentTransition.compensate,
      );
    }

    const result = await manager.update(
      SagaEntity,
      { id: sagaId, version: saga.version },
      {
        status: SagaStatus.FAILED,
        failureReason: reason,
        version: saga.version + 1,
        completedAt: new Date(),
      },
    );

    if (result.affected === 0) {
      throw new Error(
        `Concurrent modification detected for saga ${sagaId} (expected version ${saga.version})`,
      );
    }
  }

  // Applies a participant's reply. The inbox record and the saga update
  // commit (or roll back) together, so a reply whose processing failed is
  // not recorded and will be processed again when it is redelivered.
  async handleReply(reply: SagaReply, consumer: string) {
    await this.dataSource.transaction(async (manager) => {
      const isNew = await this.markProcessed(
        reply.messageId,
        consumer,
        reply.type,
        manager,
      );
      if (!isNew) {
        return; // duplicate - its effects were already committed
      }

      const failureReason =
        reply.outcome === 'success'
          ? await this.validateReplyData(reply, manager)
          : (reply.failureReason ?? `${reply.type} failed`);

      if (!failureReason) {
        await this.forwardSaga(reply.sagaId, 'success', reply.data, manager);
      } else {
        await this.compensateSaga(reply.sagaId, failureReason, manager);
      }
    });
  }

  // Runs the `validateReply` of the command the saga is currently waiting
  // on. A reply always arrives while the saga is still in the status that
  // sent the command, so that status's transition owns the validator.
  // Returns the failure reason, or null if the data is valid (or there's no
  // validator for this step).
  private async validateReplyData(
    reply: SagaReply,
    manager: EntityManager,
  ): Promise<string | null> {
    const saga = await manager.findOneBy(SagaEntity, { id: reply.sagaId });
    const validateReply =
      saga &&
      this.definitions[saga.type]?.[saga.status]?.command?.validateReply;
    if (!validateReply) {
      return null;
    }

    const error = await validateReply(reply.data ?? {});
    return error ? `Invalid ${reply.type} reply: ${error}` : null;
  }

  private async enqueueCommand(
    manager: EntityManager,
    sagaId: string,
    status: string,
    payload: Record<string, unknown>,
    command: SagaCommand,
  ) {
    // Generated here (not by the DB) because it has to be inside the stored
    // payload, so every relay retry republishes the same messageId.
    const message: SagaCommandMessage = {
      sagaId,
      messageId: randomUUID(),
      type: command.routingKey,
      data: command.buildPayload ? command.buildPayload(payload) : payload,
    };

    const outboxMessage = manager.create(OutboxEntity, {
      saga: { id: sagaId } as SagaEntity,
      exchange: command.exchange,
      routingKey: command.routingKey,
      messageType: status,
      payload: message,
    });
    await manager.save(outboxMessage);
  }

  // ---- inbox dedup ----

  // Returns false if this (messageId, consumer) pair was already recorded
  // (i.e. this message was already processed) - caller should skip it.
  //
  // Uses ON CONFLICT DO NOTHING rather than catching the unique violation:
  // in Postgres a failed statement aborts the whole transaction, which would
  // break every later query when this runs inside `handleReply`'s transaction.
  async markProcessed(
    messageId: string,
    consumer: string,
    messageType: string,
    manager: EntityManager = this.dataSource.manager,
  ): Promise<boolean> {
    const result = await manager
      .createQueryBuilder()
      .insert()
      .into(InboxEntity)
      .values({ messageId, consumer, messageType })
      .orIgnore()
      .returning(['messageId'])
      .execute();

    return result.raw.length > 0;
  }

  // ---- outbox relay ----

  @Cron('*/5 * * * * *')
  async relayOutbox() {
    const pending = await this.outboxRepo.find({
      where: { publishedAt: IsNull() },
      order: { createdAt: 'ASC' },
      take: 50,
    });

    for (const message of pending) {
      try {
        await this.publish(message);
        await this.outboxRepo.update(message.id, { publishedAt: new Date() });
      } catch (error) {
        console.error(`Failed to relay outbox message ${message.id}:`, error);
        await this.outboxRepo.update(message.id, {
          attempts: message.attempts + 1,
          lastError: String(error),
        });
      }
    }
  }

  private async publish(message: OutboxEntity) {
    if (!this.assertedExchanges.has(message.exchange)) {
      await this.ampqChannelWrapper.addSetup((channel) =>
        channel.assertExchange(message.exchange, 'direct', { durable: true }),
      );
      this.assertedExchanges.add(message.exchange);
    }

    await this.ampqChannelWrapper.publish(
      message.exchange,
      message.routingKey,
      Buffer.from(JSON.stringify(message.payload)),
    );
  }
}
