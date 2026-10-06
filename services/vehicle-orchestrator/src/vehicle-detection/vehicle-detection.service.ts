import * as amqp from 'amqp-connection-manager';

import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { SagaCoreService, SagaReply } from 'saga-core';
import { VehicleSaga } from './sagas';

@Injectable()
export class VehicleDetectionService implements OnModuleInit, OnModuleDestroy {
  private ampqConnection: amqp.AmqpConnectionManager;
  private ampqChannelWrapper: amqp.ChannelWrapper;
  private readonly VEHICLE_COMMANDS_EXCHANGE = 'vehicle.commands';
  private readonly VEHICLE_DETECTION_ROUTING_KEY = 'vehicle.detection';
  private readonly VEHICLE_DETECTION_QUEUE =
    'vehicle-orchestrator.vehicle.detection';
  private readonly SAGA_REPLIES_EXCHANGE = 'saga.replies';
  private readonly VEHICLE_DETECTION_REPLY_ROUTING_KEY =
    'vehicle-detection.reply';
  private readonly SAGA_REPLIES_QUEUE = 'vehicle-orchestrator.saga.replies';

  constructor(
    @InjectDataSource()
    private dataSource: DataSource,
    private readonly sagaCoreService: SagaCoreService,
  ) {}

  onModuleInit() {
    this.ampqConnection = amqp.connect([
      process.env.RABBITMQ_URL || 'amqp://admin:admin@localhost:5672',
    ]);
    this.ampqChannelWrapper = this.ampqConnection.createChannel({
      setup: async (channel) => {
        await channel.assertExchange(this.VEHICLE_COMMANDS_EXCHANGE, 'direct', {
          durable: true,
        });

        await channel.assertQueue(this.VEHICLE_DETECTION_QUEUE, {
          durable: true,
        });

        await channel.bindQueue(
          this.VEHICLE_DETECTION_QUEUE,
          this.VEHICLE_COMMANDS_EXCHANGE,
          this.VEHICLE_DETECTION_ROUTING_KEY,
        );

        // Every participant replies here, whichever step it handled.
        await channel.assertExchange(this.SAGA_REPLIES_EXCHANGE, 'direct', {
          durable: true,
        });

        await channel.assertQueue(this.SAGA_REPLIES_QUEUE, {
          durable: true,
        });

        await channel.bindQueue(
          this.SAGA_REPLIES_QUEUE,
          this.SAGA_REPLIES_EXCHANGE,
          this.VEHICLE_DETECTION_REPLY_ROUTING_KEY,
        );

        //TODO: add DLX/DLQ for both queues, like payment-service does

        await channel.prefetch(1);
        await channel.consume(this.VEHICLE_DETECTION_QUEUE, (message) =>
          this.vehicleDetection(message, channel),
        );
        await channel.consume(this.SAGA_REPLIES_QUEUE, (message) =>
          this.sagaReply(message, channel),
        );

        console.log('Vehicle Detection Service listening to RabbitMQ...');
      },
    });

    this.ampqConnection.on('error', (err, info) => {
      console.error('RabbitMQ channel error:', err, info);
    });
  }

  onModuleDestroy() {}

  // Only starts the saga. The NEW step's command (account.verify) is written
  // to the outbox in the same transaction and published by saga-core's relay.
  // Every next step is driven by `sagaReply`, never from here.
  async vehicleDetection(message: any, channel: any) {
    if (!message) {
      return;
    }

    try {
      const payload = message.content.toString();
      const { eventId } = JSON.parse(payload);

      //TODO: validate the incoming event (plateNumber, gateId, eventId) and nack invalid ones

      // Idempotent on eventId - a redelivered detection returns the existing
      // saga instead of starting (or advancing) it again.
      await this.sagaCoreService.startSaga({
        type: VehicleSaga.VehicleDetection,
        idempotencyKey: eventId,
        payload,
      });

      channel.ack(message);
    } catch (error) {
      console.error(error);
      channel.nack(message, false, false);
    }
  }

  // Handles replies from every participant: account-service (userId),
  // vehicle-service (registration), payment-service (money reservation),
  // tag-service (tag activation). Which step comes next is decided by the
  // saga definition in ./sagas, not here.
  async sagaReply(message: any, channel: any) {
    if (!message) {
      return;
    }

    try {
      const reply: SagaReply = JSON.parse(message.content.toString());

      //TODO: validate the reply shape and nack malformed ones

      //TODO: check that the reply belongs to the saga's current step (reply.type vs saga.status),
      // so a late reply can't advance a saga that has already moved on

      //TODO: validate step-specific data (e.g. userId from account-service) and treat invalid data as failure

      //TODO: compensate all previously completed steps in reverse order -
      // compensateSaga currently only compensates the current step (see saga-core README)

      // Dedup + forward/compensate in one transaction: a reply is only
      // recorded as processed if the saga update committed too.
      await this.sagaCoreService.handleReply(reply, 'vehicle-orchestrator');

      //TODO: when the saga reaches COMPLETED, notify payment-service about the completed vehicle detection
      // (better as the last step's command in the saga definition than as a publish from here)

      channel.ack(message);
    } catch (error) {
      //TODO: requeue retryable errors (e.g. concurrent modification) - now safe, since a failed
      // handleReply leaves no inbox record. Keep dropping/dead-lettering permanent ones (malformed reply)
      console.error(error);
      channel.nack(message, false, false);
    }
  }
}
