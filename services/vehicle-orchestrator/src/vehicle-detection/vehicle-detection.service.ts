import * as amqp from 'amqp-connection-manager';

import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { SagaCoreService } from 'saga-core';
import { VehicleSaga } from './sagas/vehicle-detection.transitions';

@Injectable()
export class VehicleDetectionService implements OnModuleInit, OnModuleDestroy {
  private ampqConnection: amqp.AmqpConnectionManager;
  private ampqChannelWrapper: amqp.ChannelWrapper;
  private readonly VEHICLE_COMMANDS_EXCHANGE = 'vehicle.commands';
  private readonly VEHICLE_DETECTION_ROUTING_KEY = 'vehicle.detection';
  private readonly VEHICLE_DETECTION_QUEUE =
    'vehicle-orchestrator.vehicle.detection';

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

        //TODO: add DLX/DLQ for the detection queue, like payment-service does

        await channel.prefetch(1);
        await channel.consume(this.VEHICLE_DETECTION_QUEUE, (message) =>
          this.vehicleDetection(message, channel),
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
  // Every next step is driven by saga-core's reply consumer, never from here.
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

  //TODO: add a validateReply (see sagas/vehicle-detection.validators.ts) to every other step's command once it's defined

  //TODO: when the saga reaches COMPLETED, notify payment-service about the completed vehicle detection
  // (as the last step's command in the saga definition)
}
