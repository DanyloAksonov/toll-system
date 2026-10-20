import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
  ServiceUnavailableException,
} from '@nestjs/common';
import { IDetectVehicle } from './interfaces/IDetectVehicle';
import * as amqp from 'amqp-connection-manager';
import { InjectRepository } from '@nestjs/typeorm';
import { OutboxEntity } from './entities/Outbox.entity';
import { Repository } from 'typeorm';
import { Cron } from '@nestjs/schedule';

@Injectable()
export class VehicleService implements OnModuleInit, OnModuleDestroy {
  private connection: amqp.AmqpConnectionManager;
  private channelWrapper: amqp.ChannelWrapper;
  private readonly VEHICLE_EXCHANGE = 'vehicle';
  private readonly VEHICLE_EXCHANGE_COMMANDS = 'vehicle.commands';

  constructor(
    @InjectRepository(OutboxEntity)
    private outboxRepo: Repository<OutboxEntity>,
  ) {}

  async onModuleInit() {
    this.connection = amqp.connect([
      process.env.RABBITMQ_URL || 'amqp://admin:admin@localhost:5672',
    ]);

    try {
      this.channelWrapper = this.connection.createChannel({
        setup: async (channel: amqp.Channel) => {
          await Promise.all([
            channel.assertExchange(this.VEHICLE_EXCHANGE, 'fanout', {
              durable: true,
            }),
            channel.assertExchange(this.VEHICLE_EXCHANGE_COMMANDS, 'direct', {
              durable: true,
            }),
          ]);
        },
      });
    } catch (error) {
      console.log(error);
    }

    console.log('Connected to RabbitMQ');
  }

  async onModuleDestroy() {
    await this.channelWrapper.close();
    await this.connection.close();
  }

  async detectVehicle(params: IDetectVehicle): Promise<any> {
    const event = {
      plateNumber: params.plateNumber,
      gateId: params.gateId,
      timestamp: new Date().toISOString(),
      eventId: crypto.randomUUID(), // для idempotency
    };
    console.log(event);

    try {
      const eventToSave = this.outboxRepo.create({
        payload: JSON.stringify(event),
        status: 'pending',
        eventType: this.VEHICLE_EXCHANGE,
      });
      const savedEvent = await this.outboxRepo.save(eventToSave);
      await this.channelWrapper.publish(
        this.VEHICLE_EXCHANGE,
        '',
        Buffer.from(JSON.stringify(event)),
        { timeout: 5000 },
      );
      // await this.channelWrapper.sendToQueue(
      //   this.QUEUE,
      //   Buffer.from(JSON.stringify(event)),
      //   { persistent: true, timeout: 5000 }, // повідомлення збережеться якщо RabbitMQ перезапуститься
      // );
      await this.outboxRepo.update(savedEvent.uuid, {
        status: 'processed',
      });
    } catch (error) {
      console.error('Failed to publish event:', error);
      return new ServiceUnavailableException('Failed to publish event');
    }

    return event;
  }

  async detectVehicle_saga(params: IDetectVehicle): Promise<any> {
    //Rename this method on detectVehicle, after implementing another method with regular communication without sagas
    const event = {
      plateNumber: params.plateNumber,
      gateId: params.gateId,
      timestamp: new Date().toISOString(),
      eventId: crypto.randomUUID(),
    };
    console.log(event);

    try {
      const eventToSave = this.outboxRepo.create({
        payload: JSON.stringify(event),
        status: 'pending',
        eventType: this.VEHICLE_EXCHANGE_COMMANDS,
      });
      const savedEvent = await this.outboxRepo.save(eventToSave);
      await this.channelWrapper.publish(
        this.VEHICLE_EXCHANGE_COMMANDS,
        'vehicle.detection',
        Buffer.from(JSON.stringify(event)),
        { timeout: 5000 },
      );
      // await this.channelWrapper.sendToQueue(
      //   this.QUEUE,
      //   Buffer.from(JSON.stringify(event)),
      //   { persistent: true, timeout: 5000 }, // повідомлення збережеться якщо RabbitMQ перезапуститься
      // );
      await this.outboxRepo.update(savedEvent.uuid, {
        status: 'processed',
      });
    } catch (error) {
      console.error('Failed to publish event:', error);
      return new ServiceUnavailableException('Failed to publish event');
    }

    return event;
  }

  @Cron('10 * * * * *')
  async republishEventsFromOutbox() {
    const outboxEvents = await this.outboxRepo.find({
      where: { status: 'pending' },
      take: 100,
      order: { createdAt: 'ASC' },
    });
    await Promise.allSettled(
      outboxEvents.map(async (event) => {
        try {
          await this.channelWrapper.sendToQueue(
            event.eventType,
            Buffer.from(event.payload),
            { persistent: true },
          );
          await this.outboxRepo.update(event.uuid, { status: 'processed' });
        } catch (err) {
          console.error(`Failed to publish event ${event.uuid}:`, err);
        }
      }),
    );
  }
}
