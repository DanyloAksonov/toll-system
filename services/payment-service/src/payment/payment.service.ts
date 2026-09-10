import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqp-connection-manager';
import { TransactionEntity } from 'src/payment/entities/transaction.entity';
import { Repository } from 'typeorm';
import { InjectRepository } from '@nestjs/typeorm';

@Injectable()
export class PaymentService implements OnModuleInit, OnModuleDestroy {
  private connection: amqp.AmqpConnectionManager;
  private channelWrapper: amqp.ChannelWrapper;

  constructor(
    @InjectRepository(TransactionEntity)
    private transactionRepo: Repository<TransactionEntity>,
  ) {}

  async onModuleInit() {
    this.connection = amqp.connect([
      process.env.RABBITMQ_URL || 'amqp://admin:admin@localhost:5672',
    ]);

    this.channelWrapper = this.connection.createChannel({
      setup: (channel) => {
        return Promise.all([
          channel.assertExchange('vehicle', 'fanout', { durable: true }),
          channel.assertQueue('payment.vehicle.queue', { durable: true }),
          channel.bindQueue('payment.vehicle.queue', 'vehicle', ''),
          channel.prefetch(1),
          channel.consume('payment.vehicle.queue', (message) =>
            this.handlePayment(message, channel),
          ),
        ]);
      },
    });
  }

  async onModuleDestroy() {
    await this.channelWrapper.close();
    await this.connection.close();
  }

  async handlePayment(message: any, channel: any) {
    if (!message) {
      return null;
    }

    try {
      console.log(JSON.stringify(message));

      const parsedMessage = JSON.parse(message.content.toString());
      const { eventId, plateNumber, gateId } = parsedMessage;
      console.log(`Message received: ${JSON.stringify(parsedMessage)}`);

      if (eventId) {
        const existMessage = await this.transactionRepo.findOne({
          where: { eventId },
        });
        if (existMessage) {
          console.log(`Message already exist eventId: ${eventId}`);
          channel.ack(message);
          return null;
        }
        const messageToSave = this.transactionRepo.create({
          amount: 5.0,
          eventId,
          plateNumber,
          status: 'completed',
          gateId,
        });
        await this.transactionRepo.save(messageToSave);
        channel.ack(message);
      }
    } catch (error) {
      console.error(error);
      channel.nack(message, false, false);
    }
  }
}
