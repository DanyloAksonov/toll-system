import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqp-connection-manager';
import { TransactionEntity } from 'src/payment/entities/transaction.entity';
import { DataSource, Repository } from 'typeorm';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { WalletEntity } from 'src/wallet/entities/wallet.entity';

@Injectable()
export class PaymentService implements OnModuleInit, OnModuleDestroy {
  private connection: amqp.AmqpConnectionManager;
  private channelWrapper: amqp.ChannelWrapper;
  private readonly VEHICLE_EXCHANGE = 'vehicle';
  private readonly VEHICLE_ARRIVAL_QUEUE = 'payment.vehicle.arrival';
  private readonly VEHICLE_DEPARTURE_QUEUE = 'payment.vehicle.departure';
  private readonly PAYMENT_DLX = 'payment.dlx';
  private readonly VEHICLE_ARRIVAL_DLQ = 'payment.vehicle.arrival.dlq';

  constructor(
    @InjectRepository(TransactionEntity)
    private transactionRepo: Repository<TransactionEntity>,
    @InjectRepository(WalletEntity)
    private walletRepo: Repository<WalletEntity>,
    @InjectDataSource()
    private dataSource: DataSource,
  ) {}

  async onModuleInit() {
    this.connection = amqp.connect([
      process.env.RABBITMQ_URL || 'amqp://admin:admin@localhost:5672',
    ]);

    this.channelWrapper = this.connection.createChannel({
      setup: async (channel) => {
        await Promise.all([
          channel.assertExchange(this.VEHICLE_EXCHANGE, 'fanout', {
            durable: true,
          }),
          channel.assertExchange(this.PAYMENT_DLX, 'direct', {
            durable: true,
          }),
        ]);

        await Promise.all([
          channel.assertQueue(this.VEHICLE_ARRIVAL_QUEUE, {
            durable: true,
            arguments: {
              'x-dead-letter-exchange': this.PAYMENT_DLX,
              'x-dead-letter-routing-key': this.VEHICLE_ARRIVAL_DLQ,
              'x-message-ttl': 30000,
            },
          }),
          channel.assertQueue(this.VEHICLE_ARRIVAL_DLQ, { durable: true }),
        ]);

        await Promise.all([
          channel.bindQueue(
            this.VEHICLE_ARRIVAL_QUEUE,
            this.VEHICLE_EXCHANGE,
            '',
          ),
          channel.bindQueue(
            this.VEHICLE_ARRIVAL_DLQ,
            this.PAYMENT_DLX,
            this.VEHICLE_ARRIVAL_DLQ,
          ),
        ]);

        await channel.prefetch(1);
        await channel.consume(this.VEHICLE_ARRIVAL_QUEUE, (message) =>
          this.handlePayment(message, channel),
        );

        console.log('Payment Service listening to RabbitMQ...');
      },
    });

    this.channelWrapper.on('error', (err, info) => {
      console.error('RabbitMQ channel error:', err, info);
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

        const amount = 5.0; //Move this value to config

        const userWallet = await this.walletRepo.findOne({
          where: {
            userId: 'paste here user id',
          },
        });
        if (!userWallet || userWallet.balance < amount) {
          channel.nack(message, false, false);
          return null;
        }

        await this.dataSource.transaction(async (manager) => {
          const messageToSave = manager.create(TransactionEntity, {
            amount,
            eventId,
            plateNumber,
            status: 'completed',
            gateId,
          });
          await manager.save(messageToSave);

          await manager.decrement(
            WalletEntity,
            { id: userWallet.id },
            'balance',
            amount,
          );
        });

        channel.ack(message);
      }
    } catch (error) {
      console.error(error);
      channel.nack(message, false, false);
    }
  }
}
