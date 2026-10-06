import {
  Injectable,
  NotFoundException,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UserEntity } from 'src/user/entities/user.entity';
import * as amqp from 'amqp-connection-manager';

@Injectable()
export class UserService implements OnModuleInit, OnModuleDestroy {
  private connection: amqp.AmqpConnectionManager;
  private channelWrapper: amqp.ChannelWrapper;
  private readonly ACCOUNT_COMMANDS_EXCHANGE = 'account.commands';
  private readonly ACCOUNT_VERIFY_QUEUE = 'account.commands.verify';
  private readonly ACCOUNT_VERIFY_ROUTING_KEY = 'account.verify';

  constructor(
    @InjectRepository(UserEntity)
    private userRepo: Repository<UserEntity>,
  ) {}

  onModuleInit() {
    this.connection = amqp.connect([
      process.env.RABBITMQ_URL || 'amqp://admin:admin@localhost:5672',
    ]);

    this.channelWrapper = this.connection.createChannel({
      setup: async (channel) => {
        await channel.assertExchange(this.ACCOUNT_COMMANDS_EXCHANGE, 'direct', {
          durable: true,
        });
        await channel.assertQueue(this.ACCOUNT_VERIFY_QUEUE, { durable: true });
        await channel.bindQueue(
          this.ACCOUNT_VERIFY_QUEUE,
          this.ACCOUNT_COMMANDS_EXCHANGE,
          this.ACCOUNT_VERIFY_ROUTING_KEY,
        );
        await channel.consume(this.ACCOUNT_VERIFY_QUEUE, (eventMessage) =>
          this.verifyAccount(eventMessage),
        );
      },
    });
  }

  onModuleDestroy() {}

  async findById(id: string): Promise<UserEntity> {
    const user = await this.userRepo.findOne({ where: { id } });
    if (!user) {
      throw new NotFoundException(`User ${id} not found`);
    }
    return user;
  }

  async verifyAccount(eventMessage: any) {
    console.log('Verifying account...', eventMessage);
    const parsedMessage = JSON.parse(eventMessage.content.toString());
    const { eventId, plateNumber, gateId } = parsedMessage;
  }
}
