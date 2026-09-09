import { Module } from '@nestjs/common';
import { PaymentService } from './payment.service';
import { PaymentController } from './payment.controller';
import { TypeOrmModule } from '@nestjs/typeorm';
import { TransactionEntity } from 'src/payment/entities/transaction.entity';

@Module({
  imports: [TypeOrmModule.forFeature([TransactionEntity])],
  controllers: [PaymentController],
  providers: [PaymentService],
})
export class PaymentModule {}
