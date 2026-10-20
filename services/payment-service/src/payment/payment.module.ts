import { Module } from '@nestjs/common';
import { PaymentService } from './payment.service';
import { PaymentController } from './payment.controller';
import { TypeOrmModule } from '@nestjs/typeorm';
import { TransactionEntity } from 'src/payment/entities/transaction.entity';
import { WalletEntity } from 'src/wallet/entities/wallet.entity';

@Module({
  imports: [TypeOrmModule.forFeature([TransactionEntity, WalletEntity])],
  controllers: [PaymentController],
  providers: [PaymentService],
})
export class PaymentModule {}
