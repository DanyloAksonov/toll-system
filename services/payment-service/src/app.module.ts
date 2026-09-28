import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PaymentModule } from './payment/payment.module';
import { DatabaseModule } from './database/database.module';
import { WalletModule } from './wallet/wallet.module';

@Module({
  imports: [PaymentModule, DatabaseModule, WalletModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
