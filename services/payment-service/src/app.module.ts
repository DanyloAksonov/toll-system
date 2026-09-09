import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PaymentModule } from './payment/payment.module';
import { DatabaseModule } from './database/database.module';

@Module({
  imports: [PaymentModule, DatabaseModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
