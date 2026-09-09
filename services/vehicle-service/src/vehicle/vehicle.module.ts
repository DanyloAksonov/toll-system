import { Module } from '@nestjs/common';
import { VehicleService } from './vehicle.service';
import { VehicleController } from './vehicle.controller';
import { TypeOrmModule } from '@nestjs/typeorm';
import { OutboxEntity } from './entities/Outbox.entity';

@Module({
  imports: [TypeOrmModule.forFeature([OutboxEntity])],
  controllers: [VehicleController],
  providers: [VehicleService],
})
export class VehicleModule {}
