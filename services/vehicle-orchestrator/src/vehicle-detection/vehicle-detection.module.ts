import { Module } from '@nestjs/common';
import { VehicleDetectionService } from './vehicle-detection.service';
import { VehicleDetectionController } from './vehicle-detection.controller';

@Module({
  controllers: [VehicleDetectionController],
  providers: [VehicleDetectionService],
})
export class VehicleDetectionModule {}
