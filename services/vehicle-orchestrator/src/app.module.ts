import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { VehicleDetectionModule } from './vehicle-detection/vehicle-detection.module';
import { SagaCoreModule } from 'saga-core';
import { DatabaseModule } from './database/database.module';
import {
  VehicleSaga,
  VEHICLE_DETECTIONS_TRANSITIONS,
} from './vehicle-detection/sagas/vehicle-detection.transitions';

@Module({
  imports: [
    DatabaseModule,
    ScheduleModule.forRoot(),
    SagaCoreModule.forRoot({
      definitions: {
        [VehicleSaga.VehicleDetection]: VEHICLE_DETECTIONS_TRANSITIONS,
      },
      consumer: 'vehicle-orchestrator',
      replies: {
        queue: 'vehicle-orchestrator.saga.replies',
        routingKeys: ['vehicle-detection.reply'],
      },
    }),
    VehicleDetectionModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
