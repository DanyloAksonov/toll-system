import { Controller } from '@nestjs/common';
import { VehicleDetectionService } from './vehicle-detection.service';

@Controller('vehicle-detection')
export class VehicleDetectionController {
  constructor(private readonly vehicleDetectionService: VehicleDetectionService) {}
}
