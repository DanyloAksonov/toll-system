import { Controller, Post, Body } from '@nestjs/common';
import { VehicleService } from './vehicle.service';
import { DetectVehicleDto } from './dto/input/detect-vehicle.dto';

@Controller('vehicle')
export class VehicleController {
  constructor(private readonly vehicleService: VehicleService) {}

  @Post('detect')
  async detectVehicle(@Body() detectVehicle: DetectVehicleDto) {
    return await this.vehicleService.detectVehicle(detectVehicle);
  }
}
