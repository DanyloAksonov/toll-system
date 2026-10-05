import { Controller } from '@nestjs/common';
import { SagaCoreService } from './saga-core.service';

@Controller('saga-core')
export class SagaCoreController {
  constructor(private readonly sagaCoreService: SagaCoreService) {}
}
