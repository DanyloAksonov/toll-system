import { Controller, Post, Body } from '@nestjs/common';
import { WalletService } from './wallet.service';
import { TopUpDto } from './dto/input/top-up.dto';

@Controller('wallet')
export class WalletController {
  constructor(private readonly walletService: WalletService) {}

  @Post('top-up')
  async topUp(@Body() topUpDto: TopUpDto) {
    return await this.walletService.topUp(topUpDto.userId, topUpDto.amount);
  }
}
