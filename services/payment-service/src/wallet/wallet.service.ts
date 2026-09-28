import { BadRequestException, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { WalletEntity } from './entities/wallet.entity';

@Injectable()
export class WalletService {
  constructor(
    @InjectRepository(WalletEntity)
    private walletRepo: Repository<WalletEntity>,
  ) {}

  async topUp(userId: string, amount: number): Promise<WalletEntity> {
    if (amount <= 0) {
      throw new BadRequestException('Top-up amount must be positive');
    }

    let wallet = await this.walletRepo.findOne({ where: { userId } });
    if (!wallet) {
      wallet = this.walletRepo.create({ userId, balance: 0 });
    }

    wallet.balance = Number(wallet.balance) + amount;
    return this.walletRepo.save(wallet);
  }
}
