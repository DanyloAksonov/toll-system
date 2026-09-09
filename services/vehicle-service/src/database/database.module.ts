import { Module } from '@nestjs/common';
import { databaseConnections } from './databaseConnectios';

@Module({
  imports: [...databaseConnections],
  exports: [...databaseConnections],
})
export class DatabaseModule {}
