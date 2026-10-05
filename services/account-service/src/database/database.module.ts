import { Module } from '@nestjs/common';
import { databaseConnections } from './databaseConnections';

@Module({
  imports: [...databaseConnections],
  exports: [...databaseConnections],
})
export class DatabaseModule {}
