import { DynamicModule, Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { SagaCoreService } from './saga-core.service';
import { SagaCoreController } from './saga-core.controller';
import { SagaEntity } from './entities/saga.entity';
import { OutboxEntity } from './entities/outbox.entity';
import { InboxEntity } from './entities/inbox.entity';
import { SAGA_DEFINITIONS, SagaDefinitions } from './types';

@Module({})
export class SagaCoreModule {
  // Registers the generic saga engine for this app, configured with the
  // app's own saga definitions (status -> transition maps, keyed by saga
  // type). Call once from AppModule; SagaCoreService is then injectable
  // anywhere in the app (this module is marked global).
  static forRoot(definitions: SagaDefinitions): DynamicModule {
    return {
      module: SagaCoreModule,
      global: true,
      imports: [TypeOrmModule.forFeature([SagaEntity, OutboxEntity, InboxEntity])],
      controllers: [SagaCoreController],
      providers: [
        SagaCoreService,
        { provide: SAGA_DEFINITIONS, useValue: definitions },
      ],
      exports: [SagaCoreService],
    };
  }
}
