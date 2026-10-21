import {
  Column,
  CreateDateColumn,
  Entity,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn,
  RelationId,
} from 'typeorm';
import { SagaEntity } from './saga.entity';

@Entity('outbox')
export class OutboxEntity {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @ManyToOne(() => SagaEntity, { nullable: false })
  @JoinColumn({ name: 'saga_id' })
  saga: SagaEntity;

  @RelationId((outbox: OutboxEntity) => outbox.saga)
  sagaId: string;

  @Column({ type: 'text' })
  exchange: string; // 'toll.commands'

  @Column({ name: 'routing_key', type: 'text' })
  routingKey: string; // 'vehicle.commands'

  @Column({ name: 'message_type', type: 'text' })
  messageType: string; // 'RegisterVehicle'

  @Column({ type: 'jsonb' })
  payload: Record<string, unknown>;

  @Column({ type: 'int', default: 0 })
  attempts: number;

  @Column({ name: 'last_error', type: 'text', nullable: true })
  lastError: string | null;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @Column({ name: 'published_at', type: 'timestamptz', nullable: true })
  publishedAt: Date | null;
}
