import {
  Entity,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  Column,
} from 'typeorm';

@Entity()
export class OutboxEntity {
  @PrimaryGeneratedColumn('uuid')
  uuid: string;

  @CreateDateColumn()
  createdAt: string;

  @Column()
  eventType: string;

  @Column('jsonb')
  payload: string;

  @Column()
  status: string;
}
