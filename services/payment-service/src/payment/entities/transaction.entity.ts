import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
} from 'typeorm';

@Entity('transaction')
export class TransactionEntity {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column()
  plateNumber: string;

  @Column()
  gateId: string;

  @Column()
  eventId: string;

  @Column({ type: 'decimal', default: 5.0 })
  amount: number;

  @Column({ default: 'completed' })
  status: string;

  @CreateDateColumn()
  createdAt: Date;
}
