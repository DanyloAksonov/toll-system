import { Column, CreateDateColumn, Entity, PrimaryColumn } from 'typeorm';

@Entity('inbox')
export class InboxEntity {
  @PrimaryColumn({ name: 'message_id', type: 'uuid' })
  messageId: string;

  @PrimaryColumn({ type: 'text' })
  consumer: string; // 'orchestrator.replies'

  @Column({ name: 'message_type', type: 'text' })
  messageType: string;

  @CreateDateColumn({ name: 'processed_at', type: 'timestamptz' })
  processedAt: Date;
}
