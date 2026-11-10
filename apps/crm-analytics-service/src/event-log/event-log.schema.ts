import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/** Copia de cada evento recibido (documento §07): eventId único, índice por correlationId. */
@Schema({ collection: 'event_log', versionKey: false })
export class EventLogEntry {
  @Prop({ required: true, unique: true })
  eventId: string;

  @Prop({ required: true })
  eventType: string;

  @Prop({ required: true })
  version: number;

  @Prop({ required: true, index: true })
  correlationId: string;

  @Prop({ required: true })
  customerId: string;

  @Prop({ required: true })
  source: string;

  @Prop({ required: true })
  occurredAt: Date;

  @Prop({ type: Object, required: true })
  payload: Record<string, unknown>;

  @Prop({ required: true })
  topic: string;

  @Prop({ required: true })
  partition: number;

  @Prop({ required: true })
  offset: string;

  @Prop({ required: true })
  receivedAt: Date;
}

export const EventLogSchema = SchemaFactory.createForClass(EventLogEntry);
