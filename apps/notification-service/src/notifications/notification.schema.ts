import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/** Email enviado. A lo sumo uno por (activationId, eventType). */
@Schema({ collection: 'notifications', timestamps: true, versionKey: false })
export class Notification {
  @Prop({ required: true })
  activationId: string;

  @Prop({ required: true })
  eventType: string;

  @Prop({ required: true })
  eventId: string;

  @Prop({ required: true })
  customerId: string;

  @Prop({ required: true })
  to: string;

  @Prop({ required: true })
  subject: string;

  @Prop({ required: true, enum: ['SENT'] })
  status: 'SENT';

  @Prop()
  messageId?: string;

  @Prop({ required: true })
  sentAt: Date;
}

export const NotificationSchema = SchemaFactory.createForClass(Notification);
NotificationSchema.index({ activationId: 1, eventType: 1 }, { unique: true });
