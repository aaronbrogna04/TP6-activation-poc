import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/**
 * Memoria de los ActivationFailed que billing ya vio (caso borde del documento §06):
 * si ActivationFailed llega antes de que billing cree la cuenta, la cuenta se anula apenas se crea.
 */
@Schema({ collection: 'activation_failures', timestamps: { createdAt: true, updatedAt: false }, versionKey: false })
export class ActivationFailure {
  @Prop({ required: true, unique: true })
  activationId: string;

  @Prop({ required: true })
  customerId: string;

  @Prop({ required: true })
  eventId: string;

  @Prop({ required: true })
  failedStep: string;

  @Prop({ required: true })
  reason: string;
}

export const ActivationFailureSchema = SchemaFactory.createForClass(ActivationFailure);
