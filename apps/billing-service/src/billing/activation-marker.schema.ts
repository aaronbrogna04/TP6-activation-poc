import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

export interface SeenFailure {
  eventId: string;
  failedStep: string;
  reason: string;
  seenAt: Date;
}

/**
 * Punto de encuentro de los dos caminos del caso borde (documento §06), uno por activación.
 * - ActivationRequested crea la cuenta y escribe accountCreatedAt acá.
 * - ActivationFailed escribe failure acá y anula la cuenta si la ve.
 * Como los dos ESCRIBEN el mismo documento dentro de su transacción, Mongo los serializa (write conflict →
 * se reintenta el segundo con datos frescos) y siempre uno ve al otro. Sin esto, con aislamiento snapshot,
 * dos transacciones concurrentes podían no verse y la cuenta quedaba CREATED (write skew).
 */
@Schema({ collection: 'activation_markers', timestamps: true, versionKey: false })
export class ActivationMarker {
  @Prop({ required: true, unique: true })
  activationId: string;

  @Prop({ required: true })
  customerId: string;

  @Prop()
  accountCreatedAt?: Date;

  @Prop({ type: Object })
  failure?: SeenFailure;
}

export const ActivationMarkerSchema = SchemaFactory.createForClass(ActivationMarker);
