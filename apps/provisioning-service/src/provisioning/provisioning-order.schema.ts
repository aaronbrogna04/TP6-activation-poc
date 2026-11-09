import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/** Orden de aprovisionamiento simulada: una por activación (activationId único). */
@Schema({ collection: 'provisioning_orders', timestamps: true, versionKey: false })
export class ProvisioningOrder {
  @Prop({ required: true, unique: true })
  activationId: string;

  @Prop({ required: true, unique: true })
  orderId: string;

  @Prop({ required: true })
  customerId: string;

  @Prop({ required: true })
  planId: string;

  @Prop({ required: true, enum: ['COMPLETED', 'FAILED'] })
  status: 'COMPLETED' | 'FAILED';

  @Prop()
  reason?: string;

  @Prop({ required: true })
  delayMs: number;
}

export const ProvisioningOrderSchema = SchemaFactory.createForClass(ProvisioningOrder);
