import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

export type BillingAccountStatus = 'CREATED' | 'CANCELLED';

/** Cuenta de facturación simulada: una por activación (activationId único). */
@Schema({ collection: 'billing_accounts', timestamps: true, versionKey: false })
export class BillingAccount {
  @Prop({ required: true, unique: true })
  activationId: string;

  @Prop({ required: true, unique: true })
  accountId: string;

  @Prop({ required: true })
  customerId: string;

  @Prop({ required: true })
  planId: string;

  @Prop({ required: true, enum: ['CREATED', 'CANCELLED'], default: 'CREATED' })
  status: BillingAccountStatus;

  @Prop()
  cancelledReason?: string;
}

export const BillingAccountSchema = SchemaFactory.createForClass(BillingAccount);
