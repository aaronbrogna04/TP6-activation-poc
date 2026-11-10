import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/** Puntos por cliente (base propia de loyalty; ningún otro servicio la lee). */
@Schema({ collection: 'loyalty_accounts', timestamps: true, versionKey: false })
export class LoyaltyAccount {
  @Prop({ required: true, unique: true })
  customerId: string;

  @Prop({ required: true, default: 0 })
  points: number;

  @Prop({ required: true, default: 0 })
  completedActivations: number;

  @Prop({ required: true, default: 0 })
  failedActivations: number;

  @Prop({ type: [String], default: [] })
  plans: string[];
}

export const LoyaltyAccountSchema = SchemaFactory.createForClass(LoyaltyAccount);
