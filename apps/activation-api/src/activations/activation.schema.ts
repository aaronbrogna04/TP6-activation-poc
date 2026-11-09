import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';
import { ACTIVATION_STATUS, ActivationStatus, SimulateFailure, StepStatus } from '@poc/contracts';

export interface StepResult {
  status: StepStatus;
  at: string;
  eventType: string;
  reason?: string;
}

export interface Steps {
  billing?: StepResult;
  provisioning?: StepResult;
}

export interface HistoryEntry {
  eventId: string;
  eventType: string;
  source: string;
  at: string;
  /** Resumen del payload (cuenta, orden, motivo del fallo...). */
  detail?: string;
  /** true si llegó cuando la activación ya era final: se registra pero no cambia el estado. */
  ignored?: boolean;
}

@Schema({ collection: 'activations', timestamps: true, versionKey: false, minimize: false })
export class Activation {
  /** _id = activationId */
  @Prop({ type: String, required: true })
  _id: string;

  @Prop({ required: true, index: true })
  customerId: string;

  @Prop({ required: true })
  planId: string;

  @Prop({ required: true, enum: Object.values(ACTIVATION_STATUS), default: ACTIVATION_STATUS.PENDING })
  status: ActivationStatus;

  @Prop({ required: true, default: 'none' })
  simulateFailure: SimulateFailure;

  @Prop({ type: Object, default: {} })
  steps: Steps;

  @Prop({ type: [Object], default: [] })
  history: HistoryEntry[];

  createdAt?: Date;
  updatedAt?: Date;
}

export type ActivationDocument = HydratedDocument<Activation>;
export const ActivationSchema = SchemaFactory.createForClass(Activation);
