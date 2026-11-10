import { Schema } from 'mongoose';

/** Igual que la retención de los topics: un evento más viejo ya no puede volver a llegar (documento §07). */
export const PROCESSED_EVENTS_TTL_SECONDS = 7 * 24 * 60 * 60;

export interface ProcessedEvent {
  eventId: string;
  eventType: string;
  correlationId: string;
  topic: string;
  partition: number;
  offset: string;
  processedAt: Date;
}

/** processed_events: eventId único + TTL de 7 días. Se inserta en la misma transacción que el efecto. */
export const ProcessedEventSchema = new Schema<ProcessedEvent>(
  {
    eventId: { type: String, required: true },
    eventType: { type: String, required: true },
    correlationId: { type: String, required: true },
    topic: { type: String, required: true },
    partition: { type: Number, required: true },
    offset: { type: String, required: true },
    processedAt: { type: Date, required: true },
  },
  { collection: 'processed_events', versionKey: false },
);
ProcessedEventSchema.index({ eventId: 1 }, { unique: true });
ProcessedEventSchema.index({ processedAt: 1 }, { expireAfterSeconds: PROCESSED_EVENTS_TTL_SECONDS });
