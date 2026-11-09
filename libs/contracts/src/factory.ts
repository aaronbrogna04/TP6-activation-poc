import { randomUUID } from 'node:crypto';
import { EventOf, EventType, EVENT_VERSIONS, PayloadByType } from './events';

export interface CreateEventInput<T extends EventType> {
  eventType: T;
  correlationId: string;
  customerId: string;
  source: string;
  payload: PayloadByType[T];
}

/** Arma el sobre común: completa eventId, version y occurredAt. */
export function createEvent<T extends EventType>(input: CreateEventInput<T>): EventOf<T> {
  return {
    eventId: randomUUID(),
    eventType: input.eventType,
    version: EVENT_VERSIONS[input.eventType],
    occurredAt: new Date().toISOString(),
    correlationId: input.correlationId,
    customerId: input.customerId,
    source: input.source,
    payload: input.payload,
  };
}
