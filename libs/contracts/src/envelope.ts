/** Sobre común a todos los eventos. El payload cambia según el tipo de evento. */
export interface EventEnvelope<TType extends string = string, TPayload = unknown> {
  /** Identificador único; base de la idempotencia. */
  eventId: string;
  /** Nombre del evento, en pasado. */
  eventType: TType;
  /** Versión del contrato de este eventType. Cambios incompatibles suben version. */
  version: number;
  /** ISO-8601 UTC. */
  occurredAt: string;
  /** El activationId: agrupa todos los eventos de una activación. */
  correlationId: string;
  /** También es la clave del mensaje en Kafka (orden por cliente). */
  customerId: string;
  /** Servicio que publicó el evento. */
  source: string;
  payload: TPayload;
}
