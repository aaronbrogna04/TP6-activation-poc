/** Topics de negocio (convención dominio.tipo). 3 particiones, retención 7 días, clave = customerId. */
export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export type Topic = (typeof TOPICS)[keyof typeof TOPICS];

export const DLQ_SUFFIX = '.dlq';

/** Nombre de la DLQ de un topic: <topic>.dlq */
export const dlqOf = (topic: Topic): string => `${topic}${DLQ_SUFFIX}`;

/** Consumer groups definidos en el documento de arquitectura. */
export const CONSUMER_GROUPS = {
  ACTIVATION_API: 'activation-api',
  BILLING: 'billing-svc',
  PROVISIONING: 'provisioning-svc',
  NOTIFICATION: 'notification-svc',
  CRM_ANALYTICS: 'crm-analytics',
  /** Escenario 5: group nuevo que lee activation.events desde el inicio. */
  LOYALTY: 'loyalty-svc',
} as const;

/** Headers que acompañan a cada mensaje (además del sobre en el value). */
export const HEADERS = {
  EVENT_TYPE: 'eventType',
  CORRELATION_ID: 'correlationId',
  EVENT_ID: 'eventId',
} as const;

/**
 * Headers que agrega el consumidor al publicar el mensaje original en <topic>.dlq
 * (documento §08, decisión 2: el motivo del error va en los headers; el value queda intacto).
 */
export const DLQ_HEADERS = {
  ERROR: 'dlq-error',
  /** INVALID_MESSAGE (no se pudo parsear / no cumple el contrato) o PROCESSING_ERROR (falló tras los reintentos). */
  REASON: 'dlq-reason',
  SERVICE: 'dlq-service',
  INSTANCE: 'dlq-instance',
  ATTEMPTS: 'dlq-attempts',
  ORIGINAL_TOPIC: 'dlq-original-topic',
  ORIGINAL_PARTITION: 'dlq-original-partition',
  ORIGINAL_OFFSET: 'dlq-original-offset',
  FAILED_AT: 'dlq-failed-at',
} as const;

