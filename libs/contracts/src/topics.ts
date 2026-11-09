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
} as const;

/** Headers que acompañan a cada mensaje (además del sobre en el value). */
export const HEADERS = {
  EVENT_TYPE: 'eventType',
  CORRELATION_ID: 'correlationId',
  EVENT_ID: 'eventId',
} as const;
