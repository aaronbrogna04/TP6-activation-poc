import { SIMULATE_FAILURE_VALUES } from './domain';
import { DomainEvent, EventType, EVENT_VERSIONS, PayloadByType } from './events';
import { TOPICS } from './topics';

/** Qué eventos puede traer cada topic (documento §05). Un eventType fuera de esta lista no cumple el contrato. */
export const EVENTS_BY_TOPIC = {
  [TOPICS.ACTIVATION_REQUESTED]: ['ActivationRequested'],
  [TOPICS.BILLING_EVENTS]: ['BillingAccountCreated', 'BillingFailed', 'BillingAccountCancelled'],
  [TOPICS.PROVISIONING_EVENTS]: ['ProvisioningCompleted', 'ProvisioningFailed'],
  [TOPICS.ACTIVATION_EVENTS]: ['ActivationCompleted', 'ActivationFailed'],
} as const satisfies Record<string, readonly EventType[]>;

/** 'string' = string no vacío; un array = uno de esos valores. Campos extra se aceptan (cambio compatible). */
type FieldRule = 'string' | readonly string[];

const PAYLOAD_RULES: { [K in EventType]: { [F in keyof PayloadByType[K]]-?: FieldRule } } = {
  ActivationRequested: { planId: 'string', channel: 'string', simulateFailure: SIMULATE_FAILURE_VALUES },
  BillingAccountCreated: { accountId: 'string', planId: 'string' },
  BillingFailed: { reason: 'string' },
  BillingAccountCancelled: { accountId: 'string', reason: 'string' },
  ProvisioningCompleted: { orderId: 'string', planId: 'string' },
  ProvisioningFailed: { reason: 'string' },
  ActivationCompleted: { planId: 'string' },
  ActivationFailed: { planId: 'string', failedStep: ['billing', 'provisioning'], reason: 'string' },
};

const ENVELOPE_STRINGS = ['eventId', 'eventType', 'occurredAt', 'correlationId', 'customerId', 'source'] as const;

export type ValidationResult = { ok: true; event: DomainEvent } | { ok: false; reason: string };

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);
const isNonEmptyString = (v: unknown): v is string => typeof v === 'string' && v.trim().length > 0;

function checkField(obj: Record<string, unknown>, field: string, rule: FieldRule, where: string): string | null {
  const v = obj[field];
  if (rule === 'string') return isNonEmptyString(v) ? null : `${where}.${field} debe ser un string no vacío`;
  return typeof v === 'string' && rule.includes(v) ? null : `${where}.${field} debe ser uno de ${rule.join('|')}`;
}

/**
 * Valida que un valor ya parseado cumpla el contrato del topic: sobre común, eventType permitido en el topic,
 * versión soportada y campos obligatorios del payload. No valida la clave del mensaje.
 */
export function validateEvent(topic: string, value: unknown): ValidationResult {
  if (!isObject(value)) return { ok: false, reason: 'el mensaje no es un objeto JSON' };
  for (const f of ENVELOPE_STRINGS) {
    if (!isNonEmptyString(value[f])) return { ok: false, reason: `sobre: falta ${f} (string no vacío)` };
  }
  const allowed = (EVENTS_BY_TOPIC as Record<string, readonly string[]>)[topic];
  if (!allowed) return { ok: false, reason: `topic desconocido: ${topic}` };
  const type = value.eventType as string;
  if (!allowed.includes(type)) return { ok: false, reason: `eventType ${type} no pertenece al topic ${topic}` };
  const expected = EVENT_VERSIONS[type as EventType];
  if (value.version !== expected) {
    return { ok: false, reason: `${type}: versión ${String(value.version)} no soportada (se espera ${expected})` };
  }
  if (Number.isNaN(Date.parse(value.occurredAt as string))) return { ok: false, reason: 'sobre: occurredAt no es una fecha ISO-8601' };
  if (!isObject(value.payload)) return { ok: false, reason: `${type}: falta payload` };
  const rules = PAYLOAD_RULES[type as EventType] as Record<string, FieldRule>;
  for (const [field, rule] of Object.entries(rules)) {
    const err = checkField(value.payload, field, rule, 'payload');
    if (err) return { ok: false, reason: `${type}: ${err}` };
  }
  return { ok: true, event: value as unknown as DomainEvent };
}
