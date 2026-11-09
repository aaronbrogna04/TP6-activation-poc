import { BillingEvent, DomainEvent, ProvisioningEvent } from '@poc/contracts';
import { HistoryEntry, StepResult } from './activation.schema';
import { StepName } from './saga';

export type ResultEvent = BillingEvent | ProvisioningEvent;

/** Traduce un evento de resultado al paso de la saga que representa (o null si no afecta la saga). */
export function toStep(event: ResultEvent): { step: StepName; result: StepResult } | null {
  const at = event.occurredAt;
  switch (event.eventType) {
    case 'BillingAccountCreated':
      return { step: 'billing', result: { status: 'OK', at, eventType: event.eventType } };
    case 'BillingFailed':
      return { step: 'billing', result: { status: 'FAILED', at, eventType: event.eventType, reason: event.payload.reason } };
    case 'ProvisioningCompleted':
      return { step: 'provisioning', result: { status: 'OK', at, eventType: event.eventType } };
    case 'ProvisioningFailed':
      return {
        step: 'provisioning',
        result: { status: 'FAILED', at, eventType: event.eventType, reason: event.payload.reason },
      };
    default:
      // BillingAccountCancelled (compensación): solo se registra en el historial.
      return null;
  }
}

/** Resumen legible del payload, para el historial (RF-09) y la timeline de la UI (RF-08). */
export function detailOf(event: DomainEvent): string | undefined {
  switch (event.eventType) {
    case 'ActivationRequested':
      return `plan ${event.payload.planId} · canal ${event.payload.channel}` +
        (event.payload.simulateFailure !== 'none' ? ` · fallo forzado en ${event.payload.simulateFailure}` : '');
    case 'BillingAccountCreated':
      return `cuenta ${event.payload.accountId}`;
    case 'BillingAccountCancelled':
      return `cuenta ${event.payload.accountId} anulada: ${event.payload.reason}`;
    case 'ProvisioningCompleted':
      return `orden ${event.payload.orderId}`;
    case 'BillingFailed':
    case 'ProvisioningFailed':
      return event.payload.reason;
    case 'ActivationCompleted':
      return `plan ${event.payload.planId} activo`;
    case 'ActivationFailed':
      return `falló ${event.payload.failedStep}: ${event.payload.reason}`;
    default:
      return undefined;
  }
}

export function historyOf(event: DomainEvent, ignored?: boolean): HistoryEntry {
  const entry: HistoryEntry = {
    eventId: event.eventId,
    eventType: event.eventType,
    source: event.source,
    at: event.occurredAt,
  };
  const detail = detailOf(event);
  if (detail) entry.detail = detail;
  if (ignored) entry.ignored = true;
  return entry;
}
