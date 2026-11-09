import { EventEnvelope } from './envelope';
import { SimulateFailure } from './domain';

/** Versión vigente de cada contrato de evento. */
export const EVENT_VERSIONS = {
  ActivationRequested: 1,
  BillingAccountCreated: 1,
  BillingFailed: 1,
  BillingAccountCancelled: 1,
  ProvisioningCompleted: 1,
  ProvisioningFailed: 1,
  ActivationCompleted: 1,
  ActivationFailed: 1,
} as const;

export type EventType = keyof typeof EVENT_VERSIONS;

// ---- Payloads ------------------------------------------------------------

/** activation.requested */
export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  simulateFailure: SimulateFailure;
}

/** billing.events */
export interface BillingAccountCreatedPayload {
  accountId: string;
  planId: string;
}
export interface BillingFailedPayload {
  reason: string;
}
export interface BillingAccountCancelledPayload {
  accountId: string;
  reason: string;
}

/** provisioning.events */
export interface ProvisioningCompletedPayload {
  orderId: string;
  planId: string;
}
export interface ProvisioningFailedPayload {
  reason: string;
}

/** activation.events */
export interface ActivationCompletedPayload {
  planId: string;
}
export interface ActivationFailedPayload {
  planId: string;
  /** Paso que falló primero. */
  failedStep: 'billing' | 'provisioning';
  reason: string;
}

export interface PayloadByType {
  ActivationRequested: ActivationRequestedPayload;
  BillingAccountCreated: BillingAccountCreatedPayload;
  BillingFailed: BillingFailedPayload;
  BillingAccountCancelled: BillingAccountCancelledPayload;
  ProvisioningCompleted: ProvisioningCompletedPayload;
  ProvisioningFailed: ProvisioningFailedPayload;
  ActivationCompleted: ActivationCompletedPayload;
  ActivationFailed: ActivationFailedPayload;
}

export type EventOf<T extends EventType> = EventEnvelope<T, PayloadByType[T]>;

export type ActivationRequested = EventOf<'ActivationRequested'>;
export type BillingAccountCreated = EventOf<'BillingAccountCreated'>;
export type BillingFailed = EventOf<'BillingFailed'>;
export type BillingAccountCancelled = EventOf<'BillingAccountCancelled'>;
export type ProvisioningCompleted = EventOf<'ProvisioningCompleted'>;
export type ProvisioningFailed = EventOf<'ProvisioningFailed'>;
export type ActivationCompleted = EventOf<'ActivationCompleted'>;
export type ActivationFailed = EventOf<'ActivationFailed'>;

export type BillingEvent = BillingAccountCreated | BillingFailed | BillingAccountCancelled;
export type ProvisioningEvent = ProvisioningCompleted | ProvisioningFailed;
export type ActivationEvent = ActivationCompleted | ActivationFailed;

/** Unión discriminada por eventType. */
export type DomainEvent = ActivationRequested | BillingEvent | ProvisioningEvent | ActivationEvent;
