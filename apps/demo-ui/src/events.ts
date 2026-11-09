import { Activation, HistoryEntry } from './api';

/** Color de cada evento en la timeline: verde = éxito, rojo = fallo, gris = compensación. */
export type EventKind = 'success' | 'failure' | 'compensation';

const FAILURES = new Set(['BillingFailed', 'ProvisioningFailed', 'ActivationFailed']);
const COMPENSATIONS = new Set(['BillingAccountCancelled']);

export function kindOf(entry: HistoryEntry): EventKind {
  if (FAILURES.has(entry.eventType)) return 'failure';
  if (COMPENSATIONS.has(entry.eventType)) return 'compensation';
  return 'success';
}

export const isOpen = (a: Activation) => a.status === 'PENDING' || a.status === 'IN_PROGRESS';

/**
 * Las fotos de una activación pueden llegar desordenadas (respuesta REST vs. mensajes WS).
 * El historial solo crece: se queda la foto con más entradas.
 */
export function newest(current: Activation | undefined, incoming: Activation): Activation {
  if (!current) return incoming;
  if (incoming.history.length !== current.history.length) {
    return incoming.history.length > current.history.length ? incoming : current;
  }
  return incoming.updatedAt >= current.updatedAt ? incoming : current;
}

export const STATUS_LABEL: Record<Activation['status'], string> = {
  PENDING: 'Pendiente',
  IN_PROGRESS: 'En curso',
  ACTIVE: 'Activa',
  FAILED: 'Fallida',
};

export const SOURCE_LABEL: Record<string, string> = {
  'activation-api': 'activation-api',
  'billing-service': 'billing',
  'provisioning-service': 'provisioning',
};

export function clock(iso: string): string {
  const d = new Date(iso);
  const pad = (n: number, w = 2) => String(n).padStart(w, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

export function elapsed(fromIso: string, toIso: string): string {
  const ms = new Date(toIso).getTime() - new Date(fromIso).getTime();
  return ms < 1000 ? `+${ms} ms` : `+${(ms / 1000).toFixed(2)} s`;
}
