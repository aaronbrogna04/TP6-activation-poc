import { io, Socket } from 'socket.io-client';

/** URL pública de activation-api (REST y WebSocket). La usa el navegador, no el contenedor. */
export const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:3000';

export type SimulateFailure = 'none' | 'billing' | 'provisioning';
export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

export interface StepResult {
  status: 'OK' | 'FAILED';
  at: string;
  eventType: string;
  reason?: string;
}

export interface HistoryEntry {
  eventId: string;
  eventType: string;
  source: string;
  at: string;
  detail?: string;
  ignored?: boolean;
}

/** Mismo formato que GET /activations/:id (RF-09). */
export interface Activation {
  activationId: string;
  customerId: string;
  planId: string;
  status: ActivationStatus;
  simulateFailure: SimulateFailure;
  steps: { billing?: StepResult; provisioning?: StepResult };
  history: HistoryEntry[];
  createdAt: string;
  updatedAt: string;
}

/** Mensaje `activation.updated` del @WebSocketGateway de activation-api. */
export interface ActivationUpdated {
  correlationId: string;
  event: HistoryEntry;
  activation: Activation;
}

export const WS_ACTIVATION_UPDATED = 'activation.updated';

export function connectSocket(): Socket {
  return io(API_URL, { transports: ['websocket', 'polling'] });
}

async function json<T>(res: Response): Promise<T> {
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body?.message ? String(body.message) : `HTTP ${res.status}`);
  return body as T;
}

export async function createActivation(input: {
  customerId: string;
  planId: string;
  simulateFailure: SimulateFailure;
}): Promise<{ httpStatus: number; activationId: string; status: ActivationStatus }> {
  const res = await fetch(`${API_URL}/activations`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  });
  const body = await json<{ activationId: string; status: ActivationStatus }>(res);
  return { httpStatus: res.status, ...body };
}

export const fetchRecent = (limit = 20) => fetch(`${API_URL}/activations?limit=${limit}`).then((r) => json<Activation[]>(r));

export const fetchActivation = (id: string) => fetch(`${API_URL}/activations/${id}`).then((r) => json<Activation>(r));
