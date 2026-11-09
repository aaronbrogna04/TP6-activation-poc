import { createEvent } from '@poc/contracts';
import { historyOf, toStep } from './history';

const base = { correlationId: 'act-1', customerId: 'C-1' };

describe('historial de la activación (RF-09)', () => {
  it('BillingAccountCancelled es compensación: no afecta la saga y tiene detalle', () => {
    const e = createEvent({
      ...base,
      eventType: 'BillingAccountCancelled',
      source: 'billing-service',
      payload: { accountId: 'acc-1', reason: 'ActivationFailed' },
    });
    expect(toStep(e)).toBeNull();
    expect(historyOf(e)).toMatchObject({
      eventId: e.eventId,
      eventType: 'BillingAccountCancelled',
      source: 'billing-service',
      at: e.occurredAt,
      detail: 'cuenta acc-1 anulada: ActivationFailed',
    });
    expect(historyOf(e).ignored).toBeUndefined();
  });

  it('un resultado tardío se marca ignored', () => {
    const e = createEvent({
      ...base,
      eventType: 'ProvisioningCompleted',
      source: 'provisioning-service',
      payload: { orderId: 'ord-1', planId: 'P' },
    });
    expect(toStep(e)).toMatchObject({ step: 'provisioning', result: { status: 'OK' } });
    expect(historyOf(e, true)).toMatchObject({ ignored: true, detail: 'orden ord-1' });
  });

  it('los fallos llevan el motivo', () => {
    const e = createEvent({ ...base, eventType: 'ProvisioningFailed', source: 'provisioning-service', payload: { reason: 'sin cobertura' } });
    expect(toStep(e)).toMatchObject({ step: 'provisioning', result: { status: 'FAILED', reason: 'sin cobertura' } });
    expect(historyOf(e).detail).toBe('sin cobertura');
  });
});
