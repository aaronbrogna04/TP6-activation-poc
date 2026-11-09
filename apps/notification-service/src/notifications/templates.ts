import { ActivationEvent } from '@poc/contracts';
import { Mail } from './mailer';

/** No hay email en el contrato: se deriva una dirección de demo a partir del customerId. */
export const recipientOf = (customerId: string): string =>
  `${customerId.toLowerCase().replace(/[^a-z0-9._-]+/g, '-')}@clientes.example.com`;

const esc = (s: string) => s.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);

export function mailFor(event: ActivationEvent): Mail {
  const to = recipientOf(event.customerId);
  const id = event.correlationId;
  if (event.eventType === 'ActivationCompleted') {
    const plan = event.payload.planId;
    return {
      to,
      subject: `¡Bienvenido! Tu plan ${plan} ya está activo`,
      text: `Hola ${event.customerId}:\n\nTu plan ${plan} quedó activo.\n\nActivación: ${id}\n`,
      html: `<p>Hola <b>${esc(event.customerId)}</b>:</p><p>Tu plan <b>${esc(plan)}</b> quedó activo. ¡Bienvenido!</p><p style="color:#666">Activación: ${esc(id)}</p>`,
    };
  }
  const { planId, failedStep, reason } = event.payload;
  return {
    to,
    subject: `No pudimos activar tu plan ${planId}`,
    text: `Hola ${event.customerId}:\n\nNo pudimos activar tu plan ${planId}. No se te va a cobrar nada.\nMotivo: ${reason} (paso: ${failedStep})\n\nActivación: ${id}\n`,
    html: `<p>Hola <b>${esc(event.customerId)}</b>:</p><p>No pudimos activar tu plan <b>${esc(planId)}</b>. No se te va a cobrar nada.</p><p>Motivo: ${esc(reason)} (paso: ${esc(failedStep)})</p><p style="color:#666">Activación: ${esc(id)}</p>`,
  };
}
