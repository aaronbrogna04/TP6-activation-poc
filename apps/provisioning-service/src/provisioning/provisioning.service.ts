import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { ActivationRequested, createEvent, SOURCES, TOPICS } from '@poc/contracts';
import { Tx } from '@poc/kafka-toolkit';
import { ProvisioningOrder } from './provisioning-order.schema';

const MIN_DELAY_MS = 1000;
const MAX_DELAY_MS = 3000;

@Injectable()
export class ProvisioningService {
  private readonly logger = new Logger(ProvisioningService.name);

  constructor(
    @InjectModel(ProvisioningOrder.name) private readonly orders: Model<ProvisioningOrder>,
  ) {}

  /**
   * RF-04: simula el aprovisionamiento (retardo de 1 a 3 s) y publica en provisioning.events.
   * Corre dentro de la transacción de EventConsumer; el evento sale después del commit (tx.publish).
   */
  async onActivationRequested(event: ActivationRequested, tx: Tx): Promise<void> {
    const delayMs = MIN_DELAY_MS + Math.floor(Math.random() * (MAX_DELAY_MS - MIN_DELAY_MS + 1));
    this.logger.log(`[${event.correlationId}] aprovisionando ${event.payload.planId} (${delayMs} ms)`);
    await sleep(delayMs);

    // Fallo de negocio simulado: se publica ProvisioningFailed, no es un error técnico → no va a la DLQ.
    const fail = event.payload.simulateFailure === 'provisioning';
    const reason = fail ? 'Fallo simulado en provisioning (simulateFailure=provisioning)' : undefined;

    // Upsert por activationId (índice único): ni con otro eventId para la misma activación se duplica la orden.
    const order = await this.orders
      .findOneAndUpdate(
        { activationId: event.correlationId },
        {
          $setOnInsert: {
            activationId: event.correlationId,
            orderId: `ord-${randomUUID().slice(0, 8)}`,
            customerId: event.customerId,
            planId: event.payload.planId,
            status: fail ? 'FAILED' : 'COMPLETED',
            reason,
            delayMs,
          },
        },
        { upsert: true, new: true, session: tx.session },
      )
      .lean();

    const base = { correlationId: event.correlationId, customerId: event.customerId, source: SOURCES.PROVISIONING };
    if (order!.status === 'FAILED') {
      this.logger.warn(`[${event.correlationId}] orden ${order!.orderId} FAILED (fallo simulado)`);
      tx.publish(
        TOPICS.PROVISIONING_EVENTS,
        createEvent({ ...base, eventType: 'ProvisioningFailed', payload: { reason: order!.reason ?? 'falló' } }),
      );
      return;
    }

    this.logger.log(`[${event.correlationId}] orden ${order!.orderId} COMPLETED`);
    tx.publish(
      TOPICS.PROVISIONING_EVENTS,
      createEvent({ ...base, eventType: 'ProvisioningCompleted', payload: { orderId: order!.orderId, planId: order!.planId } }),
    );
  }
}
