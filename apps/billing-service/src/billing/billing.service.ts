import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { randomUUID } from 'node:crypto';
import { ActivationFailed, ActivationRequested, createEvent, SOURCES, TOPICS } from '@poc/contracts';
import { Tx } from '@poc/kafka-toolkit';
import { ActivationMarker } from './activation-marker.schema';
import { BillingAccount } from './billing-account.schema';

/**
 * Todo corre dentro de la transacción de EventConsumer (processed_events + efecto).
 * Los eventos se encolan con tx.publish y salen recién después del commit.
 */
@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);

  constructor(
    @InjectModel(BillingAccount.name) private readonly accounts: Model<BillingAccount>,
    @InjectModel(ActivationMarker.name) private readonly markers: Model<ActivationMarker>,
  ) {}

  /** RF-04: crea la cuenta simulada y publica el resultado en billing.events. */
  async onActivationRequested(event: ActivationRequested, tx: Tx): Promise<void> {
    const { session } = tx;
    const activationId = event.correlationId;
    const base = { correlationId: activationId, customerId: event.customerId, source: SOURCES.BILLING };

    // Fallo de negocio simulado: es un evento (BillingFailed), no un error técnico → no va a la DLQ.
    if (event.payload.simulateFailure === 'billing') {
      this.logger.warn(`[${activationId}] ${event.eventType}: fallo simulado, no se crea la cuenta`);
      tx.publish(
        TOPICS.BILLING_EVENTS,
        createEvent({ ...base, eventType: 'BillingFailed', payload: { reason: 'Fallo simulado en billing (simulateFailure=billing)' } }),
      );
      return;
    }

    // Upsert por activationId (índice único): ni con otro eventId para la misma activación se duplica la cuenta.
    const account = (await this.accounts
      .findOneAndUpdate(
        { activationId },
        {
          $setOnInsert: {
            activationId,
            accountId: `acc-${randomUUID().slice(0, 8)}`,
            customerId: event.customerId,
            planId: event.payload.planId,
            status: 'CREATED',
          },
        },
        { upsert: true, new: true, session },
      )
      .lean())!;
    this.logger.log(`[${activationId}] ${event.eventType}: cuenta ${account.accountId} ${account.status}`);
    tx.publish(
      TOPICS.BILLING_EVENTS,
      createEvent({ ...base, eventType: 'BillingAccountCreated', payload: { accountId: account.accountId, planId: account.planId } }),
    );

    // Caso borde (documento §06): si ActivationFailed llegó antes de crear la cuenta, se anula ahora.
    const marker = await this.touchMarker(activationId, event.customerId, { accountCreatedAt: new Date() }, tx);
    if (marker.failure) {
      this.logger.warn(`[${activationId}] ${event.eventType}: ActivationFailed ya visto; se anula la cuenta recién creada`);
      await this.cancelAccount(activationId, event.customerId, `ActivationFailed (${marker.failure.failedStep}): ${marker.failure.reason}`, tx);
    }
  }

  /** Compensación (RF-06): recuerda el fallo y anula la cuenta si existe. */
  async onActivationFailed(event: ActivationFailed, tx: Tx): Promise<void> {
    const { failedStep, reason } = event.payload;
    await this.touchMarker(
      event.correlationId,
      event.customerId,
      { failure: { eventId: event.eventId, failedStep, reason, seenAt: new Date() } },
      tx,
    );
    const cancelled = await this.cancelAccount(event.correlationId, event.customerId, `ActivationFailed (${failedStep}): ${reason}`, tx);
    if (!cancelled) {
      this.logger.log(
        `[${event.correlationId}] ${event.eventType}: no hay cuenta CREATED que anular (todavía no se creó, billing falló o ya estaba anulada); queda registrado`,
      );
    }
  }

  private async touchMarker(activationId: string, customerId: string, set: Partial<ActivationMarker>, tx: Tx) {
    return (await this.markers
      .findOneAndUpdate(
        { activationId },
        { $set: set, $setOnInsert: { activationId, customerId } },
        { upsert: true, new: true, session: tx.session },
      )
      .lean())!;
  }

  /** CREATED → CANCELLED (condicional) y encola BillingAccountCancelled. false si no había nada que anular. */
  private async cancelAccount(activationId: string, customerId: string, reason: string, tx: Tx): Promise<boolean> {
    const account = await this.accounts
      .findOneAndUpdate(
        { activationId, status: 'CREATED' },
        { $set: { status: 'CANCELLED', cancelledReason: reason } },
        { new: true, session: tx.session },
      )
      .lean();
    if (!account) return false;

    this.logger.warn(`[${activationId}] BillingAccountCancelled: cuenta ${account.accountId} CANCELLED (${reason})`);
    tx.publish(
      TOPICS.BILLING_EVENTS,
      createEvent({
        eventType: 'BillingAccountCancelled',
        correlationId: activationId,
        customerId,
        source: SOURCES.BILLING,
        payload: { accountId: account.accountId, reason },
      }),
    );
    return true;
  }
}
