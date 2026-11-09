import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { randomUUID } from 'node:crypto';
import { ActivationFailed, ActivationRequested, createEvent, SOURCES, TOPICS } from '@poc/contracts';
import { EventPublisher } from '@poc/kafka-toolkit';
import { ActivationFailure } from './activation-failure.schema';
import { BillingAccount } from './billing-account.schema';

@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);

  constructor(
    @InjectModel(BillingAccount.name) private readonly accounts: Model<BillingAccount>,
    @InjectModel(ActivationFailure.name) private readonly failures: Model<ActivationFailure>,
    private readonly publisher: EventPublisher,
  ) {}

  /** RF-04: crea la cuenta simulada y publica el resultado en billing.events. */
  async onActivationRequested(event: ActivationRequested): Promise<void> {
    const base = { correlationId: event.correlationId, customerId: event.customerId, source: SOURCES.BILLING };

    // Fallo de negocio simulado: es un evento (BillingFailed), no un error técnico → no va a la DLQ.
    if (event.payload.simulateFailure === 'billing') {
      const failed = createEvent({
        ...base,
        eventType: 'BillingFailed',
        payload: { reason: 'Fallo simulado en billing (simulateFailure=billing)' },
      });
      this.logger.warn(`[${event.correlationId}] ${event.eventType}: fallo simulado, no se crea la cuenta`);
      await this.publisher.publish(TOPICS.BILLING_EVENTS, failed);
      return;
    }

    // Upsert por activationId (índice único): una redelivery no crea una segunda cuenta.
    // TODO(fase 3): idempotencia completa con processed_events en la misma transacción.
    const account = await this.accounts
      .findOneAndUpdate(
        { activationId: event.correlationId },
        {
          $setOnInsert: {
            activationId: event.correlationId,
            accountId: `acc-${randomUUID().slice(0, 8)}`,
            customerId: event.customerId,
            planId: event.payload.planId,
            status: 'CREATED',
          },
        },
        { upsert: true, new: true },
      )
      .lean();

    this.logger.log(`[${event.correlationId}] ${event.eventType}: cuenta ${account!.accountId} ${account!.status}`);

    // TODO(fase 4 · outbox): publicación directa después de guardar. Si el servicio se cae entre el
    // upsert y el publish, el resultado se pierde y la activación queda IN_PROGRESS.
    await this.publisher.publish(
      TOPICS.BILLING_EVENTS,
      createEvent({
        ...base,
        eventType: 'BillingAccountCreated',
        payload: { accountId: account!.accountId, planId: account!.planId },
      }),
    );

    // Caso borde (documento §06): si ActivationFailed llegó antes de crear la cuenta, se anula ahora.
    const failure = await this.failures.findOne({ activationId: event.correlationId }).lean();
    if (failure) {
      this.logger.warn(`[${event.correlationId}] ${event.eventType}: ActivationFailed ya visto; se anula la cuenta recién creada`);
      await this.cancelAccount(event.correlationId, event.customerId, `ActivationFailed (${failure.failedStep}): ${failure.reason}`);
    }
  }

  /**
   * Compensación (RF-06). Primero recuerda el fallo y después anula la cuenta si existe.
   * Como onActivationRequested crea la cuenta y después busca el fallo (y acá es al revés),
   * al menos uno de los dos caminos ve al otro aunque corran en paralelo (p. ej. con billing escalado).
   * Si los dos lo ven, el update condicional CREATED → CANCELLED deja publicar a uno solo.
   */
  async onActivationFailed(event: ActivationFailed): Promise<void> {
    await this.failures.updateOne(
      { activationId: event.correlationId },
      {
        $setOnInsert: {
          activationId: event.correlationId,
          customerId: event.customerId,
          eventId: event.eventId,
          failedStep: event.payload.failedStep,
          reason: event.payload.reason,
        },
      },
      { upsert: true },
    );

    const cancelled = await this.cancelAccount(
      event.correlationId,
      event.customerId,
      `ActivationFailed (${event.payload.failedStep}): ${event.payload.reason}`,
    );
    if (!cancelled) {
      this.logger.log(
        `[${event.correlationId}] ${event.eventType}: no hay cuenta CREATED que anular (todavía no se creó, billing falló o ya estaba anulada); queda registrado`,
      );
    }
  }

  /** CREATED → CANCELLED (condicional) y publica BillingAccountCancelled. Devuelve false si no había nada que anular. */
  private async cancelAccount(activationId: string, customerId: string, reason: string): Promise<boolean> {
    const account = await this.accounts
      .findOneAndUpdate(
        { activationId, status: 'CREATED' },
        { $set: { status: 'CANCELLED', cancelledReason: reason } },
        { new: true },
      )
      .lean();
    if (!account) return false;

    this.logger.warn(`[${activationId}] BillingAccountCancelled: cuenta ${account.accountId} CANCELLED (${reason})`);
    // TODO(fase 4 · outbox): mismo riesgo de publicación directa.
    await this.publisher.publish(
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
