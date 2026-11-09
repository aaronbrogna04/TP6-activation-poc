import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { randomUUID } from 'node:crypto';
import { ActivationRequested, createEvent, SOURCES, TOPICS } from '@poc/contracts';
import { EventPublisher } from '@poc/kafka-toolkit';
import { BillingAccount } from './billing-account.schema';

@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);

  constructor(
    @InjectModel(BillingAccount.name) private readonly accounts: Model<BillingAccount>,
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
      this.logger.warn(`[${event.correlationId}] fallo simulado: no se crea la cuenta`);
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

    this.logger.log(`[${event.correlationId}] cuenta ${account!.accountId} ${account!.status}`);

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
  }

  // TODO(fase 2 · compensación, RF-06): consumir activation.events → ActivationFailed anula la cuenta
  // (CANCELLED + BillingAccountCancelled), también si la cuenta se crea después de haber visto el fallo.
}
