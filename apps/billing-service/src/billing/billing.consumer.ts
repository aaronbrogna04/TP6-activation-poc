import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer } from '@poc/kafka-toolkit';
import { BillingService } from './billing.service';

/** Consumer group billing-svc. Parseo, contrato, idempotencia, reintentos, DLQ y commit: EventConsumer. */
@Controller()
export class BillingConsumer {
  constructor(
    private readonly consumer: EventConsumer,
    private readonly service: BillingService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onActivationRequested(@Ctx() ctx: KafkaContext) {
    return this.consumer.process(ctx, ['ActivationRequested'], (e, tx) => this.service.onActivationRequested(e, tx));
  }

  /** Compensación (RF-06): ActivationFailed anula la cuenta. ActivationCompleted no requiere nada. */
  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Ctx() ctx: KafkaContext) {
    return this.consumer.process(ctx, ['ActivationFailed'], (e, tx) => this.service.onActivationFailed(e, tx));
  }
}
