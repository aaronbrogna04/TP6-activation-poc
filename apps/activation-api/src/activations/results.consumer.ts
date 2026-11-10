import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer } from '@poc/kafka-toolkit';
import { ActivationsService } from './activations.service';

/** Consumer group activation-api: junta los resultados de billing y provisioning. */
@Controller()
export class ResultsConsumer {
  constructor(
    private readonly consumer: EventConsumer,
    private readonly service: ActivationsService,
  ) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBilling(@Ctx() ctx: KafkaContext) {
    return this.consumer.process(ctx, ['BillingAccountCreated', 'BillingFailed', 'BillingAccountCancelled'], (e, tx) =>
      this.service.applyResult(e, tx),
    );
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioning(@Ctx() ctx: KafkaContext) {
    return this.consumer.process(ctx, ['ProvisioningCompleted', 'ProvisioningFailed'], (e, tx) =>
      this.service.applyResult(e, tx),
    );
  }
}
