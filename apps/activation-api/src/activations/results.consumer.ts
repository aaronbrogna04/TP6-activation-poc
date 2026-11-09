import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { BillingEvent, ProvisioningEvent, TOPICS } from '@poc/contracts';
import { commitAfter, logReceived } from '@poc/kafka-toolkit';
import { ActivationsService } from './activations.service';

/** Consumer group activation-api: junta los resultados de billing y provisioning. */
@Controller()
export class ResultsConsumer {
  private readonly logger = new Logger(ResultsConsumer.name);

  constructor(private readonly service: ActivationsService) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBilling(@Payload() event: BillingEvent, @Ctx() ctx: KafkaContext) {
    logReceived(this.logger, event, ctx);
    return commitAfter(ctx, () => this.service.applyResult(event));
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioning(@Payload() event: ProvisioningEvent, @Ctx() ctx: KafkaContext) {
    logReceived(this.logger, event, ctx);
    return commitAfter(ctx, () => this.service.applyResult(event));
  }
}
