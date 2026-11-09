import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ActivationRequested, TOPICS } from '@poc/contracts';
import { commitAfter, logReceived } from '@poc/kafka-toolkit';
import { BillingService } from './billing.service';

/** Consumer group billing-svc. */
@Controller()
export class BillingConsumer {
  private readonly logger = new Logger(BillingConsumer.name);

  constructor(private readonly service: BillingService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onActivationRequested(@Payload() event: ActivationRequested, @Ctx() ctx: KafkaContext) {
    logReceived(this.logger, event, ctx);
    return commitAfter(ctx, () => this.service.onActivationRequested(event));
  }
}
