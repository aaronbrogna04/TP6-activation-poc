import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ActivationRequested, TOPICS } from '@poc/contracts';
import { commitAfter, logReceived } from '@poc/kafka-toolkit';
import { ProvisioningService } from './provisioning.service';

/** Consumer group provisioning-svc. */
@Controller()
export class ProvisioningConsumer {
  private readonly logger = new Logger(ProvisioningConsumer.name);

  constructor(private readonly service: ProvisioningService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onActivationRequested(@Payload() event: ActivationRequested, @Ctx() ctx: KafkaContext) {
    logReceived(this.logger, event, ctx);
    return commitAfter(ctx, () => this.service.onActivationRequested(event));
  }
}
