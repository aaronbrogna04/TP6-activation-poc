import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer } from '@poc/kafka-toolkit';
import { ProvisioningService } from './provisioning.service';

/** Consumer group provisioning-svc. Parseo, contrato, idempotencia, reintentos, DLQ y commit: EventConsumer. */
@Controller()
export class ProvisioningConsumer {
  constructor(
    private readonly consumer: EventConsumer,
    private readonly service: ProvisioningService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onActivationRequested(@Ctx() ctx: KafkaContext) {
    return this.consumer.process(ctx, ['ActivationRequested'], (e, tx) => this.service.onActivationRequested(e, tx));
  }
}
