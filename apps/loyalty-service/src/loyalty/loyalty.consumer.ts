import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer } from '@poc/kafka-toolkit';
import { LoyaltyService } from './loyalty.service';

/** Consumer group loyalty-svc. */
@Controller()
export class LoyaltyConsumer {
  constructor(
    private readonly consumer: EventConsumer,
    private readonly service: LoyaltyService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Ctx() ctx: KafkaContext) {
    return this.consumer.process(ctx, ['ActivationCompleted', 'ActivationFailed'], (e, tx) => this.service.apply(e, tx));
  }
}
