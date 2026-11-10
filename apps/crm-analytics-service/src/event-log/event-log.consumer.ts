import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { EVENTS_BY_TOPIC, Topic, TOPICS } from '@poc/contracts';
import { EventConsumer, KafkaCtx } from '@poc/kafka-toolkit';
import { EventLogService } from './event-log.service';

/** Consumer group crm-analytics: suscripto a los 4 topics de negocio, no publica nada. */
@Controller()
export class EventLogConsumer {
  constructor(
    private readonly consumer: EventConsumer,
    private readonly service: EventLogService,
  ) {}

  private record(topic: Topic, ctx: KafkaCtx) {
    return this.consumer.process(ctx, EVENTS_BY_TOPIC[topic], (e, tx) => this.service.record(e, ctx, tx));
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onActivationRequested(@Ctx() ctx: KafkaContext) {
    return this.record(TOPICS.ACTIVATION_REQUESTED, ctx);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBilling(@Ctx() ctx: KafkaContext) {
    return this.record(TOPICS.BILLING_EVENTS, ctx);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioning(@Ctx() ctx: KafkaContext) {
    return this.record(TOPICS.PROVISIONING_EVENTS, ctx);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivation(@Ctx() ctx: KafkaContext) {
    return this.record(TOPICS.ACTIVATION_EVENTS, ctx);
  }
}
