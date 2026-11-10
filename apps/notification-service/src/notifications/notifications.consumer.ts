import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventConsumer } from '@poc/kafka-toolkit';
import { NotificationsService } from './notifications.service';

/** Consumer group notification-svc. Parseo, contrato, idempotencia, reintentos, DLQ y commit: EventConsumer. */
@Controller()
export class NotificationsConsumer {
  constructor(
    private readonly consumer: EventConsumer,
    private readonly service: NotificationsService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Ctx() ctx: KafkaContext) {
    return this.consumer.process(ctx, ['ActivationCompleted', 'ActivationFailed'], (e, tx) =>
      this.service.onActivationEvent(e, tx),
    );
  }
}
