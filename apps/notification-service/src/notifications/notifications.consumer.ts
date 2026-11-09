import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ActivationEvent, TOPICS } from '@poc/contracts';
import { commitAfter, logReceived } from '@poc/kafka-toolkit';
import { NotificationsService } from './notifications.service';

/** Consumer group notification-svc. */
@Controller()
export class NotificationsConsumer {
  private readonly logger = new Logger(NotificationsConsumer.name);

  constructor(private readonly service: NotificationsService) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Payload() event: ActivationEvent, @Ctx() ctx: KafkaContext) {
    logReceived(this.logger, event, ctx);
    return commitAfter(ctx, () => this.service.onActivationEvent(event));
  }
}
