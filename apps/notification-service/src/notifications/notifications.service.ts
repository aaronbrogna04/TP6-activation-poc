import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { ActivationEvent } from '@poc/contracts';
import { Mailer } from './mailer';
import { Notification } from './notification.schema';
import { mailFor } from './templates';

@Injectable()
export class NotificationsService {
  private readonly logger = new Logger(NotificationsService.name);

  constructor(
    @InjectModel(Notification.name) private readonly notifications: Model<Notification>,
    private readonly mailer: Mailer,
  ) {}

  /**
   * RF-07: un email por cada ActivationCompleted o ActivationFailed.
   * Si ya hay una notificación para (activationId, eventType), una redelivery no reenvía el email.
   * TODO(fase 3): processed_events en la misma transacción. Hoy, si el servicio se cae entre el envío
   * y el insert, la redelivery manda el email otra vez.
   */
  async onActivationEvent(event: ActivationEvent): Promise<void> {
    const key = { activationId: event.correlationId, eventType: event.eventType };
    const tag = `[${event.correlationId}] ${event.eventType}`;
    if (await this.notifications.exists(key)) {
      this.logger.log(`${tag}: el email ya se había enviado; se descarta`);
      return;
    }

    const mail = mailFor(event);
    const messageId = await this.mailer.send(mail);
    await this.notifications.updateOne(
      key,
      {
        $setOnInsert: {
          ...key,
          eventId: event.eventId,
          customerId: event.customerId,
          to: mail.to,
          subject: mail.subject,
          status: 'SENT',
          messageId,
          sentAt: new Date(),
        },
      },
      { upsert: true },
    );
    this.logger.log(`${tag}: email "${mail.subject}" enviado a ${mail.to}`);
  }
}
