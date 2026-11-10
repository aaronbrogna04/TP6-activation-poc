import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { ActivationEvent } from '@poc/contracts';
import { Tx } from '@poc/kafka-toolkit';
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
   * RF-07: un email por cada ActivationCompleted o ActivationFailed. Corre en la transacción de EventConsumer,
   * que ya insertó el eventId en processed_events:
   *   1. si ya hay notificación para (activationId, eventType) — p. ej. el mismo resultado con otro eventId —
   *      no se reenvía;
   *   2. se inserta la notificación;
   *   3. se envía el email por SMTP como ÚLTIMO paso antes del commit.
   * Si el SMTP falla, la transacción se aborta (no queda ni processed_events ni la notificación) y
   * EventConsumer reintenta 1 s / 2 s / 4 s y después manda el mensaje a la DLQ.
   * Un duplicado concurrente choca antes en el insert de processed_events (write conflict / clave duplicada),
   * así que no llega a enviar. Riesgo residual: si el proceso cae entre el envío y el commit, la redelivery
   * reenvía; el Message-ID derivado del eventId permite identificar ese duplicado.
   */
  async onActivationEvent(event: ActivationEvent, tx: Tx): Promise<void> {
    const { session } = tx;
    const key = { activationId: event.correlationId, eventType: event.eventType };
    const tag = `[${event.correlationId}] ${event.eventType}`;
    if (await this.notifications.exists(key).session(session)) {
      this.logger.log(`${tag}: el email ya se había enviado; se descarta`);
      return;
    }

    const mail = mailFor(event);
    const messageId = `<${event.eventId}@activation-poc.example>`;
    await this.notifications.create(
      [
        {
          ...key,
          eventId: event.eventId,
          customerId: event.customerId,
          to: mail.to,
          subject: mail.subject,
          status: 'SENT',
          messageId,
          sentAt: new Date(),
        },
      ],
      { session },
    );
    await this.mailer.send({ ...mail, messageId });
    this.logger.log(`${tag}: email "${mail.subject}" enviado a ${mail.to}`);
  }
}
