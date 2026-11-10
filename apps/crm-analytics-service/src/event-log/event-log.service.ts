import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { DomainEvent } from '@poc/contracts';
import { KafkaCtx, Tx } from '@poc/kafka-toolkit';
import { EventLogEntry } from './event-log.schema';

const SUMMARY_DEBOUNCE_MS = 2000;

@Injectable()
export class EventLogService implements OnModuleDestroy {
  private readonly logger = new Logger(EventLogService.name);
  private sinceSummary = 0;
  private timer?: NodeJS.Timeout;

  constructor(@InjectModel(EventLogEntry.name) private readonly log: Model<EventLogEntry>) {}

  /** RF-10: copia del evento en event_log, en la misma transacción que processed_events. */
  async record(event: DomainEvent, ctx: KafkaCtx, tx: Tx): Promise<void> {
    await this.log.create(
      [
        {
          eventId: event.eventId,
          eventType: event.eventType,
          version: event.version,
          correlationId: event.correlationId,
          customerId: event.customerId,
          source: event.source,
          occurredAt: new Date(event.occurredAt),
          payload: event.payload as unknown as Record<string, unknown>,
          topic: ctx.getTopic(),
          partition: ctx.getPartition(),
          offset: ctx.getMessage().offset,
          receivedAt: new Date(),
        },
      ],
      { session: tx.session },
    );
    tx.afterCommit(() => this.scheduleSummary());
  }

  /** Un resumen cuando el flujo se calma (p. ej. al terminar de leer el historial al arrancar). */
  private scheduleSummary(): void {
    this.sinceSummary++;
    clearTimeout(this.timer);
    this.timer = setTimeout(() => void this.logSummary(), SUMMARY_DEBOUNCE_MS);
  }

  private async logSummary(): Promise<void> {
    const byType = await this.log.aggregate<{ _id: string; n: number }>([
      { $group: { _id: '$eventType', n: { $sum: 1 } } },
      { $sort: { _id: 1 } },
    ]);
    const total = byType.reduce((acc, t) => acc + t.n, 0);
    const activations = (await this.log.distinct('correlationId')).length;
    this.logger.log(
      `event_log: +${this.sinceSummary} eventos → ${total} en total, ${activations} activaciones · ` +
        byType.map((t) => `${t._id}=${t.n}`).join(' '),
    );
    this.sinceSummary = 0;
  }

  onModuleDestroy(): void {
    clearTimeout(this.timer);
  }
}
