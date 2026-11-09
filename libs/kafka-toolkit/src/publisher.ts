import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { lastValueFrom } from 'rxjs';
import { DomainEvent, HEADERS, Topic } from '@poc/contracts';

export const KAFKA_PRODUCER = Symbol('KAFKA_PRODUCER');

/**
 * Publica eventos con el sobre común.
 * - key = customerId → orden por cliente (RNF-02).
 * - headers eventType / correlationId / eventId → rastreo sin deserializar (RNF-06).
 * Espera la confirmación del broker antes de resolver.
 */
@Injectable()
export class EventPublisher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger('EventPublisher');

  constructor(@Inject(KAFKA_PRODUCER) private readonly client: ClientKafka) {}

  async onModuleInit(): Promise<void> {
    await this.client.connect();
  }

  async onModuleDestroy(): Promise<void> {
    await this.client.close();
  }

  async publish(topic: Topic, event: DomainEvent): Promise<void> {
    await lastValueFrom(
      this.client.emit(topic, {
        key: event.customerId,
        value: event,
        headers: {
          [HEADERS.EVENT_TYPE]: event.eventType,
          [HEADERS.CORRELATION_ID]: event.correlationId,
          [HEADERS.EVENT_ID]: event.eventId,
        },
      }),
      { defaultValue: undefined },
    );
    this.logger.log(`[${event.correlationId}] ${event.eventType} → ${topic} (key=${event.customerId})`);
  }
}
