import { DynamicModule, Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { KafkaEnv } from './config';
import { DlqPublisher, TOOLKIT_OPTIONS, ToolkitOptions } from './dlq';
import { EventConsumer } from './event-consumer';
import { kafkaProducerOptions } from './options';
import { EventPublisher, KAFKA_PRODUCER } from './publisher';

/**
 * Productor + EventConsumer (idempotencia, reintentos, DLQ). Requiere MongooseModule.forRoot en la app:
 * processed_events vive en la base de cada servicio.
 */
@Module({})
export class KafkaToolkitModule {
  static forRoot(env: KafkaEnv, opts: Partial<ToolkitOptions> = {}): DynamicModule {
    const options: ToolkitOptions = { service: opts.service ?? env.clientId };
    return {
      module: KafkaToolkitModule,
      global: true,
      imports: [
        ClientsModule.register([
          { name: KAFKA_PRODUCER, transport: Transport.KAFKA, options: kafkaProducerOptions(env) },
        ]),
      ],
      providers: [{ provide: TOOLKIT_OPTIONS, useValue: options }, EventPublisher, DlqPublisher, EventConsumer],
      exports: [EventPublisher, EventConsumer],
    };
  }
}
