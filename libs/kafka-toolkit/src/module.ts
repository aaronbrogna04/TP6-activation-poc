import { DynamicModule, Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { KafkaEnv } from './config';
import { kafkaProducerOptions } from './options';
import { EventPublisher, KAFKA_PRODUCER } from './publisher';

@Module({})
export class KafkaToolkitModule {
  static forRoot(env: KafkaEnv): DynamicModule {
    return {
      module: KafkaToolkitModule,
      global: true,
      imports: [
        ClientsModule.register([
          { name: KAFKA_PRODUCER, transport: Transport.KAFKA, options: kafkaProducerOptions(env) },
        ]),
      ],
      providers: [EventPublisher],
      exports: [EventPublisher],
    };
  }
}
