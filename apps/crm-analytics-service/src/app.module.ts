import { DynamicModule, Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaEnv, KafkaToolkitModule } from '@poc/kafka-toolkit';
import { EventLogModule } from './event-log/event-log.module';

/** crm-analytics no publica eventos; el productor del toolkit se usa solo para la DLQ. */
@Module({})
export class AppModule {
  static forRoot(env: KafkaEnv): DynamicModule {
    return {
      module: AppModule,
      imports: [
        MongooseModule.forRoot(
          process.env.MONGO_URI ?? 'mongodb://localhost:27017/crm?replicaSet=rs0&directConnection=true',
        ),
        KafkaToolkitModule.forRoot(env),
        EventLogModule,
      ],
    };
  }
}
