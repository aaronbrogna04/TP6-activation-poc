import { DynamicModule, Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaEnv, KafkaToolkitModule } from '@poc/kafka-toolkit';
import { NotificationsModule } from './notifications/notifications.module';

/** notification no publica eventos de negocio; el productor del toolkit se usa solo para la DLQ. */
@Module({})
export class AppModule {
  static forRoot(env: KafkaEnv): DynamicModule {
    return {
      module: AppModule,
      imports: [
        MongooseModule.forRoot(
          process.env.MONGO_URI ?? 'mongodb://localhost:27017/notification?replicaSet=rs0&directConnection=true',
        ),
        KafkaToolkitModule.forRoot(env),
        NotificationsModule,
      ],
    };
  }
}
