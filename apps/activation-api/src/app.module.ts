import { DynamicModule, Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaEnv, KafkaToolkitModule } from '@poc/kafka-toolkit';
import { ActivationsModule } from './activations/activations.module';
import { HealthController } from './health.controller';

@Module({})
export class AppModule {
  static forRoot(env: KafkaEnv): DynamicModule {
    return {
      module: AppModule,
      controllers: [HealthController],
      imports: [
        MongooseModule.forRoot(
          process.env.MONGO_URI ?? 'mongodb://localhost:27017/activation?replicaSet=rs0&directConnection=true',
        ),
        KafkaToolkitModule.forRoot(env),
        ActivationsModule,
      ],
    };
  }
}
