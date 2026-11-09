import { DynamicModule, Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaEnv, KafkaToolkitModule } from '@poc/kafka-toolkit';
import { BillingModule } from './billing/billing.module';

@Module({})
export class AppModule {
  static forRoot(env: KafkaEnv): DynamicModule {
    return {
      module: AppModule,
      imports: [
        MongooseModule.forRoot(
          process.env.MONGO_URI ?? 'mongodb://localhost:27017/billing?replicaSet=rs0&directConnection=true',
        ),
        KafkaToolkitModule.forRoot(env),
        BillingModule,
      ],
    };
  }
}
