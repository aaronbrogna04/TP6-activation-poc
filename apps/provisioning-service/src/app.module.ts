import { DynamicModule, Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaEnv, KafkaToolkitModule } from '@poc/kafka-toolkit';
import { ProvisioningModule } from './provisioning/provisioning.module';

@Module({})
export class AppModule {
  static forRoot(env: KafkaEnv): DynamicModule {
    return {
      module: AppModule,
      imports: [
        MongooseModule.forRoot(
          process.env.MONGO_URI ?? 'mongodb://localhost:27017/provisioning?replicaSet=rs0&directConnection=true',
        ),
        KafkaToolkitModule.forRoot(env),
        ProvisioningModule,
      ],
    };
  }
}
