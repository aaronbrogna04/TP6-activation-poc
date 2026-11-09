import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { kafkaEnv, kafkaServerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const env = kafkaEnv({ clientId: 'billing-service', groupId: CONSUMER_GROUPS.BILLING });
  const app = await NestFactory.createMicroservice(AppModule.forRoot(env), kafkaServerOptions(env));
  app.enableShutdownHooks();
  await app.listen();
  Logger.log(`billing-service consumiendo (group=${env.groupId}, brokers=${env.brokers.join(',')})`, 'Bootstrap');
}

bootstrap();
