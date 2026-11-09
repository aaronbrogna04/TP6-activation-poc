import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { kafkaEnv, kafkaServerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const env = kafkaEnv({ clientId: 'notification-service', groupId: CONSUMER_GROUPS.NOTIFICATION });
  const app = await NestFactory.createMicroservice(AppModule, kafkaServerOptions(env));
  app.enableShutdownHooks();
  await app.listen();
  Logger.log(`notification-service consumiendo (group=${env.groupId}, brokers=${env.brokers.join(',')})`, 'Bootstrap');
}

bootstrap();
