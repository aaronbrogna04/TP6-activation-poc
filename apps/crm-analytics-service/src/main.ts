import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { initGroupOffsets, kafkaEnv, kafkaServerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const env = kafkaEnv({ clientId: 'crm-analytics-service', groupId: CONSUMER_GROUPS.CRM_ANALYTICS });
  // fromBeginning: un group nuevo lee todo el historial retenido (RF-10).
  const app = await NestFactory.createMicroservice(AppModule.forRoot(env), kafkaServerOptions(env, { fromBeginning: true }));
  app.enableShutdownHooks();
  await app.listen();
  await initGroupOffsets(app, env.groupId);
  Logger.log(`crm-analytics-service consumiendo (group=${env.groupId}, brokers=${env.brokers.join(',')})`, 'Bootstrap');
}

bootstrap();
