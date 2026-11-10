import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { initGroupOffsets, kafkaEnv, kafkaServerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

/**
 * Escenario 5: un consumidor nuevo, con su propio consumer group, que lee activation.events desde el inicio
 * (auto.offset.reset=earliest) y procesa todo el historial sin tocar a ningún otro servicio.
 */
async function bootstrap() {
  const env = kafkaEnv({ clientId: 'loyalty-service', groupId: CONSUMER_GROUPS.LOYALTY });
  const app = await NestFactory.createMicroservice(AppModule.forRoot(env), kafkaServerOptions(env, { fromBeginning: true }));
  app.enableShutdownHooks();
  await app.listen();
  await initGroupOffsets(app, env.groupId);
  Logger.log(`loyalty-service consumiendo desde el inicio (group=${env.groupId}, brokers=${env.brokers.join(',')})`, 'Bootstrap');
}

bootstrap();
