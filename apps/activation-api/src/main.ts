import 'reflect-metadata';
import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { kafkaEnv, kafkaServerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const env = kafkaEnv({ clientId: 'activation-api', groupId: CONSUMER_GROUPS.ACTIVATION_API });
  const app = await NestFactory.create(AppModule.forRoot(env));
  app.enableCors({ origin: (process.env.CORS_ORIGIN ?? 'http://localhost:5173').split(',') });
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }));
  app.enableShutdownHooks();

  // App híbrida: HTTP + consumidor de billing.events / provisioning.events (group activation-api).
  app.connectMicroservice(kafkaServerOptions(env), { inheritAppConfig: true });
  await app.startAllMicroservices();

  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port);
  Logger.log(`activation-api escuchando en :${port} (group=${env.groupId}, brokers=${env.brokers.join(',')})`, 'Bootstrap');
}

bootstrap();
