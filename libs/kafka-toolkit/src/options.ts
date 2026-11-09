import { KafkaOptions, Transport } from '@nestjs/microservices';
import { logLevel, Partitioners } from 'kafkajs';
import { KafkaEnv } from './config';

const SESSION_TIMEOUT_MS = Number(process.env.KAFKA_SESSION_TIMEOUT_MS ?? 30000);

function baseClient(env: KafkaEnv) {
  return {
    clientId: env.clientId,
    brokers: env.brokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 10 },
  };
}

const producerConfig = {
  // Los topics los crea solo kafka-init (auto.create.topics.enable=false en el broker).
  allowAutoTopicCreation: false,
  // Murmur2 compatible con el cliente Java: misma partición para la misma clave (customerId).
  createPartitioner: Partitioners.DefaultPartitioner,
};

/**
 * Opciones del servidor (consumidor) Nest con transporte Kafka.
 * - postfixId: '' → Nest agrega "-server" a clientId/groupId por defecto; así el consumer group
 *   se llama exactamente como en el documento (billing-svc, provisioning-svc, activation-api, ...).
 * - autoCommit: false → commit manual después de procesar (ver commitAfter).
 */
export function kafkaServerOptions(env: KafkaEnv): KafkaOptions {
  return {
    transport: Transport.KAFKA,
    options: {
      postfixId: '',
      client: baseClient(env),
      consumer: {
        groupId: env.groupId,
        sessionTimeout: SESSION_TIMEOUT_MS,
        allowAutoTopicCreation: false,
      },
      producer: producerConfig,
      run: { autoCommit: false },
      subscribe: { fromBeginning: false },
    },
  };
}

/**
 * Opciones del ClientKafka (solo productor).
 * producerOnlyMode evita que el cliente cree su propio consumer: con postfixId '' se uniría
 * al mismo consumer group que el servidor.
 */
export function kafkaProducerOptions(env: KafkaEnv): KafkaOptions['options'] {
  return {
    postfixId: '',
    producerOnlyMode: true,
    client: { ...baseClient(env), clientId: `${env.clientId}-producer` },
    producer: producerConfig,
  };
}
