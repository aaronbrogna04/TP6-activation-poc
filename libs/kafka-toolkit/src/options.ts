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

export interface ServerOptions {
  /**
   * Dónde empieza un group en las particiones donde todavía no tiene offset confirmado
   * (= auto.offset.reset=earliest). Default true: con commit manual, una partición que el group nunca
   * confirmó (porque no le llegó nada) arrancaría en "latest" al volver y perdería lo publicado mientras
   * estaba caído (RNF-05, escenario 3). Ver D-26 en CLAUDE.md.
   */
  fromBeginning?: boolean;
}

/**
 * Opciones del servidor (consumidor) Nest con transporte Kafka.
 * - postfixId: '' → Nest agrega "-server" a clientId/groupId por defecto; así el consumer group
 *   se llama exactamente como en el documento (billing-svc, provisioning-svc, activation-api, ...).
 * - autoCommit: false → commit manual después de procesar (ver EventConsumer).
 * - parser.keepBinary → el value llega como Buffer: lo parsea y valida EventConsumer, y a la DLQ
 *   va el mensaje original byte a byte.
 */
export function kafkaServerOptions(env: KafkaEnv, opts: ServerOptions = {}): KafkaOptions {
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
      parser: { keepBinary: true },
      run: { autoCommit: false },
      subscribe: { fromBeginning: opts.fromBeginning ?? true },
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
