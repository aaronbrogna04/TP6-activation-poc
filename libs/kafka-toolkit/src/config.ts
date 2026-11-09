export interface KafkaEnv {
  brokers: string[];
  clientId: string;
  groupId: string;
}

/**
 * Lee las variables comunes del documento: KAFKA_BROKERS, KAFKA_CLIENT_ID, KAFKA_GROUP_ID.
 * Los defaults son los nombres del documento para cada servicio.
 */
export function kafkaEnv(defaults: { clientId: string; groupId: string }): KafkaEnv {
  const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((b) => b.trim())
    .filter(Boolean);
  return {
    brokers,
    clientId: process.env.KAFKA_CLIENT_ID ?? defaults.clientId,
    groupId: process.env.KAFKA_GROUP_ID ?? defaults.groupId,
  };
}
