import type { Consumer, KafkaMessage } from 'kafkajs';

/**
 * Lo que usamos del KafkaContext de Nest, como tipo estructural: cada app puede resolver su propia
 * copia de @nestjs/microservices (p. ej. activation-api, que suma el peer opcional @nestjs/websockets)
 * y una clase con miembros protected no es asignable entre copias.
 * Con parser.keepBinary, getMessage().value es el Buffer original; key y headers vienen decodificados por Nest.
 */
export interface KafkaCtx {
  getMessage(): KafkaMessage;
  getTopic(): string;
  getPartition(): number;
  getConsumer(): Consumer;
  getHeartbeat(): () => Promise<void>;
}

/** topic/partition@offset, para los logs. */
export const where = (ctx: KafkaCtx): string => `${ctx.getTopic()}/${ctx.getPartition()}@${ctx.getMessage().offset}`;

/** Clave del mensaje como string (Nest la decodifica; puede venir null). */
export function keyOf(ctx: KafkaCtx): string | null {
  const key = ctx.getMessage().key as unknown;
  if (key === null || key === undefined) return null;
  return Buffer.isBuffer(key) ? key.toString() : String(key);
}
