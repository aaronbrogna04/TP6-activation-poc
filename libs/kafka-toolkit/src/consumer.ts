import { Logger } from '@nestjs/common';
import type { Consumer, KafkaMessage } from 'kafkajs';
import { DomainEvent } from '@poc/contracts';

/**
 * Lo que usamos del KafkaContext de Nest, como tipo estructural: cada app puede resolver su propia
 * copia de @nestjs/microservices (p. ej. activation-api, que suma el peer opcional @nestjs/websockets)
 * y una clase con miembros protected no es asignable entre copias.
 */
export interface KafkaCtx {
  getMessage(): KafkaMessage;
  getTopic(): string;
  getPartition(): number;
  getConsumer(): Consumer;
}

/** Log uniforme de un evento recibido: [correlationId] eventType ← topic/partition@offset */
export function logReceived(logger: Logger, event: DomainEvent, ctx: KafkaCtx): void {
  const msg = ctx.getMessage();
  logger.log(
    `[${event.correlationId}] ${event.eventType} ← ${ctx.getTopic()}/${ctx.getPartition()}@${msg.offset} (key=${msg.key?.toString()})`,
  );
}

/**
 * Ejecuta el handler y recién después confirma el offset (commit manual, nunca antes).
 * Si el handler lanza, no se confirma y el error se propaga.
 * TODO(fase 3): idempotencia (processed_events), reintentos 1s/2s/4s y DLQ se enganchan acá.
 */
export async function commitAfter<T>(ctx: KafkaCtx, handler: () => Promise<T>): Promise<T> {
  const result = await handler();
  const { offset } = ctx.getMessage();
  await ctx.getConsumer().commitOffsets([
    { topic: ctx.getTopic(), partition: ctx.getPartition(), offset: (BigInt(offset) + 1n).toString() },
  ]);
  return result;
}
