import { Logger } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import { DomainEvent } from '@poc/contracts';

/** Log uniforme de un evento recibido: [correlationId] eventType ← topic/partition@offset */
export function logReceived(logger: Logger, event: DomainEvent, ctx: KafkaContext): void {
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
export async function commitAfter<T>(ctx: KafkaContext, handler: () => Promise<T>): Promise<T> {
  const result = await handler();
  const { offset } = ctx.getMessage();
  await ctx.getConsumer().commitOffsets([
    { topic: ctx.getTopic(), partition: ctx.getPartition(), offset: (BigInt(offset) + 1n).toString() },
  ]);
  return result;
}
