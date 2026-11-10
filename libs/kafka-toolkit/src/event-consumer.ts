import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { getConnectionToken } from '@nestjs/mongoose';
import type { ClientSession, Connection, Model } from 'mongoose';
import { DomainEvent, EventType, Topic, validateEvent } from '@poc/contracts';
import { KafkaCtx, where } from './consumer';
import { DlqPublisher } from './dlq';
import { ProcessedEvent, ProcessedEventSchema } from './processed-event';
import { EventPublisher } from './publisher';
import { errorMessage, RetriesExhaustedError, withRetries } from './retry';

/**
 * Transacción en la que corre el efecto de un handler.
 * - session: pasarla a TODAS las escrituras/lecturas de Mongo del handler.
 * - publish / afterCommit: se ejecutan recién después del commit (si la transacción se reintenta,
 *   lo encolado en el intento anterior se descarta).
 */
export interface Tx {
  readonly session: ClientSession;
  publish(topic: Topic, event: DomainEvent): void;
  afterCommit(fn: () => unknown): void;
}

/** Evento recibido, como unión discriminada por eventType de los tipos aceptados. */
export type EventFor<K extends EventType> = Extract<DomainEvent, { eventType: K }>;
export type Handler<K extends EventType> = (event: EventFor<K>, tx: Tx) => Promise<void>;

class DuplicateEventError extends Error {}

interface Pending {
  publish: { topic: Topic; event: DomainEvent }[];
  after: (() => unknown)[];
}

const isDuplicateKey = (err: unknown): boolean => (err as { code?: number })?.code === 11000;

/**
 * Procesa un mensaje de Kafka de forma confiable (documento §08, decisiones 1 y 2). Lo usan todos los consumidores:
 *
 * 1. Parsea y valida el contrato. Si no se puede → DLQ directo (sin reintentos: reintentar no lo arregla).
 * 2. Idempotencia: inserta eventId en processed_events en la MISMA transacción que el efecto del handler.
 *    Duplicado → se descarta y se confirma el offset.
 * 3. Error técnico → 3 reintentos (1 s, 2 s, 4 s). Si sigue fallando → mensaje original a <topic>.dlq.
 *    Los fallos de negocio simulados no lanzan: el handler publica BillingFailed/ProvisioningFailed.
 * 4. Después del commit de Mongo: publica los eventos encolados y corre los afterCommit.
 * 5. Recién al final confirma el offset (commit manual, nunca antes). Nunca deja la partición bloqueada.
 */
@Injectable()
export class EventConsumer implements OnModuleInit {
  private readonly logger = new Logger('EventConsumer');
  private readonly processed: Model<ProcessedEvent>;

  constructor(
    @Inject(getConnectionToken()) private readonly connection: Connection,
    private readonly publisher: EventPublisher,
    private readonly dlq: DlqPublisher,
  ) {
    this.processed = connection.models.ProcessedEvent ?? connection.model('ProcessedEvent', ProcessedEventSchema);
  }

  async onModuleInit(): Promise<void> {
    // Crea la colección y los índices (eventId único + TTL) antes de consumir: dentro de una
    // transacción no conviene crear colecciones implícitamente.
    await this.processed.createCollection().catch(() => undefined);
    await this.processed.init();
  }

  /**
   * @param accepts tipos de evento que este consumidor procesa. Los demás tipos válidos del topic
   *                (p. ej. ActivationCompleted para billing) se confirman sin efecto.
   */
  async process<K extends EventType>(ctx: KafkaCtx, accepts: readonly K[], handle: Handler<K>): Promise<void> {
    const msg = ctx.getMessage();
    const topic = ctx.getTopic();

    // 1) Parseo + contrato.
    const parsed = this.decode(topic, msg.value);
    if (!parsed.ok) {
      this.logger.warn(`${where(ctx)} mensaje inválido: ${parsed.reason}`);
      await this.toDlq(ctx, 'INVALID_MESSAGE', parsed.reason, 1);
      return;
    }
    const event = parsed.event;
    const tag = `[${event.correlationId}] ${event.eventType}`;

    if (!(accepts as readonly string[]).includes(event.eventType)) {
      this.logger.debug(`${tag} ← ${where(ctx)}: no aplica a este servicio`);
      await this.commit(ctx);
      return;
    }
    this.logger.log(`${tag} ← ${where(ctx)} (key=${msg.key?.toString()}, eventId=${event.eventId})`);

    // Atajo: si ya está procesado no se abre transacción (p. ej. provisioning no espera su retardo).
    // La garantía real es el índice único dentro de la transacción.
    if (await this.processed.exists({ eventId: event.eventId })) {
      this.logDuplicate(tag, event.eventId);
      await this.commit(ctx);
      return;
    }

    // 2) + 3) Efecto en transacción, con reintentos.
    let pending: Pending;
    try {
      pending = await withRetries(() => this.runInTransaction(ctx, event as EventFor<K>, handle), {
        shouldRetry: (err) => !(err instanceof DuplicateEventError),
        onRetry: async (attempt, err, delay) => {
          this.logger.warn(`${tag}: intento ${attempt} falló (${errorMessage(err)}); reintento en ${delay / 1000} s`);
          await ctx.getHeartbeat()().catch(() => undefined);
        },
      });
    } catch (err) {
      if (err instanceof DuplicateEventError) {
        this.logDuplicate(tag, event.eventId);
        await this.commit(ctx);
        return;
      }
      const attempts = err instanceof RetriesExhaustedError ? err.attempts : 1;
      this.logger.error(`${tag}: falló ${attempts} veces; va a la DLQ`);
      await this.toDlq(ctx, 'PROCESSING_ERROR', errorMessage(err instanceof RetriesExhaustedError ? err.lastError : err), attempts);
      return;
    }

    // 4) Efectos posteriores al commit de Mongo.
    await this.flush(tag, pending);

    // 5) Offset.
    await this.commit(ctx);
  }

  private decode(topic: string, value: Buffer | null) {
    if (value === null || value.length === 0) return { ok: false as const, reason: 'mensaje vacío' };
    let json: unknown;
    try {
      json = JSON.parse(value.toString('utf8'));
    } catch (err) {
      return { ok: false as const, reason: `JSON inválido: ${errorMessage(err)}` };
    }
    return validateEvent(topic, json);
  }

  private async runInTransaction<K extends EventType>(ctx: KafkaCtx, event: EventFor<K>, handle: Handler<K>): Promise<Pending> {
    const session = await this.connection.startSession();
    let pending: Pending = { publish: [], after: [] };
    try {
      // withTransaction reintenta solo ante TransientTransactionError (p. ej. write conflict con otra
      // instancia): por eso lo encolado se reinicia en cada vuelta.
      await session.withTransaction(async () => {
        pending = { publish: [], after: [] };
        try {
          await this.processed.create(
            [
              {
                eventId: event.eventId,
                eventType: event.eventType,
                correlationId: event.correlationId,
                topic: ctx.getTopic(),
                partition: ctx.getPartition(),
                offset: ctx.getMessage().offset,
                processedAt: new Date(),
              },
            ],
            { session },
          );
        } catch (err) {
          if (isDuplicateKey(err)) throw new DuplicateEventError(event.eventId);
          throw err;
        }
        const p = pending;
        await handle(event, {
          session,
          publish: (topic, e) => void p.publish.push({ topic, event: e }),
          afterCommit: (fn) => void p.after.push(fn),
        });
      });
      return pending;
    } finally {
      await session.endSession();
    }
  }

  /**
   * TODO(fase 4 · outbox): se publica directo después de guardar. Si el servicio se cae entre el commit de
   * Mongo y este publish, el evento se pierde (y la redelivery lo descarta por duplicado). Por eso se reintenta
   * acá mismo, sin volver a correr el efecto.
   */
  private async flush(tag: string, pending: Pending): Promise<void> {
    for (const { topic, event } of pending.publish) {
      try {
        await withRetries(() => this.publisher.publish(topic, event), {
          onRetry: (attempt, err, delay) =>
            this.logger.warn(`${tag}: publicar ${event.eventType} falló (intento ${attempt}: ${errorMessage(err)}); reintento en ${delay / 1000} s`),
        });
      } catch (err) {
        this.logger.error(`${tag}: ${event.eventType} NO se publicó tras los reintentos (riesgo documentado; outbox en fase 4): ${errorMessage(err)}`);
      }
    }
    for (const fn of pending.after) {
      try {
        await fn();
      } catch (err) {
        this.logger.warn(`${tag}: afterCommit falló: ${errorMessage(err)}`);
      }
    }
  }

  private async toDlq(ctx: KafkaCtx, reason: 'INVALID_MESSAGE' | 'PROCESSING_ERROR', error: string, attempts: number) {
    // Si la DLQ tampoco está disponible no se confirma el offset: lanzar hace que kafkajs lo reentregue.
    await withRetries(() => this.dlq.send(ctx, reason, error, attempts));
    await this.commit(ctx);
  }

  private logDuplicate(tag: string, eventId: string) {
    this.logger.warn(`${tag}: eventId ${eventId} ya procesado (processed_events); se descarta y se confirma el offset`);
  }

  /** Commit manual del offset siguiente. Si falla (p. ej. rebalanceo) el mensaje se reentrega y la idempotencia lo descarta. */
  private async commit(ctx: KafkaCtx): Promise<void> {
    const { offset } = ctx.getMessage();
    try {
      await ctx.getConsumer().commitOffsets([
        { topic: ctx.getTopic(), partition: ctx.getPartition(), offset: (BigInt(offset) + 1n).toString() },
      ]);
    } catch (err) {
      this.logger.warn(`${where(ctx)}: no se pudo confirmar el offset (${errorMessage(err)}); se reentregará y se descartará por duplicado`);
    }
  }
}
