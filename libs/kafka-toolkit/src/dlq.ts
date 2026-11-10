import { Inject, Injectable, Logger } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { hostname } from 'node:os';
import { lastValueFrom } from 'rxjs';
import { DLQ_HEADERS } from '@poc/contracts';
import { KafkaCtx, keyOf, where } from './consumer';
import { KAFKA_PRODUCER } from './publisher';

export const TOOLKIT_OPTIONS = Symbol('TOOLKIT_OPTIONS');
export interface ToolkitOptions {
  /** Nombre del servicio que consume (header dlq-service). */
  service: string;
}

export type DlqReason = 'INVALID_MESSAGE' | 'PROCESSING_ERROR';

/** Nest decodifica los headers (string, objeto JSON o array si se repiten): se vuelven a texto para reenviarlos. */
function headerToWire(v: unknown): string | string[] {
  if (Array.isArray(v)) return v.map((x) => headerToWire(x) as string);
  if (Buffer.isBuffer(v) || typeof v === 'string') return v.toString();
  if (v === null || v === undefined) return '';
  return typeof v === 'object' ? JSON.stringify(v) : String(v);
}

/**
 * Publica el mensaje ORIGINAL (mismo value en bytes, misma clave y headers) en <topic>.dlq,
 * con el motivo del error y el origen en headers (documento §08, decisión 2).
 */
@Injectable()
export class DlqPublisher {
  private readonly logger = new Logger('DLQ');
  private readonly instance = hostname();

  constructor(
    @Inject(KAFKA_PRODUCER) private readonly client: ClientKafka,
    @Inject(TOOLKIT_OPTIONS) private readonly opts: ToolkitOptions,
  ) {}

  async send(ctx: KafkaCtx, reason: DlqReason, error: string, attempts: number): Promise<string> {
    const msg = ctx.getMessage();
    const dlqTopic = `${ctx.getTopic()}.dlq`;
    const original = Object.fromEntries(
      Object.entries(msg.headers ?? {}).map(([k, v]) => [k, headerToWire(v)]),
    );
    await lastValueFrom(
      this.client.emit(dlqTopic, {
        key: keyOf(ctx),
        value: msg.value, // Buffer original (parser.keepBinary)
        headers: {
          ...original,
          [DLQ_HEADERS.ERROR]: error.slice(0, 1000),
          [DLQ_HEADERS.REASON]: reason,
          [DLQ_HEADERS.SERVICE]: this.opts.service,
          [DLQ_HEADERS.INSTANCE]: this.instance,
          [DLQ_HEADERS.ATTEMPTS]: String(attempts),
          [DLQ_HEADERS.ORIGINAL_TOPIC]: ctx.getTopic(),
          [DLQ_HEADERS.ORIGINAL_PARTITION]: String(ctx.getPartition()),
          [DLQ_HEADERS.ORIGINAL_OFFSET]: msg.offset,
          [DLQ_HEADERS.FAILED_AT]: new Date().toISOString(),
        },
      }),
      { defaultValue: undefined },
    );
    this.logger.error(`${where(ctx)} → ${dlqTopic} (${reason}, intentos=${attempts}): ${error}`);
    return dlqTopic;
  }
}
