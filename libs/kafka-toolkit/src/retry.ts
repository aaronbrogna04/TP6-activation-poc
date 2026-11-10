import { setTimeout as sleep } from 'node:timers/promises';

/** Documento §08, decisión 2: 3 reintentos con espera creciente de 1 s, 2 s y 4 s (4 intentos en total). */
export const RETRY_DELAYS_MS: readonly number[] = (process.env.RETRY_DELAYS_MS ?? '1000,2000,4000')
  .split(',')
  .map(Number);

export class RetriesExhaustedError extends Error {
  constructor(
    readonly attempts: number,
    readonly lastError: unknown,
  ) {
    super(errorMessage(lastError));
  }
}

export const errorMessage = (err: unknown): string =>
  err instanceof Error ? `${err.name}: ${err.message}` : String(err);

/**
 * Ejecuta fn; si lanza, espera 1 s, 2 s, 4 s entre intentos. Tras el último lanza RetriesExhaustedError.
 * shouldRetry permite cortar antes (errores que no son técnicos).
 */
export async function withRetries<T>(
  fn: (attempt: number) => Promise<T>,
  hooks: { onRetry?: (attempt: number, err: unknown, delayMs: number) => Promise<void> | void; shouldRetry?: (err: unknown) => boolean } = {},
): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn(attempt);
    } catch (err) {
      if (hooks.shouldRetry && !hooks.shouldRetry(err)) throw err;
      const delay = RETRY_DELAYS_MS[attempt - 1];
      if (delay === undefined) throw new RetriesExhaustedError(attempt, err);
      await hooks.onRetry?.(attempt, err, delay);
      await sleep(delay);
    }
  }
}
