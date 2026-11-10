import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { ActivationEvent } from '@poc/contracts';
import { Tx } from '@poc/kafka-toolkit';
import { LoyaltyAccount } from './loyalty-account.schema';

/** Puntos por activación completada. Las fallidas se cuentan pero no suman. */
export const POINTS_PER_ACTIVATION = Number(process.env.LOYALTY_POINTS_PER_ACTIVATION ?? 100);
const SUMMARY_DEBOUNCE_MS = 2000;

@Injectable()
export class LoyaltyService implements OnModuleDestroy {
  private readonly logger = new Logger(LoyaltyService.name);
  private readonly run = { completed: 0, failed: 0, oldest: '', newest: '' };
  private timer?: NodeJS.Timeout;

  constructor(@InjectModel(LoyaltyAccount.name) private readonly accounts: Model<LoyaltyAccount>) {}

  /** En la transacción de EventConsumer: processed_events evita sumar dos veces el mismo evento. */
  async apply(event: ActivationEvent, tx: Tx): Promise<void> {
    const completed = event.eventType === 'ActivationCompleted';
    await this.accounts.updateOne(
      { customerId: event.customerId },
      {
        $inc: completed ? { points: POINTS_PER_ACTIVATION, completedActivations: 1 } : { failedActivations: 1 },
        ...(completed ? { $addToSet: { plans: event.payload.planId } } : {}),
        $setOnInsert: { customerId: event.customerId },
      },
      { upsert: true, session: tx.session },
    );
    this.logger.log(
      `[${event.correlationId}] ${event.eventType}: ${event.customerId} ${completed ? `+${POINTS_PER_ACTIVATION} pts` : 'sin puntos (activación fallida)'} (evento del ${event.occurredAt})`,
    );
    tx.afterCommit(() => this.track(completed, event.occurredAt));
  }

  /** Resumen cuando se termina de leer lo pendiente (al arrancar: todo el historial). */
  private track(completed: boolean, occurredAt: string): void {
    if (completed) this.run.completed++;
    else this.run.failed++;
    if (!this.run.oldest || occurredAt < this.run.oldest) this.run.oldest = occurredAt;
    if (occurredAt > this.run.newest) this.run.newest = occurredAt;
    clearTimeout(this.timer);
    this.timer = setTimeout(() => void this.logSummary(), SUMMARY_DEBOUNCE_MS);
  }

  private async logSummary(): Promise<void> {
    const { completed, failed, oldest, newest } = this.run;
    const top = await this.accounts.find().sort({ points: -1, customerId: 1 }).lean();
    const totalPoints = top.reduce((acc, a) => acc + a.points, 0);
    this.logger.log(
      `RESUMEN: procesados ${completed + failed} eventos de activation.events (${completed} completadas, ${failed} fallidas) ` +
        `entre ${oldest} y ${newest} → ${top.length} clientes, ${totalPoints} puntos`,
    );
    for (const a of top) {
      this.logger.log(
        `  ${a.customerId.padEnd(12)} ${String(a.points).padStart(5)} pts · ${a.completedActivations} completadas · ${a.failedActivations} fallidas · planes: ${a.plans.join(', ') || '—'}`,
      );
    }
    Object.assign(this.run, { completed: 0, failed: 0, oldest: '', newest: '' });
  }

  onModuleDestroy(): void {
    clearTimeout(this.timer);
  }
}
