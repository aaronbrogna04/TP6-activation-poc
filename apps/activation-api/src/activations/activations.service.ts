import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { randomUUID } from 'node:crypto';
import {
  ACTIVATION_STATUS,
  BillingEvent,
  createEvent,
  ProvisioningEvent,
  SOURCES,
  TOPICS,
} from '@poc/contracts';
import { EventPublisher } from '@poc/kafka-toolkit';
import { Activation, HistoryEntry, StepResult } from './activation.schema';
import { CreateActivationDto } from './create-activation.dto';
import { decide, StepName } from './saga';

type ResultEvent = BillingEvent | ProvisioningEvent;

/** Traduce un evento de resultado al paso de la saga que representa (o null si no afecta la saga). */
function toStep(event: ResultEvent): { step: StepName; result: StepResult } | null {
  const at = event.occurredAt;
  switch (event.eventType) {
    case 'BillingAccountCreated':
      return { step: 'billing', result: { status: 'OK', at, eventType: event.eventType } };
    case 'BillingFailed':
      return { step: 'billing', result: { status: 'FAILED', at, eventType: event.eventType, reason: event.payload.reason } };
    case 'ProvisioningCompleted':
      return { step: 'provisioning', result: { status: 'OK', at, eventType: event.eventType } };
    case 'ProvisioningFailed':
      return {
        step: 'provisioning',
        result: { status: 'FAILED', at, eventType: event.eventType, reason: event.payload.reason },
      };
    default:
      // BillingAccountCancelled (compensación): solo se registra en el historial.
      return null;
  }
}

const historyOf = (e: { eventId: string; eventType: string; source: string; occurredAt: string }): HistoryEntry => ({
  eventId: e.eventId,
  eventType: e.eventType,
  source: e.source,
  at: e.occurredAt,
});

@Injectable()
export class ActivationsService {
  private readonly logger = new Logger(ActivationsService.name);

  constructor(
    @InjectModel(Activation.name) private readonly activations: Model<Activation>,
    private readonly publisher: EventPublisher,
  ) {}

  /** RF-02 / RF-03: guarda PENDING, publica ActivationRequested y responde sin esperar a nadie. */
  async create(dto: CreateActivationDto) {
    const activationId = `act-${randomUUID().replace(/-/g, '').slice(0, 12)}`;
    const simulateFailure = dto.simulateFailure ?? 'none';
    const event = createEvent({
      eventType: 'ActivationRequested',
      correlationId: activationId,
      customerId: dto.customerId,
      source: SOURCES.ACTIVATION_API,
      payload: { planId: dto.planId, channel: dto.channel ?? 'web', simulateFailure },
    });

    await this.activations.create({
      _id: activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      status: ACTIVATION_STATUS.PENDING,
      simulateFailure,
      steps: {},
      history: [historyOf(event)],
    });

    // TODO(fase 4 · outbox transaccional): publicación directa después de guardar.
    // RIESGO: si el proceso se cae entre el insert de arriba y este publish, la activación queda
    // PENDING para siempre y ActivationRequested se pierde (nadie más lo va a publicar).
    // En la fase 4 el evento se guarda en la colección `outbox` en la misma transacción de Mongo
    // y un proceso aparte lo publica cada 500 ms (documento §08, decisión 3).
    await this.publisher.publish(TOPICS.ACTIVATION_REQUESTED, event);

    this.logger.log(`[${activationId}] activación creada (customer=${dto.customerId}, plan=${dto.planId}, simulateFailure=${simulateFailure})`);
    return { activationId, status: ACTIVATION_STATUS.PENDING };
  }

  /** RF-09 */
  async findOne(id: string) {
    const doc = await this.activations.findById(id).lean();
    if (!doc) throw new NotFoundException(`activación ${id} no existe`);
    const { _id, ...rest } = doc;
    return { activationId: _id, ...rest };
  }

  /**
   * Agregador de la saga (RF-04, RF-05, RF-06).
   * Updates atómicos y condicionales: seguro ante resultados concurrentes o instancias escaladas.
   * TODO(fase 3): idempotencia por eventId en processed_events, en la misma transacción.
   */
  async applyResult(event: ResultEvent): Promise<void> {
    const id = event.correlationId;
    const mapped = toStep(event);
    const entry = historyOf(event);

    // 1) Registrar el resultado solo si la activación sigue abierta.
    const open = mapped
      ? await this.activations
          .findOneAndUpdate(
            { _id: id, status: { $in: [ACTIVATION_STATUS.PENDING, ACTIVATION_STATUS.IN_PROGRESS] } },
            {
              $set: { [`steps.${mapped.step}`]: mapped.result, status: ACTIVATION_STATUS.IN_PROGRESS },
              $push: { history: entry },
            },
            { new: true },
          )
          .lean()
      : null;

    if (!open) {
      // Activación final (o evento sin efecto en la saga): solo queda en el historial.
      const res = await this.activations.updateOne(
        { _id: id },
        { $push: { history: { ...entry, ignored: mapped !== null } } },
      );
      if (res.matchedCount === 0) {
        this.logger.warn(`[${id}] ${event.eventType} para una activación desconocida; se descarta`);
      } else if (mapped) {
        this.logger.log(`[${id}] ${event.eventType} llegó con la activación ya final; solo se registra en history`);
      }
      return;
    }

    // 2) Decidir.
    const decision = decide(open);
    if (decision.kind === 'WAIT') {
      this.logger.log(`[${id}] ${mapped!.step}=${mapped!.result.status}; esperando el otro resultado`);
      return;
    }

    // 3) Transición final: solo publica quien gana el update condicional (nunca dos veces).
    const final = decision.kind === 'COMPLETE' ? ACTIVATION_STATUS.ACTIVE : ACTIVATION_STATUS.FAILED;
    const outcome =
      decision.kind === 'COMPLETE'
        ? createEvent({
            eventType: 'ActivationCompleted',
            correlationId: id,
            customerId: open.customerId,
            source: SOURCES.ACTIVATION_API,
            payload: { planId: open.planId },
          })
        : createEvent({
            eventType: 'ActivationFailed',
            correlationId: id,
            customerId: open.customerId,
            source: SOURCES.ACTIVATION_API,
            payload: { planId: open.planId, failedStep: decision.failedStep, reason: decision.reason },
          });

    const won = await this.activations.findOneAndUpdate(
      { _id: id, status: ACTIVATION_STATUS.IN_PROGRESS },
      { $set: { status: final }, $push: { history: historyOf(outcome) } },
    );
    if (!won) return; // otra entrega/instancia ya cerró la activación

    // TODO(fase 4 · outbox): mismo riesgo que en create(): si se cae acá, el estado queda final
    // pero ActivationCompleted/ActivationFailed no se publica.
    await this.publisher.publish(TOPICS.ACTIVATION_EVENTS, outcome);
    this.logger.log(`[${id}] activación ${final}`);
  }
}
