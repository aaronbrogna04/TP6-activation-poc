import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { randomUUID } from 'node:crypto';
import { ACTIVATION_STATUS, createEvent, SOURCES, TOPICS } from '@poc/contracts';
import { EventPublisher, Tx } from '@poc/kafka-toolkit';
import { Activation, HistoryEntry } from './activation.schema';
import { toView } from './activation.view';
import { ActivationsGateway } from './activations.gateway';
import { CreateActivationDto } from './create-activation.dto';
import { historyOf, ResultEvent, toStep } from './history';
import { decide } from './saga';

const OPEN = [ACTIVATION_STATUS.PENDING, ACTIVATION_STATUS.IN_PROGRESS];

@Injectable()
export class ActivationsService {
  private readonly logger = new Logger(ActivationsService.name);

  constructor(
    @InjectModel(Activation.name) private readonly activations: Model<Activation>,
    private readonly publisher: EventPublisher,
    private readonly gateway: ActivationsGateway,
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
    const entry = historyOf(event);

    const created = await this.activations.create({
      _id: activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      status: ACTIVATION_STATUS.PENDING,
      simulateFailure,
      steps: {},
      history: [entry],
    });

    // TODO(fase 4 · outbox transaccional): publicación directa después de guardar.
    // RIESGO: si el proceso se cae entre el insert de arriba y este publish, la activación queda
    // PENDING para siempre y ActivationRequested se pierde (nadie más lo va a publicar).
    // En la fase 4 el evento se guarda en la colección `outbox` en la misma transacción de Mongo
    // y un proceso aparte lo publica cada 500 ms (documento §08, decisión 3).
    await this.publisher.publish(TOPICS.ACTIVATION_REQUESTED, event);

    this.gateway.emitUpdate(toView(created.toObject()), entry);
    this.logger.log(`[${activationId}] activación creada (customer=${dto.customerId}, plan=${dto.planId}, simulateFailure=${simulateFailure})`);
    return { activationId, status: ACTIVATION_STATUS.PENDING };
  }

  /** RF-09: estado actual, resultados parciales (steps) e historial completo de eventos. */
  async findOne(id: string) {
    const doc = await this.activations.findById(id).lean();
    if (!doc) throw new NotFoundException(`activación ${id} no existe`);
    return toView(doc);
  }

  /** Activaciones recientes (lista seleccionable de la demo-ui), la más nueva primero. */
  async findRecent(limit: number) {
    const docs = await this.activations.find().sort({ createdAt: -1 }).limit(limit).lean();
    return docs.map(toView);
  }

  /**
   * Agregador de la saga (RF-04, RF-05, RF-06).
   * Corre dentro de la transacción de EventConsumer (processed_events + estos updates): un resultado repetido
   * no vuelve a tocar la activación. Los updates son condicionales sobre el mismo documento, así que dos
   * resultados concurrentes (o instancias escaladas) chocan y Mongo reintenta uno con el estado fresco.
   * El evento final y los mensajes WebSocket (RF-08) salen recién después del commit.
   */
  async applyResult(event: ResultEvent, tx: Tx): Promise<void> {
    const { session } = tx;
    const id = event.correlationId;
    const mapped = toStep(event);

    // 1) Registrar el resultado solo si la activación sigue abierta.
    const entry = historyOf(event);
    const open = mapped
      ? await this.activations
          .findOneAndUpdate(
            { _id: id, status: { $in: OPEN } },
            {
              $set: { [`steps.${mapped.step}`]: mapped.result, status: ACTIVATION_STATUS.IN_PROGRESS },
              $push: { history: entry },
            },
            { new: true, session },
          )
          .lean()
      : null;

    if (!open) {
      // Activación final (resultado tardío → ignored) o evento sin efecto en la saga
      // (BillingAccountCancelled, la compensación): solo queda en el historial.
      const late = historyOf(event, mapped !== null);
      const doc = await this.activations
        .findOneAndUpdate({ _id: id }, { $push: { history: late } }, { new: true, session })
        .lean();
      if (!doc) {
        this.logger.warn(`[${id}] ${event.eventType} para una activación desconocida; se descarta`);
        return;
      }
      if (late.ignored) {
        this.logger.log(`[${id}] ${event.eventType} llegó con la activación ya final; solo se registra en history`);
      }
      tx.afterCommit(() => this.emit(doc, late));
      return;
    }

    tx.afterCommit(() => this.emit(open, entry));

    // 2) Decidir.
    const decision = decide(open);
    if (decision.kind === 'WAIT') {
      this.logger.log(`[${id}] ${mapped!.step}=${mapped!.result.status}; esperando el otro resultado`);
      return;
    }

    // 3) Transición final: condicional, en la misma transacción.
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

    const outcomeEntry = historyOf(outcome);
    const won = await this.activations
      .findOneAndUpdate(
        { _id: id, status: ACTIVATION_STATUS.IN_PROGRESS },
        { $set: { status: final }, $push: { history: outcomeEntry } },
        { new: true, session },
      )
      .lean();
    if (!won) return; // otra entrega/instancia ya cerró la activación

    // TODO(fase 4 · outbox): EventConsumer publica después del commit de Mongo. Si se cae entre los dos,
    // el estado queda final pero ActivationCompleted/ActivationFailed no se publica.
    tx.publish(TOPICS.ACTIVATION_EVENTS, outcome);
    tx.afterCommit(() => {
      this.emit(won, outcomeEntry);
      this.logger.log(`[${id}] activación ${final}`);
    });
  }

  private emit(doc: Activation, entry: HistoryEntry): void {
    this.gateway.emitUpdate(toView(doc), entry);
  }
}
