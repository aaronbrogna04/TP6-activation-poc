import { Logger } from '@nestjs/common';
import { OnGatewayConnection, OnGatewayDisconnect, WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import type { Server, Socket } from 'socket.io';
import { HistoryEntry } from './activation.schema';
import { ActivationView } from './activation.view';

/** Nombre del mensaje WebSocket que recibe la demo-ui. */
export const WS_ACTIVATION_UPDATED = 'activation.updated';

/** Mensaje que se empuja a la UI por cada evento o cambio de estado de una activación. */
export interface ActivationUpdatedMessage {
  /** = activationId: la UI agrupa la timeline por este campo. */
  correlationId: string;
  /** Entrada de historial que provocó el cambio (evento recibido o publicado). */
  event: HistoryEntry;
  /** Foto completa de la activación después del cambio (mismo formato que GET /activations/:id). */
  activation: ActivationView;
}

const corsOrigin = (process.env.CORS_ORIGIN ?? 'http://localhost:5173').split(',');

/**
 * RF-08: empuja cada cambio a la UI (socket.io, mismo puerto que la API REST).
 * Se emite a todos los clientes conectados: la UI muestra la lista de recientes y agrupa cada
 * mensaje por correlationId en la timeline de esa activación.
 */
@WebSocketGateway({ cors: { origin: corsOrigin } })
export class ActivationsGateway implements OnGatewayConnection, OnGatewayDisconnect {
  private readonly logger = new Logger(ActivationsGateway.name);

  @WebSocketServer()
  private readonly server: Server;

  handleConnection(client: Socket): void {
    this.logger.log(`cliente WS conectado (${client.id}, total=${this.server.engine.clientsCount})`);
  }

  handleDisconnect(client: Socket): void {
    this.logger.log(`cliente WS desconectado (${client.id})`);
  }

  emitUpdate(activation: ActivationView, event: HistoryEntry): void {
    const message: ActivationUpdatedMessage = { correlationId: activation.activationId, event, activation };
    this.server.emit(WS_ACTIVATION_UPDATED, message);
    this.logger.log(`[${message.correlationId}] ${event.eventType} → WS (status=${activation.status})`);
  }
}
