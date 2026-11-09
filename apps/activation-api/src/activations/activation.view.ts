import { Activation } from './activation.schema';

/** Representación pública de una activación (REST y WebSocket): _id se expone como activationId. */
export type ActivationView = Omit<Activation, '_id'> & { activationId: string };

export function toView(doc: Activation): ActivationView {
  const { _id, ...rest } = doc;
  return { activationId: _id, ...rest };
}
