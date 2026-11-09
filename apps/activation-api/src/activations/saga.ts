import { ACTIVATION_STATUS, ActivationStatus } from '@poc/contracts';
import { StepResult, Steps } from './activation.schema';

export type StepName = 'billing' | 'provisioning';

export type SagaDecision =
  | { kind: 'WAIT' }
  | { kind: 'COMPLETE' }
  | { kind: 'FAIL'; failedStep: StepName; reason: string };

const FINAL: ActivationStatus[] = [ACTIVATION_STATUS.ACTIVE, ACTIVATION_STATUS.FAILED];

export const isFinal = (status: ActivationStatus): boolean => FINAL.includes(status);

/**
 * Regla de agregación (documento §06):
 * - con billing y provisioning OK → COMPLETE (ActivationCompleted)
 * - con el primer fallo → FAIL (ActivationFailed)
 * - si ya es final → WAIT (los resultados posteriores solo se registran en history)
 */
export function decide(state: { status: ActivationStatus; steps: Steps }): SagaDecision {
  if (isFinal(state.status)) return { kind: 'WAIT' };

  const failed = (Object.entries(state.steps) as [StepName, StepResult | undefined][])
    .filter(([, r]) => r?.status === 'FAILED')
    .sort(([, a], [, b]) => a!.at.localeCompare(b!.at));
  if (failed.length > 0) {
    const [failedStep, result] = failed[0];
    return { kind: 'FAIL', failedStep, reason: result!.reason ?? `${failedStep} falló` };
  }

  if (state.steps.billing?.status === 'OK' && state.steps.provisioning?.status === 'OK') {
    return { kind: 'COMPLETE' };
  }
  return { kind: 'WAIT' };
}
