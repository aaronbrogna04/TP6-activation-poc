/** Solo para la demo: indica a billing o provisioning que deben fallar. */
export const SIMULATE_FAILURE_VALUES = ['none', 'billing', 'provisioning'] as const;
export type SimulateFailure = (typeof SIMULATE_FAILURE_VALUES)[number];

export const ACTIVATION_STATUS = {
  PENDING: 'PENDING',
  IN_PROGRESS: 'IN_PROGRESS',
  ACTIVE: 'ACTIVE',
  FAILED: 'FAILED',
} as const;
export type ActivationStatus = (typeof ACTIVATION_STATUS)[keyof typeof ACTIVATION_STATUS];

export type StepStatus = 'OK' | 'FAILED';

export const SOURCES = {
  ACTIVATION_API: 'activation-api',
  BILLING: 'billing-service',
  PROVISIONING: 'provisioning-service',
} as const;
