export enum SagaStatus {
  NEW = 'NEW',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

export type StartSagaParams = {
  type: string;
  idempotencyKey: string;
  payload: string;
};

export type SagaCommand = {
  exchange: string;
  routingKey: string;
};

export type SagaTransition = {
  successStatus: string;
  failureStatus?: string;
  command?: SagaCommand;
  compensate?: SagaCommand;
};

// Keyed by status (e.g. SagaStatus.NEW, 'AccountVerification', ...)
export type SagaDefinition = Record<string, SagaTransition>;

// Keyed by saga type (e.g. 'RegisterVehicle', 'VehicleDetection')
export type SagaDefinitions = Record<string, SagaDefinition>;

export const SAGA_DEFINITIONS = 'SAGA_DEFINITIONS';
