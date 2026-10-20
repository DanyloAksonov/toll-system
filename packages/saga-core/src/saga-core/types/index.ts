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
  // Picks what this step's participant needs out of the saga's accumulated
  // payload. Omitted -> the participant receives the whole payload.
  buildPayload?: (
    sagaPayload: Record<string, unknown>,
  ) => Record<string, unknown>;
  // Checks the `data` of this command's success reply before the saga moves
  // on. Return an error message to treat the reply as a failure, or null if
  // the data is valid.
  validateReply?: (
    data: Record<string, unknown>,
  ) => string | null | Promise<string | null>;
};

// What every participant receives on the wire. `sagaId` is what it echoes
// back in its reply; `messageId` is stable across relay retries, so it can
// be used as the participant's inbox dedup key.
export type SagaCommandMessage<
  TData extends Record<string, unknown> = Record<string, unknown>,
> = {
  sagaId: string;
  messageId: string;
  type: string; // the command's routingKey
  data: TData;
};

// What every participant sends back to the orchestrator.
export type SagaReply<
  TData extends Record<string, unknown> = Record<string, unknown>,
> = {
  sagaId: string; // from the SagaCommandMessage being answered
  messageId: string; // unique per reply, used for inbox dedup
  type: string; // the answered command's routingKey
  outcome: 'success' | 'failure';
  data?: TData; // merged into the saga payload on success
  failureReason?: string;
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
