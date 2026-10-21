import { SagaDefinition, SagaStatus } from 'saga-core';
import {
  AccountVerifyReply,
  validateWith,
} from './vehicle-detection.validators';

export enum VehicleSagaSteps {
  AccountVerification = 'AccountVerification',
  VehicleRegistration = 'VehicleRegistration',
  MoneyReservation = 'MoneyReservation',
  VehicleTagActivation = 'VehicleTagActivation',
}

export enum VehicleSaga {
  VehicleDetection = 'VehicleDetection',
}

// Keyed by status, not by saga type - a saga type has exactly one
// definition, but that definition needs one entry per step it can be in.
export const VEHICLE_DETECTIONS_TRANSITIONS: SagaDefinition = {
  [SagaStatus.NEW]: {
    successStatus: VehicleSagaSteps.AccountVerification,
    failureStatus: SagaStatus.FAILED,
    command: {
      exchange: 'account.commands', // TODO: confirm once account-service's RabbitMQ consumer exists
      routingKey: 'account.verify',
      validateReply: validateWith(AccountVerifyReply),
    },
  },
  [VehicleSagaSteps.AccountVerification]: {
    successStatus: VehicleSagaSteps.VehicleRegistration,
    failureStatus: SagaStatus.FAILED,
    // TODO: command + compensate once the vehicle-registration participant is defined
  },
  [VehicleSagaSteps.VehicleRegistration]: {
    successStatus: VehicleSagaSteps.MoneyReservation,
    failureStatus: SagaStatus.FAILED,
    // TODO: command + compensate once the money-reservation participant is defined
  },
  [VehicleSagaSteps.MoneyReservation]: {
    successStatus: VehicleSagaSteps.VehicleTagActivation,
    failureStatus: SagaStatus.FAILED,
    // TODO: command + compensate once the tag-activation participant is defined
  },
  [VehicleSagaSteps.VehicleTagActivation]: {
    successStatus: SagaStatus.COMPLETED,
    failureStatus: SagaStatus.FAILED,
    // TODO: command + compensate once the tag-activation participant is defined
  },
};
