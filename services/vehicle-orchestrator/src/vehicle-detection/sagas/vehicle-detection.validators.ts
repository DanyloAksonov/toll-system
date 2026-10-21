import { plainToInstance } from 'class-transformer';
import { IsUUID, validate } from 'class-validator';

type ClassType<T> = new () => T;

// Adapts a class-validator DTO to saga-core's `validateReply` hook: returns
// every constraint message joined into one failure reason, or null if valid.
export const validateWith =
  <T extends object>(dto: ClassType<T>) =>
  async (data: Record<string, unknown>): Promise<string | null> => {
    const errors = await validate(plainToInstance(dto, data));
    if (errors.length === 0) {
      return null;
    }

    return errors
      .flatMap((error) => Object.values(error.constraints ?? {}))
      .join('; ');
  };

// Reply `data` from account-service to the `account.verify` command.
export class AccountVerifyReply {
  @IsUUID()
  userId: string;
}
