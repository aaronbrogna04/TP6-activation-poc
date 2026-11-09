import { IsIn, IsNotEmpty, IsOptional, IsString, MaxLength } from 'class-validator';
import { SIMULATE_FAILURE_VALUES, SimulateFailure } from '@poc/contracts';

export class CreateActivationDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(64)
  customerId: string;

  @IsString()
  @IsNotEmpty()
  @MaxLength(64)
  planId: string;

  @IsOptional()
  @IsIn(SIMULATE_FAILURE_VALUES)
  simulateFailure?: SimulateFailure;

  @IsOptional()
  @IsString()
  @MaxLength(32)
  channel?: string;
}
