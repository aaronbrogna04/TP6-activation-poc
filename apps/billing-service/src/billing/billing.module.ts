import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ActivationFailure, ActivationFailureSchema } from './activation-failure.schema';
import { BillingAccount, BillingAccountSchema } from './billing-account.schema';
import { BillingConsumer } from './billing.consumer';
import { BillingService } from './billing.service';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: BillingAccount.name, schema: BillingAccountSchema },
      { name: ActivationFailure.name, schema: ActivationFailureSchema },
    ]),
  ],
  controllers: [BillingConsumer],
  providers: [BillingService],
})
export class BillingModule {}
