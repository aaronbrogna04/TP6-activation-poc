import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ActivationMarker, ActivationMarkerSchema } from './activation-marker.schema';
import { BillingAccount, BillingAccountSchema } from './billing-account.schema';
import { BillingConsumer } from './billing.consumer';
import { BillingService } from './billing.service';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: BillingAccount.name, schema: BillingAccountSchema },
      { name: ActivationMarker.name, schema: ActivationMarkerSchema },
    ]),
  ],
  controllers: [BillingConsumer],
  providers: [BillingService],
})
export class BillingModule {}
