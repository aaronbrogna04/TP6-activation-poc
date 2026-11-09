import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { BillingAccount, BillingAccountSchema } from './billing-account.schema';
import { BillingConsumer } from './billing.consumer';
import { BillingService } from './billing.service';

@Module({
  imports: [MongooseModule.forFeature([{ name: BillingAccount.name, schema: BillingAccountSchema }])],
  controllers: [BillingConsumer],
  providers: [BillingService],
})
export class BillingModule {}
