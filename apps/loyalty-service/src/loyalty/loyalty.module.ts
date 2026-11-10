import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { LoyaltyAccount, LoyaltyAccountSchema } from './loyalty-account.schema';
import { LoyaltyConsumer } from './loyalty.consumer';
import { LoyaltyService } from './loyalty.service';

@Module({
  imports: [MongooseModule.forFeature([{ name: LoyaltyAccount.name, schema: LoyaltyAccountSchema }])],
  controllers: [LoyaltyConsumer],
  providers: [LoyaltyService],
})
export class LoyaltyModule {}
