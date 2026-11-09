import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ProvisioningOrder, ProvisioningOrderSchema } from './provisioning-order.schema';
import { ProvisioningConsumer } from './provisioning.consumer';
import { ProvisioningService } from './provisioning.service';

@Module({
  imports: [MongooseModule.forFeature([{ name: ProvisioningOrder.name, schema: ProvisioningOrderSchema }])],
  controllers: [ProvisioningConsumer],
  providers: [ProvisioningService],
})
export class ProvisioningModule {}
