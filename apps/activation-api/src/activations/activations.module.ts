import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { Activation, ActivationSchema } from './activation.schema';
import { ActivationsController } from './activations.controller';
import { ActivationsGateway } from './activations.gateway';
import { ActivationsService } from './activations.service';
import { ResultsConsumer } from './results.consumer';

@Module({
  imports: [MongooseModule.forFeature([{ name: Activation.name, schema: ActivationSchema }])],
  controllers: [ActivationsController, ResultsConsumer],
  providers: [ActivationsService, ActivationsGateway],
})
export class ActivationsModule {}
