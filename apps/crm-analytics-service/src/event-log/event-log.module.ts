import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { EventLogConsumer } from './event-log.consumer';
import { EventLogEntry, EventLogSchema } from './event-log.schema';
import { EventLogService } from './event-log.service';

@Module({
  imports: [MongooseModule.forFeature([{ name: EventLogEntry.name, schema: EventLogSchema }])],
  controllers: [EventLogConsumer],
  providers: [EventLogService],
})
export class EventLogModule {}
