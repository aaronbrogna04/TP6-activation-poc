import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { NotificationsModule } from './notifications/notifications.module';

/** notification-service no publica eventos: no necesita el productor de KafkaToolkitModule. */
@Module({
  imports: [
    MongooseModule.forRoot(
      process.env.MONGO_URI ?? 'mongodb://localhost:27017/notification?replicaSet=rs0&directConnection=true',
    ),
    NotificationsModule,
  ],
})
export class AppModule {}
