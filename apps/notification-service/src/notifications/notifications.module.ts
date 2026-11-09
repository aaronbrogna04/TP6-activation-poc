import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { Mailer } from './mailer';
import { Notification, NotificationSchema } from './notification.schema';
import { NotificationsConsumer } from './notifications.consumer';
import { NotificationsService } from './notifications.service';

@Module({
  imports: [MongooseModule.forFeature([{ name: Notification.name, schema: NotificationSchema }])],
  controllers: [NotificationsConsumer],
  providers: [NotificationsService, Mailer],
})
export class NotificationsModule {}
