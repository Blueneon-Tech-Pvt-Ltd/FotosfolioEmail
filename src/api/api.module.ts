import { Module } from '@nestjs/common';
import { ApiIndexController } from './api-index.controller';
import { EmailController } from './email.controller';
import { WorkersModule } from '../workersqueue/workerqueue.module';
import { SenderModule } from '../sender/sender.module';
import { VideoQueueModule } from '../queues/video/video.queue.module';
import { VideoController } from './video.controller';

@Module({
  imports: [WorkersModule, SenderModule, VideoQueueModule],
  controllers: [ApiIndexController, EmailController, VideoController],
})
export class ApiModule {}