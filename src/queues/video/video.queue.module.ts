import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { VideoProcessQueue } from './video.queue';
import { VideoProcessWorker } from './video.worker';
import { VideoProcessor } from './video.processor';
import { VideoS3Service } from './video.s3.service';

@Module({
  imports: [ConfigModule],
  providers: [VideoProcessQueue, VideoProcessWorker, VideoProcessor, VideoS3Service],
  exports: [VideoProcessQueue],
})
export class VideoQueueModule {}
