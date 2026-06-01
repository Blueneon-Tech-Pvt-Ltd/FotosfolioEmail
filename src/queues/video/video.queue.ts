import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Queue } from 'bullmq';
import { VideoProcessJob } from './video.job.interface';

@Injectable()
export class VideoProcessQueue implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(VideoProcessQueue.name);
  private queue!: Queue<VideoProcessJob>;

  constructor(private readonly configService: ConfigService) {}

  onModuleInit(): void {
    const connection = {
      host: this.configService.get<string>('REDIS_HOST') ?? 'localhost',
      port: Number(this.configService.get<string>('REDIS_PORT') ?? 6379),
      password: this.configService.get<string>('REDIS_PASSWORD') ?? undefined,
    };

    this.queue = new Queue<VideoProcessJob>('video-process', {
      connection,
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000,
        },
        removeOnComplete: true,
        removeOnFail: true,
      },
    });

    this.logger.log('Video process queue initialized');
  }

  async addVideoProcessJob(data: VideoProcessJob): Promise<void> {
    if (!this.queue) {
      throw new Error('Video process queue is not initialized');
    }

    await this.queue.add('video-process', data, {
      jobId: data.uploadId,
    });
  }

  async onModuleDestroy(): Promise<void> {
    if (this.queue) {
      await this.queue.close();
    }
  }
}
