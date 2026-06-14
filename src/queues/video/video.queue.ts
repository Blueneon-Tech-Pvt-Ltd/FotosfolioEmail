import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Queue } from 'bullmq';
import { VideoProcessJob } from './video.job.interface';
import {
  getVideoQueueConnectionConfig,
  VIDEO_PROCESS_QUEUE_NAME,
} from './video-queue.config';

@Injectable()
export class VideoProcessQueue implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(VideoProcessQueue.name);
  private queue!: Queue<VideoProcessJob>;

  constructor(private readonly configService: ConfigService) {}

  onModuleInit(): void {
    const { connection, prefix, description } = getVideoQueueConnectionConfig(
      this.configService,
    );

    this.queue = new Queue<VideoProcessJob>(VIDEO_PROCESS_QUEUE_NAME, {
      connection,
      prefix,
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

    this.logger.log(`Video process queue initialized (${description})`);
  }

  async addVideoProcessJob(data: VideoProcessJob): Promise<void> {
    if (!this.queue) {
      throw new Error('Video process queue is not initialized');
    }

    const existingJob = await this.queue.getJob(data.uploadId);
    if (existingJob) {
      const state = await existingJob.getState();
      this.logger.warn(
        `Video process job already exists for uploadId ${data.uploadId} in state ${state}; key=${existingJob.data.key}`,
      );
    }

    const job = await this.queue.add(VIDEO_PROCESS_QUEUE_NAME, data, {
      jobId: data.uploadId,
    });

    const state = await job.getState();
    this.logger.log(
      `Video process job queued for ${data.key} (jobId: ${job.id}, state: ${state})`,
    );
  }

  async onModuleDestroy(): Promise<void> {
    if (this.queue) {
      await this.queue.close();
    }
  }
}
