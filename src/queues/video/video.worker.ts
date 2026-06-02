import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Job, Worker } from 'bullmq';
import { VideoProcessJob } from './video.job.interface';
import { VideoProcessor } from './video.processor';

@Injectable()
export class VideoProcessWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(VideoProcessWorker.name);
  private worker!: Worker<VideoProcessJob>;

  constructor(
    private readonly configService: ConfigService,
    private readonly videoProcessor: VideoProcessor,
  ) {}

  onModuleInit(): void {
    const connection = {
      host: this.configService.get<string>('REDIS_HOST') ?? 'localhost',
      port: Number(this.configService.get<string>('REDIS_PORT') ?? 6379),
      password: this.configService.get<string>('REDIS_PASSWORD') ?? undefined,
    };

    this.worker = new Worker<VideoProcessJob>(
      'video-process',
      async (job: Job<VideoProcessJob>) => this.videoProcessor.process(job),
      {
        connection,
        concurrency: 5,
      },
    );

    this.worker.on('active', (job) => {
      this.logger.log(
        `Video job active for ${job.data.key} (jobId: ${job.id}, attempts: ${job.attemptsMade + 1})`,
      );
    });

    this.worker.on('completed', (job) => {
      this.logger.log(`Video job completed for ${job.data.key}`);
    });

    this.worker.on('drained', () => {
      this.logger.log('Video process worker drained; no waiting jobs');
    });

    this.worker.on('error', (error) => {
      this.logger.error(`Video process worker error: ${error.message}`, error.stack);
    });

    this.worker.on('failed', (job, error) => {
      this.logger.error(
        `Video job failed for ${job?.data.key ?? 'unknown key'}: ${error.message}`,
      );
    });

    this.worker.on('paused', () => {
      this.logger.warn('Video process worker paused');
    });

    this.worker.on('resumed', () => {
      this.logger.log('Video process worker resumed');
    });

    this.worker.on('stalled', (jobId) => {
      this.logger.warn(`Video job stalled (jobId: ${jobId})`);
    });

    this.worker.on('closed', () => {
      this.logger.warn('Video process worker closed');
    });

    this.worker.on('closing', () => {
      this.logger.warn('Video process worker closing');
    });

    this.logger.log('Video process worker initialized');
  }

  async onModuleDestroy(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
    }
  }
}
