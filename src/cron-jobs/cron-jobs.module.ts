import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { CronJobsController } from './cron-jobs.controller';
import { CronJobsService } from './cron-jobs.service';
import { WorkersModule } from '../workersqueue/workerqueue.module';

@Module({
  imports: [ConfigModule, WorkersModule],
  controllers: [CronJobsController],
  providers: [CronJobsService],
  exports: [CronJobsService],
})
export class CronJobsModule {}
