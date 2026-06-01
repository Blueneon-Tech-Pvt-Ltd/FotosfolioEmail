import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ApiModule } from './api/api.module';
import { WorkersModule } from './workersqueue/workerqueue.module';
import { SenderModule } from './sender/sender.module';
import { HealthModule } from './health/health.module';
import { CronJobsModule } from './cron-jobs/cron-jobs.module';
import redisConfig from './config/redis.config';
import resendConfig from './config/resend.config';
import { LoggerModule } from './logger/logger.module';

@Module({
  imports: [
    // Global configuration
    ConfigModule.forRoot({
      load: [redisConfig, resendConfig],
      isGlobal: true,
      envFilePath: '.env',
    }),

    LoggerModule,
    ApiModule,
    WorkersModule,
    SenderModule,
    HealthModule,
    CronJobsModule,
  ],
})
export class AppModule {}
