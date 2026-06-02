import { ConfigService } from '@nestjs/config';
import type { RedisOptions } from 'ioredis';

export const VIDEO_PROCESS_QUEUE_NAME = 'video-process';

export interface VideoQueueConnectionConfig {
  connection: RedisOptions;
  prefix?: string;
  description: string;
}

export function getVideoQueueConnectionConfig(
  configService: ConfigService,
): VideoQueueConnectionConfig {
  const host = configService.get<string>('REDIS_HOST') ?? 'localhost';
  const port = Number(configService.get<string>('REDIS_PORT') ?? 6379);
  const db = Number(configService.get<string>('REDIS_DB') ?? 0);
  const prefix = configService.get<string>('BULLMQ_PREFIX') || undefined;
  const hasPassword = Boolean(configService.get<string>('REDIS_PASSWORD'));

  return {
    connection: {
      host,
      port,
      db,
      password: hasPassword
        ? configService.get<string>('REDIS_PASSWORD')
        : undefined,
    },
    prefix,
    description: `redis://${host}:${port}/${db}, queue=${VIDEO_PROCESS_QUEUE_NAME}, prefix=${prefix ?? 'bull'}`,
  };
}
