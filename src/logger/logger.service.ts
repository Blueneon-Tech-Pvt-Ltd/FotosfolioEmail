import { Injectable, LoggerService, Optional } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import pino, { Logger, LoggerOptions } from 'pino';

@Injectable()
export class AppLoggerService implements LoggerService {
  private pino: Logger;

  constructor(@Optional() private readonly configService?: ConfigService) {
    const nodeEnv = process.env.NODE_ENV || 'development';

    const loggerConfig: LoggerOptions = {
      level: 'info',
    };

    // Pretty printing in development
    if (nodeEnv === 'development') {
      loggerConfig.transport = {
        target: 'pino-pretty',
        options: {
          colorize: true,
          translateTime: 'HH:MM:ss',
          ignore: 'pid,hostname',
        },
      };
    }

    this.pino = pino(loggerConfig);
  }

  log(message: any, context?: string): void {
    this.pino.info(this.formatMessage(message, context));
  }

  error(message: any, trace?: string, context?: string): void {
    if (trace) {
      this.pino.error({ trace }, this.formatMessage(message, context));
    } else {
      this.pino.error(this.formatMessage(message, context));
    }
  }

  warn(message: any, context?: string): void {
    this.pino.warn(this.formatMessage(message, context));
  }

  debug(message: any, context?: string): void {
    this.pino.debug(this.formatMessage(message, context));
  }

  verbose(message: any, context?: string): void {
    this.pino.trace(this.formatMessage(message, context));
  }

  private formatMessage(message: any, context?: string) {
    return context ? `[${context}] ${message}` : message;
  }

  // Email-specific logging
  logEmailSent(emailType: string, recipient: string) {
    this.pino.info({
      event: 'email_sent',
      emailType,
      recipient,
      timestamp: new Date().toISOString(),
    });
  }

  logEmailFailed(emailType: string, recipient: string, error: string) {
    this.pino.error({
      event: 'email_failed',
      emailType,
      recipient,
      error,
      timestamp: new Date().toISOString(),
    });
  }

  logQueueJob(queueName: string, jobId: string, status: string) {
    this.pino.info({
      event: 'queue_job',
      queueName,
      jobId,
      status,
      timestamp: new Date().toISOString(),
    });
  }
}
