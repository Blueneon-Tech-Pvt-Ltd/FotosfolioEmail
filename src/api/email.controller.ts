import {
  Controller,
  Post,
  Get,
  Body,
  Param,
  Query,
  HttpCode,
  HttpStatus,
  Logger,
  ValidationPipe,
} from '@nestjs/common';
import { WorkerPoolService } from '../workersqueue/workerqueue-pool.service';
import { EmailSenderService } from '../sender/email-sender.service';
import { SendEmailDto, QueueStatsQueryDto } from './dtos/send-email.dto';
import { EmailCategory, EmailType } from '../types/email-types';

@Controller('emails')
export class EmailController {
  private readonly logger = new Logger(EmailController.name);

  constructor(
    private readonly workerPool: WorkerPoolService,
    private readonly emailSender: EmailSenderService,
  ) {}

  /**
   * POST /emails/send
   * Queue a single email for processing
   */
  @Post('send')
  @HttpCode(HttpStatus.ACCEPTED)
  async sendEmail(@Body(ValidationPipe) dto: SendEmailDto) {
    this.logger.log(
      `📬 Received request to queue ${dto.type} email (category: ${dto.category})`,
    );

    try {
      await this.workerPool.addJob(dto.category, dto.type, dto.payload);

      return {
        success: true,
        message: 'Email queued successfully',
        data: {
          category: dto.category,
          type: dto.type,
        },
      };
    } catch (error: any) {
      this.logger.error(`Failed to queue email: ${error.message}`);
      throw error;
    }
  }

  /**
   * POST /emails/send/account
   * Shortcut for account emails
   */
  @Post('send/account')
  @HttpCode(HttpStatus.ACCEPTED)
  async sendAccountEmail(@Body() body: { type: EmailType; payload: any }) {
    return this.sendEmail({
      category: EmailCategory.ACCOUNT,
      type: body.type,
      payload: body.payload,
    });
  }

  /**
   * POST /emails/send/subscription
   * Shortcut for subscription emails
   */
  @Post('send/subscription')
  @HttpCode(HttpStatus.ACCEPTED)
  async sendSubscriptionEmail(@Body() body: { type: EmailType; payload: any }) {
    return this.sendEmail({
      category: EmailCategory.SUBSCRIPTION,
      type: body.type,
      payload: body.payload,
    });
  }

  /**
   * POST /emails/send/security
   * Shortcut for security emails
   */
  @Post('send/security')
  @HttpCode(HttpStatus.ACCEPTED)
  async sendSecurityEmail(@Body() body: { type: EmailType; payload: any }) {
    return this.sendEmail({
      category: EmailCategory.SECURITY,
      type: body.type,
      payload: body.payload,
    });
  }

  /**
   * POST /emails/send/project
   * Shortcut for project emails
   */
  @Post('send/project')
  @HttpCode(HttpStatus.ACCEPTED)
  async sendProjectEmail(@Body() body: { type: EmailType; payload: any }) {
    return this.sendEmail({
      category: EmailCategory.PROJECT,
      type: body.type,
      payload: body.payload,
    });
  }

  /**
   * POST /emails/send/payment
   * Shortcut for payment emails
   */
  @Post('send/payment')
  @HttpCode(HttpStatus.ACCEPTED)
  async sendPaymentEmail(@Body() body: { type: EmailType; payload: any }) {
    return this.sendEmail({
      category: EmailCategory.PAYMENT,
      type: body.type,
      payload: body.payload,
    });
  }

  /**
   * POST /emails/send/storage
   * Shortcut for storage emails
   */
  @Post('send/storage')
  @HttpCode(HttpStatus.ACCEPTED)
  async sendStorageEmail(@Body() body: { type: EmailType; payload: any }) {
    return this.sendEmail({
      category: EmailCategory.STORAGE,
      type: body.type,
      payload: body.payload,
    });
  }

  /**
   * GET /emails/stats
   * Get queue statistics for all or specific category
   */
  @Get('stats')
  async getQueueStats(@Query() query: QueueStatsQueryDto) {
    if (query.category) {
      const stats = await this.workerPool.getQueueStats(query.category);
      return {
        success: true,
        data: {
          category: query.category,
          ...stats,
        },
      };
    }

    const allStats = await this.workerPool.getAllQueueStats();
    return {
      success: true,
      data: allStats,
    };
  }

  /**
   * GET /emails/usage
   * Get email sender usage statistics
   */
  @Get('usage')
  async getUsageStats() {
    const stats = this.emailSender.getUsageStats();
    return {
      success: true,
      data: stats,
    };
  }

  /**
   * POST /emails/queue/:category/pause
   * Pause a specific queue
   */
  @Post('queue/:category/pause')
  @HttpCode(HttpStatus.OK)
  async pauseQueue(@Param('category') category: EmailCategory) {
    await this.workerPool.pauseQueue(category);
    return {
      success: true,
      message: `Queue ${category} paused successfully`,
    };
  }

  /**
   * POST /emails/queue/:category/resume
   * Resume a specific queue
   */
  @Post('queue/:category/resume')
  @HttpCode(HttpStatus.OK)
  async resumeQueue(@Param('category') category: EmailCategory) {
    await this.workerPool.resumeQueue(category);
    return {
      success: true,
      message: `Queue ${category} resumed successfully`,
    };
  }

  /**
   * POST /emails/queue/:category/clean
   * Clean completed/failed jobs from a queue
   */
  @Post('queue/:category/clean')
  @HttpCode(HttpStatus.OK)
  async cleanQueue(
    @Param('category') category: EmailCategory,
    @Query('grace') grace?: number,
  ) {
    await this.workerPool.cleanQueue(category, grace);
    return {
      success: true,
      message: `Queue ${category} cleaned successfully`,
    };
  }

  /**
   * GET /emails/queue/:category/job/:jobId
   * Get details of a specific job
   */
  @Get('queue/:category/job/:jobId')
  async getJob(
    @Param('category') category: EmailCategory,
    @Param('jobId') jobId: string,
  ) {
    const job = await this.workerPool.getJob(category, jobId);

    if (!job) {
      return {
        success: false,
        message: 'Job not found',
      };
    }

    return {
      success: true,
      data: {
        id: job.id,
        name: job.name,
        data: job.data,
        progress: job.progress,
        attemptsMade: job.attemptsMade,
        processedOn: job.processedOn,
        finishedOn: job.finishedOn,
        failedReason: job.failedReason,
      },
    };
  }
}
