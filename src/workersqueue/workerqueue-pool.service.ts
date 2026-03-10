import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Queue, Worker, Job } from 'bullmq';
import { EmailSenderService } from '../sender/email-sender.service';
import { AccountEmailsService } from '../email-services/account-emails.service';
import { SubscriptionEmailsService } from '../email-services/subscription-emails.service';
import { SecurityEmailsService } from '../email-services/security-emails.service';
import { ProjectEmailsService } from '../email-services/project-emails.service';
import { PaymentEmailsService } from '../email-services/payment-emails.service';
import { StorageEmailsService } from '../email-services/storage-emails.service';
import { EmailCategory, EmailType, EmailData } from '../types/email-types';

@Injectable()
export class WorkerPoolService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WorkerPoolService.name);
  private queues: Map<EmailCategory, Queue> = new Map();
  private workers: Worker[] = [];

  constructor(
    private readonly configService: ConfigService,
    private readonly emailSender: EmailSenderService,
    private readonly accountEmails: AccountEmailsService,
    private readonly subscriptionEmails: SubscriptionEmailsService,
    private readonly securityEmails: SecurityEmailsService,
    private readonly projectEmails: ProjectEmailsService,
    private readonly paymentEmails: PaymentEmailsService,
    private readonly storageEmails: StorageEmailsService,
  ) {}

  async onModuleInit() {
    this.logger.log('🚀 Initializing BullMQ worker pools...');
    
    const connection = {
      host: this.configService.get('redis.host'),
      port: this.configService.get('redis.port'),
      password: this.configService.get('redis.password'),
    };

    const concurrency = parseInt(this.configService.get('WORKER_CONCURRENCY') || '2', 10);

    // Create queues and workers for each category
    const categories: EmailCategory[] = [
      EmailCategory.ACCOUNT,
      EmailCategory.SUBSCRIPTION,
      EmailCategory.SECURITY,
      EmailCategory.PROJECT,
      EmailCategory.PAYMENT,
      EmailCategory.STORAGE,
    ];

    for (const category of categories) {
      const queueName = `${category}-emails`;
      
      // Create queue
      const queue = new Queue(queueName, { connection });
      this.queues.set(category, queue);

      // Create worker with rate limiting
      // Resend allows 2 requests/second globally
      // With 6 queues, we use conservative limits + BullMQ retry for rate limit errors
      const worker = new Worker(
        queueName,
        async (job: Job) => this.processJob(category, job),
        {
          connection,
          concurrency: 2, // Process 2 emails at a time per queue
          limiter: {
            max: 2,
            duration: 1000, // 2 jobs per second per queue
          },
        },
      );

      // Worker event listeners
      worker.on('completed', (job) => {
        this.logger.log(`✅ [${category}] Job ${job.id} completed successfully`);
      });

      worker.on('failed', (job, err) => {
        this.logger.error(
          `❌ [${category}] Job ${job?.id} failed: ${err.message}`,
          err.stack,
        );
      });

      worker.on('error', (err) => {
        this.logger.error(`⚠️ [${category}] Worker error: ${err.message}`);
      });

      worker.on('stalled', (jobId) => {
        this.logger.warn(`⏸️ [${category}] Job ${jobId} stalled`);
      });

      this.workers.push(worker);
      this.logger.log(
        `✓ Worker pool created for ${category} (concurrency: ${concurrency})`,
      );
    }

    this.logger.log(
      `🎉 All ${categories.length} worker pools initialized successfully`,
    );
  }

  /**
   * Process a single job from the queue
   */
  private async processJob(category: EmailCategory, job: Job): Promise<void> {
    const { type, payload } = job.data;
    
    this.logger.log(
      `📧 Processing ${type} email (Job ID: ${job.id}, Attempt: ${job.attemptsMade + 1})`,
    );

    try {
      // Build email data based on category
      let emailData: EmailData;

      switch (category) {
        case EmailCategory.ACCOUNT:
          emailData = await this.buildAccountEmail(type, payload) as EmailData;
          break;
        case EmailCategory.SUBSCRIPTION:
          emailData = await this.buildSubscriptionEmail(type, payload) as EmailData;
          break;
        case EmailCategory.SECURITY:
          emailData = await this.buildSecurityEmail(type, payload) as EmailData;
          break;
        case EmailCategory.PROJECT:
          emailData = await this.buildProjectEmail(type, payload) as EmailData;
          break;
        case EmailCategory.PAYMENT:
          emailData = await this.buildPaymentEmail(type, payload) as EmailData;
          break;
        case EmailCategory.STORAGE:
          emailData = await this.buildStorageEmail(type, payload) as EmailData;
          break;
        default:
          throw new Error(`Unknown email category: ${category}`);
      }

      // Ensure category and type are set (for legacy services that don't include them)
      if (!emailData.category) {
        emailData.category = category;
      }
      if (!emailData.type) {
        emailData.type = type;
      }

      // Send the email
      await this.emailSender.sendEmail(emailData);
      
      this.logger.log(`✓ Successfully processed ${type} for ${emailData.to}`);
    } catch (error: any) {
      this.logger.error(
        `Failed to process job ${job.id}: ${error.message}`,
        error.stack,
      );
      throw error; // Re-throw to trigger retry
    }
  }

  /**
   * Build account-related emails
   */
  private async buildAccountEmail(type: EmailType, payload: any): Promise<EmailData> {
    switch (type) {
      case EmailType.ACCOUNT_CREATED:
        return this.accountEmails.buildAccountCreatedEmail(payload.to, payload.username);
      case EmailType.PASSWORD_RESET:
        return this.accountEmails.buildPasswordResetEmail(payload.to, payload.username, payload.resetLink);
      case EmailType.EMAIL_VERIFICATION:
        return this.accountEmails.buildOtpEmail(payload.to, payload.userName, payload.otpCode);
      case EmailType.PASSWORD_CHANGED:
        return this.accountEmails.buildPasswordResetSuccessEmail(payload.to, payload.username);
      case EmailType.EMAIL_CHANGED:
        // Map to OTP verified as a placeholder
        return this.accountEmails.buildOtpVerifiedEmail(payload.to, payload.userName);
      case EmailType.ACCOUNT_DELETED:
        // Fallback to account created (inverse logic)
        return this.accountEmails.buildAccountCreatedEmail(payload.to, payload.username);
      case EmailType.ACCOUNT_ACTIVATION:
        return this.subscriptionEmails.buildAccountActivationEmail(
          payload.to,
          payload.userName,
          payload.planName
        );
      case EmailType.TWO_FACTOR_ENABLED:
        return this.securityEmails.build2FAEnabledEmail(
          payload.userEmail,
          payload.device || 'Unknown Device',
          payload.location || 'Unknown Location',
          payload.ipAddress || 'Unknown IP',
          payload.datetime || new Date()
        );
      case EmailType.TWO_FACTOR_CODE:
        return this.accountEmails.buildOtpEmail(payload.to, payload.userName, payload.otpCode);
      case EmailType.CONTACT_FORM:
        // Contact form uses a special DTO structure
        return this.accountEmails.buildAccountCreatedEmail(
          payload.contact?.email || payload.to,
          payload.contact?.name || 'User'
        );
      default:
        throw new Error(`Unknown account email type: ${type}`);
    }
  }

  /**
   * Build subscription-related emails
   */
  private async buildSubscriptionEmail(type: EmailType, payload: any): Promise<EmailData> {
    switch (type) {
      case EmailType.SUBSCRIPTION_STARTED:
        return this.subscriptionEmails.buildSubscriptionCreatedEmail(
          payload.to,
          payload.userName,
          payload.planName,
          payload.startDate,
          payload.endDate,
          payload.status
        );
      case EmailType.SUBSCRIPTION_RENEWED:
        return this.subscriptionEmails.buildSubscriptionRenewedEmail(
          payload.to,
          payload.userName,
          payload.planName,
          payload.startDate,
          payload.endDate,
          payload.status
        );
      case EmailType.SUBSCRIPTION_CANCELLED:
        // Map to subscription expired
        return this.subscriptionEmails.buildSubscriptionExpiredEmail(
          payload.to,
          payload.userName,
          payload.graceDaysRemaining 
        );
      case EmailType.SUBSCRIPTION_EXPIRING:
        return this.subscriptionEmails.buildSubscriptionExpirationReminderEmail(
          payload.to,
          payload.userName,
          payload.daysRemaining || payload.daysLeft 
        );
      case EmailType.SUBSCRIPTION_EXPIRED:
        return this.subscriptionEmails.buildSubscriptionExpiredEmail(
          payload.to,
          payload.userName,
          payload.graceDaysRemaining || 3
        );
      case EmailType.PLAN_UPGRADED:
        // Map to account activation
        return this.subscriptionEmails.buildAccountActivationEmail(
          payload.to,
          payload.userName,
          payload.planName
        );
      case EmailType.PLAN_DOWNGRADED:
        // Map to subscription created
        return this.subscriptionEmails.buildSubscriptionCreatedEmail(
          payload.to,
          payload.userName,
          payload.planName,
          payload.startDate,
          payload.endDate,
          payload.status
        );
      default:
        throw new Error(`Unknown subscription email type: ${type}`);
    }
  }

  /**
   * Build security-related emails
   */
  private async buildSecurityEmail(type: EmailType, payload: any): Promise<EmailData> {
    switch (type) {
      case EmailType.LOGIN_ALERT:
        return this.securityEmails.buildNewIpLoginAlertEmail(
          payload.userEmail,
          payload.ipAddress,
          payload.device,
          payload.location,
          payload.datetime
        );
      case EmailType.NEW_IP_LOGIN:
        return this.securityEmails.buildNewIpLoginAlertEmail(
          payload.userEmail,
          payload.ipAddress,
          payload.device,
          payload.location,
          payload.datetime
        );
      case EmailType.SUSPICIOUS_ACTIVITY:
        return this.securityEmails.buildLoginNotificationEmail(
          payload.to,
          payload.userName,
          payload.loginTime,
          payload.ipAddress
        );
      case EmailType.GOOGLE_ACCOUNT_DISCONNECTED:
        return this.accountEmails.buildGoogleAccountDisconnectedEmail(
          payload.userEmail,
          payload.username,
          payload.device || 'Unknown Device',
          payload.location || 'Unknown Location',
          payload.ipAddress || 'Unknown IP',
          payload.datetime || new Date()
        );
      case EmailType.TWO_FA_DISABLED:
        return this.securityEmails.build2FADisabledEmail(
          payload.userEmail,
          payload.device || 'Unknown Device',
          payload.location || 'Unknown Location',
          payload.ipAddress || 'Unknown IP',
          payload.datetime || new Date()
        );
      case EmailType.PASSKEY_ENABLED:
        return this.securityEmails.buildPasskeyEnabledEmail(
          payload.userEmail,
          payload.passkeyName || 'Default Passkey',
          payload.device || 'Unknown Device',
          payload.location || 'Unknown Location',
          payload.ipAddress || 'Unknown IP',
          payload.datetime || new Date()
        );
      case EmailType.PASSKEY_DISABLED:
        return this.securityEmails.buildPasskeyDisabledEmail(
          payload.userEmail,
          payload.passkeyName || 'Default Passkey',
          payload.device || 'Unknown Device',
          payload.location || 'Unknown Location',
          payload.ipAddress || 'Unknown IP',
          payload.datetime || new Date()
        );
      case EmailType.SECURITY_QUESTION_ENABLED:
        return this.securityEmails.buildSecurityQuestionEnabledEmail(
          payload.userEmail,
          payload.device || 'Unknown Device',
          payload.location || 'Unknown Location',
          payload.ipAddress || 'Unknown IP',
          payload.datetime || new Date()
        );
      case EmailType.SECURITY_QUESTION_DISABLED:
        return this.securityEmails.buildSecurityQuestionDisabledEmail(
          payload.userEmail,
          payload.device || 'Unknown Device',
          payload.location || 'Unknown Location',
          payload.ipAddress || 'Unknown IP',
          payload.datetime || new Date()
        );
      default:
        throw new Error(`Unknown security email type: ${type}`);
    }
  }

  /**
   * Build project-related emails
   */
  private async buildProjectEmail(type: EmailType, payload: any): Promise<EmailData> {
    switch (type) {
      case EmailType.PROJECT_SHARED:
        return this.projectEmails.buildProjectTransferEmail(
          payload.receiverEmail,
          payload.receiverName,
          payload.senderName,
          payload.projectName,
          payload.projectLink,
          payload.totalFiles,
          payload.totalSize
        );
      case EmailType.GALLERY_SHARED:
        // Map to project invitation
        return this.projectEmails.buildProjectInvitationEmail(
          payload.projectName,
          payload.inviterName,
          payload.receiverEmail,
          payload.projectLink
        );
      case EmailType.FOLDER_SHARED:
        // Map to access request
        return this.projectEmails.buildAccessRequestEmail(
          payload.projectName,
          payload.requesterEmail,
          payload.ownerEmail,
          payload.photographerName || payload.requesterName,
          payload.projectId
        );
      default:
        throw new Error(`Unknown project email type: ${type}`);
    }
  }

  /**
   * Build payment-related emails
   */
  private async buildPaymentEmail(type: EmailType, payload: any): Promise<EmailData> {
    switch (type) {
      case EmailType.PAYMENT_SUCCESS:
        return this.paymentEmails.buildPaymentSuccess(payload);
      case EmailType.PAYMENT_FAILED:
        return this.paymentEmails.buildPaymentFailed(payload);
      case EmailType.PAYMENT_REJECTION:
        return this.paymentEmails.buildPaymentRejectionEmail(payload.to, payload.userName, payload.planName);
      case EmailType.REFUND_PROCESSED:
        return this.paymentEmails.buildRefundProcessed(payload);
      default:
        throw new Error(`Unknown payment email type: ${type}`);
    }
  }

  /**
   * Build storage-related emails
   */
  private async buildStorageEmail(type: EmailType, payload: any): Promise<EmailData> {
    switch (type) {
      case EmailType.STORAGE_WARNING:
        return this.storageEmails.buildStorageWarning(payload);
      case EmailType.STORAGE_FULL:
        return this.storageEmails.buildStorageFull(payload);
      case EmailType.ADDON_EXPIRY:
        return this.storageEmails.buildAddonExpiry(payload);
      case EmailType.ADDON_FINAL_GRACE:
        return this.storageEmails.buildAddonFinalGrace(payload);
      default:
        throw new Error(`Unknown storage email type: ${type}`);
    }
  }

  /**
   * Add a new job to the queue (called by REST API or main app)
   */
  async addJob(
    category: EmailCategory,
    type: EmailType,
    payload: any,
  ): Promise<void> {
    const queue = this.queues.get(category);
    if (!queue) {
      throw new Error(`Queue for category ${category} not found`);
    }

    await queue.add(
      type,
      { type, payload },
      {
        attempts: 5, // Retry up to 5 times
        backoff: {
          type: 'exponential',
          delay: 2000, // Start with 2s, then 4s, 8s, 16s, 32s
        },
        removeOnComplete: {
          count: 100, // Keep last 100 completed jobs
          age: 3600, // Remove after 1 hour
        },
        removeOnFail: false, // Keep failed jobs for debugging
      },
    );

    this.logger.log(`➕ Added ${type} job to ${category} queue`);
  }

  /**
   * Get statistics for a specific queue
   */
  async getQueueStats(category: EmailCategory) {
    const queue = this.queues.get(category);
    if (!queue) {
      return null;
    }

    try {
      const [waiting, active, completed, failed, delayed, paused] = await Promise.all([
        queue.getWaitingCount(),
        queue.getActiveCount(),
        queue.getCompletedCount(),
        queue.getFailedCount(),
        queue.getDelayedCount(),
        queue.isPaused(),
      ]);

      return {
        waiting,
        active,
        completed,
        failed,
        delayed,
        paused,
      };
    } catch (error: any) {
      this.logger.error(`Failed to get stats for ${category}: ${error.message}`);
      return null;
    }
  }

  /**
   * Get statistics for all queues
   */
  async getAllQueueStats() {
    const stats: Record<string, any> = {};

    for (const category of Object.values(EmailCategory)) {
      stats[category] = await this.getQueueStats(category);
    }

    return stats;
  }

  /**
   * Pause a specific queue
   */
  async pauseQueue(category: EmailCategory): Promise<void> {
    const queue = this.queues.get(category);
    if (!queue) {
      throw new Error(`Queue for category ${category} not found`);
    }

    await queue.pause();
    this.logger.log(`⏸️ Paused ${category} queue`);
  }

  /**
   * Resume a specific queue
   */
  async resumeQueue(category: EmailCategory): Promise<void> {
    const queue = this.queues.get(category);
    if (!queue) {
      throw new Error(`Queue for category ${category} not found`);
    }

    await queue.resume();
    this.logger.log(`▶️ Resumed ${category} queue`);
  }

  /**
   * Clean up completed/failed jobs from a queue
   */
  async cleanQueue(
    category: EmailCategory,
    grace: number = 3600000, // 1 hour default
  ): Promise<void> {
    const queue = this.queues.get(category);
    if (!queue) {
      throw new Error(`Queue for category ${category} not found`);
    }

    await queue.clean(grace, 100, 'completed');
    await queue.clean(grace, 100, 'failed');
    
    this.logger.log(`🧹 Cleaned ${category} queue (grace: ${grace}ms)`);
  }

  /**
   * Get a specific job details
   */
  async getJob(category: EmailCategory, jobId: string) {
    const queue = this.queues.get(category);
    if (!queue) {
      throw new Error(`Queue for category ${category} not found`);
    }

    return await queue.getJob(jobId);
  }

  /**
   * Graceful shutdown - close all workers and queues
   */
  async onModuleDestroy() {
    this.logger.log('🛑 Shutting down worker pools...');
    
    try {
      // Close all workers first
      await Promise.all(this.workers.map((worker) => worker.close()));
      this.logger.log('✓ All workers closed');

      // Then close all queues
      await Promise.all(
        Array.from(this.queues.values()).map((queue) => queue.close()),
      );
      this.logger.log('✓ All queues closed');

      this.logger.log('✅ Worker pool service shut down successfully');
    } catch (error: any) {
      this.logger.error(`Error during shutdown: ${error.message}`);
    }
  }
}