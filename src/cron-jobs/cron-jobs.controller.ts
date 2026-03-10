import { Controller, Post, HttpCode, HttpStatus, Logger, Body } from '@nestjs/common';
import { CronJobsService } from './cron-jobs.service';
import { WorkerPoolService } from '../workersqueue/workerqueue-pool.service';
import { EmailCategory, EmailType } from '../types/email-types';

@Controller('cron')
export class CronJobsController {
  private readonly logger = new Logger(CronJobsController.name);

  constructor(
    private readonly cronJobsService: CronJobsService,
    private readonly workerPool: WorkerPoolService,
  ) {}

  /**
   * POST /cron-jobs/midnight
   * Manually trigger midnight tasks (for testing/debugging)
   */
  @Post('midnight')
  @HttpCode(HttpStatus.OK)
  async triggerMidnightTasks() {
    this.logger.log('Manual trigger: Midnight tasks');
    
    try {
      const result = await this.cronJobsService.runMidnightTasks();
      return {
        success: true,
        message: 'Midnight tasks completed successfully',
        data: result,
      };
    } catch (error: any) {
      this.logger.error('Failed to run midnight tasks', error);
      return {
        success: false,
        message: 'Failed to run midnight tasks',
        error: error.message,
      };
    }
  }

  /**
   * POST /cron-jobs/reminders
   * Manually trigger subscription reminder tasks
   */
  @Post('reminders')
  @HttpCode(HttpStatus.OK)
  async triggerReminderTasks() {
    this.logger.log('Manual trigger: Subscription reminders');
    
    try {
      const result = await this.cronJobsService.runSubscriptionReminderTasks();
      return {
        success: true,
        message: 'Reminder tasks completed successfully',
        data: result,
      };
    } catch (error: any) {
      this.logger.error('Failed to run reminder tasks', error);
      return {
        success: false,
        message: 'Failed to run reminder tasks',
        error: error.message,
      };
    }
  }

  /**
   * POST /cron-jobs/nightly-storage
   * Manually trigger nightly storage tasks
   */
  @Post('nightly-storage')
  @HttpCode(HttpStatus.OK)
  async triggerNightlyStorageTasks() {
    this.logger.log('Manual trigger: Nightly storage tasks');
    
    try {
      const result = await this.cronJobsService.runNightlyTasks();
      return {
        success: true,
        message: 'Nightly storage tasks completed successfully',
        data: result,
      };
    } catch (error: any) {
      this.logger.error('Failed to run nightly storage tasks', error);
      return {
        success: false,
        message: 'Failed to run nightly storage tasks',
        error: error.message,
      };
    }
  }

  /**
   * POST /cron/subscription/send-expiration-emails
   * Receive subscription expiration data from main backend and queue emails
   */
  @Post('subscription/send-expiration-emails')
  @HttpCode(HttpStatus.OK)
  async sendSubscriptionExpirationEmails(@Body() body: { subscriptions?: any[]; data?: any[] }) {
    this.logger.log('Received request to send subscription expiration emails');
    
    try {
      const subscriptions = body.subscriptions || body.data || body || [];
      const dataArray = Array.isArray(subscriptions) ? subscriptions : [subscriptions];
      this.logger.log(`Processing ${dataArray.length} subscriptions for expiration emails`);

      let queued = 0;
      for (const subscription of dataArray) {
        try {
          // Transform the subscription data to match email service expectations
          const payload = {
            to: subscription.email || subscription.userEmail || subscription.user?.email,
            userName: subscription.userName || subscription.user?.name || 'User',
            daysRemaining: subscription.daysRemaining || subscription.daysLeft || 0,
          };

          this.logger.log(`Queueing email for ${payload.to} (${payload.daysRemaining} days remaining)`);

          await this.workerPool.addJob(
            EmailCategory.SUBSCRIPTION,
            EmailType.SUBSCRIPTION_EXPIRING,
            payload
          );
          queued++;
        } catch (error: any) {
          this.logger.error(`Failed to queue email for subscription ${subscription.id}: ${error.message}`);
        }
      }

      this.logger.log(`✅ Successfully queued ${queued}/${dataArray.length} expiration emails`);
      
      return {
        success: true,
        message: 'Subscription expiration emails queued',
        data: {
          total: dataArray.length,
          queued,
          failed: dataArray.length - queued,
        },
      };
    } catch (error: any) {
      this.logger.error('Failed to process subscription expiration emails', error);
      return {
        success: false,
        message: error.message,
      };
    }
  }

  /**
   * POST /cron/subscription/send-expired-emails
   * Receive expired subscription data from main backend and queue emails
   */
  @Post('subscription/send-expired-emails')
  @HttpCode(HttpStatus.OK)
  async sendExpiredSubscriptionEmails(@Body() body: { subscriptions?: any[]; data?: any[] }) {
    this.logger.log('Received request to send expired subscription emails');
    
    try {
      const subscriptions = body.subscriptions || body.data || body || [];
      const dataArray = Array.isArray(subscriptions) ? subscriptions : [subscriptions];
      this.logger.log(`Processing ${dataArray.length} expired subscriptions`);

      let queued = 0;
      for (const subscription of dataArray) {
        try {
          // Transform the subscription data to match email service expectations
          const payload = {
            to: subscription.email || subscription.userEmail || subscription.user?.email,
            userName: subscription.userName || subscription.user?.name || 'User',
            graceDaysRemaining: subscription.graceDaysRemaining || subscription.graceDays || 3,
          };

          this.logger.log(`Queueing expired email for ${payload.to} (${payload.graceDaysRemaining} grace days)`);

          await this.workerPool.addJob(
            EmailCategory.SUBSCRIPTION,
            EmailType.SUBSCRIPTION_EXPIRED,
            payload
          );
          queued++;
        } catch (error: any) {
          this.logger.error(`Failed to queue email for subscription ${subscription.id}: ${error.message}`);
        }
      }

      this.logger.log(`✅ Successfully queued ${queued}/${dataArray.length} expired emails`);
      
      return {
        success: true,
        message: 'Expired subscription emails queued',
        data: {
          total: dataArray.length,
          queued,
          failed: dataArray.length - queued,
        },
      };
    } catch (error: any) {
      this.logger.error('Failed to process expired subscription emails', error);
      return {
        success: false,
        message: error.message,
      };
    }
  }

  /**
   * POST /cron/storage/send-grace-period-notifications
   * Receive storage grace period data and queue emails
   */
  @Post('storage/send-grace-period-notifications')
  @HttpCode(HttpStatus.OK)
  async sendGracePeriodNotifications(@Body() body: { storages?: any[]; data?: any[] }) {
    this.logger.log('Received request to send grace period notifications');
    
    try {
      const storages = body.storages || body.data || body || [];
      const dataArray = Array.isArray(storages) ? storages : [storages];
      this.logger.log(`Processing ${dataArray.length} storages for grace period notifications`);

      let queued = 0;
      for (const storage of dataArray) {
        try {
          // Transform the storage data to match email service expectations
          // buildAddonFinalGrace expects: userEmail, graceDaysRemaining, storageUsed, renewLink, deleteLink
          const payload = {
            userEmail: storage.email || storage.userEmail || storage.user?.email,
            userName: storage.userName || storage.user?.name || 'User',
            graceDaysRemaining: storage.graceDaysRemaining || storage.graceDays || 3,
            storageUsed: storage.storageUsed || storage.used || 0,
            storageLimit: storage.storageLimit || storage.limit || 0,
            renewLink: storage.renewLink || 'https://prod.fotosfolio.com/billing',
            deleteLink: storage.deleteLink || 'https://prod.fotosfolio.com/storage',
          };

          this.logger.log(`Queueing grace period email for ${payload.userEmail}`);

          await this.workerPool.addJob(
            EmailCategory.STORAGE,
            EmailType.ADDON_FINAL_GRACE,
            payload
          );
          queued++;
        } catch (error: any) {
          this.logger.error(`Failed to queue grace period email for storage ${storage.id}: ${error.message}`);
        }
      }

      this.logger.log(`✅ Successfully queued ${queued}/${dataArray.length} grace period notifications`);
      
      return {
        success: true,
        message: 'Grace period notifications queued',
        data: {
          total: dataArray.length,
          queued,
          failed: dataArray.length - queued,
        },
      };
    } catch (error: any) {
      this.logger.error('Failed to process grace period notifications', error);
      return {
        success: false,
        message: error.message,
      };
    }
  }
}
