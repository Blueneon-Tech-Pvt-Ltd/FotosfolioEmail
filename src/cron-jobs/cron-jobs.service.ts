import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as cron from 'node-cron';
import axios from 'axios';
import { WorkerPoolService } from '../workersqueue/workerqueue-pool.service';
import { EmailType, EmailCategory } from '../types/email-types';

@Injectable()
export class CronJobsService implements OnModuleInit {
  private readonly logger = new Logger(CronJobsService.name);
  private readonly mainBackendUrl: string;

  constructor(
    private readonly configService: ConfigService,
    private readonly workerPool: WorkerPoolService,
  ) {
    // Get main backend URL from config
    this.mainBackendUrl = this.configService.get('MAIN_BACKEND_URL', 'https://prod.fotosfolio.com');
    this.logger.log(`🔗 Main Backend URL: ${this.mainBackendUrl}`);
  }

  onModuleInit() {
    this.logger.log('🕐 Initializing unified cron jobs...');

    // Runs daily at 12:00 AM (midnight) - Subscription & Storage tasks
    cron.schedule('0 0 * * *', async () => {
      await this.runMidnightTasks();
    });

    // Runs daily at 9:00 AM - Subscription expiration reminders
    cron.schedule('0 9 * * *', async () => {
      await this.runSubscriptionReminderTasks();
    });

    // Runs daily at 11:59 PM - Storage tracking and project cleanup
    cron.schedule('59 23 * * *', async () => {
      await this.runNightlyTasks();
    });

    this.logger.log('✅ All cron jobs registered successfully');
    this.logger.log('  - 00:00 (Midnight): Expired subscriptions, storage, grace periods');
    this.logger.log('  - 09:00 (9 AM): Subscription expiration reminders');
    this.logger.log('  - 23:59 (11:59 PM): Storage tracking, project cleanup');
  }

  /**
   * Midnight Tasks - Fetch data from backend and queue emails
   */
  async runMidnightTasks() {
    const startTime = new Date();
    this.logger.log(`[Cron] Starting midnight tasks at ${startTime.toISOString()}`);

    const results = {
      expiredSubscriptions: { success: false, message: '' },
      expiredAddons: { success: false, message: '' },
      gracePeriod: { success: false, message: '' },
      markExpired: { success: false, message: '' },
    };

    try {
      // 1. Send expired subscription emails
      try {
        const result = await this.sendExpiredEmails();
        results.expiredSubscriptions = result;
      } catch (error: any) {
        results.expiredSubscriptions = { success: false, message: error.message };
      }

      // 2. Expire addon storages
      try {
        const result = await this.expireAddonStorages();
        results.expiredAddons = result;
      } catch (error: any) {
        results.expiredAddons = { success: false, message: error.message };
      }

      // 3. Send grace period notifications
      try {
        const result = await this.sendGracePeriodNotifications();
        results.gracePeriod = result;
      } catch (error: any) {
        results.gracePeriod = { success: false, message: error.message };
      }

      // 4. Mark subscriptions as ended
      try {
        const result = await this.markExpiredAsEnded();
        results.markExpired = result;
      } catch (error: any) {
        results.markExpired = { success: false, message: error.message };
      }

      const endTime = new Date();
      const duration = (endTime.getTime() - startTime.getTime()) / 1000;
      this.logger.log(`[Cron] ✅ Midnight tasks completed. Duration: ${duration}s`);

      return { success: true, duration: `${duration}s`, timestamp: endTime.toISOString(), results };
    } catch (error) {
      this.logger.error('[Cron] ❌ Failed midnight tasks', error.stack || error);
      throw error;
    }
  }

  /**
   * Morning Tasks - Send subscription expiration reminders
   */
  async runSubscriptionReminderTasks() {
    this.logger.log('[Cron] Starting subscription reminder tasks');

    try {
      const result = await this.sendExpirationEmails();
      
      this.logger.log('[Cron] ✅ Subscription reminder tasks completed');
      return { success: result.success, timestamp: new Date().toISOString(), data: result };
    } catch (error) {
      this.logger.error('[Cron] ❌ Failed subscription reminder tasks', error.stack || error);
      throw error;
    }
  }

  /**
   * Nightly Tasks - Trigger main backend for storage tracking and project cleanup
   */
  async runNightlyTasks() {
    this.logger.log('[Cron] Starting nightly tasks');

    const results = {
      storageTracking: { success: false, message: '' },
      cleanupReservations: { success: false, message: '' },
      archiveProjects: { success: false, message: '' },
      deleteExpired: { success: false, message: '' },
    };

    try {
      // 1. Update storage usage tracking
      try {
        const result = await this.setTotalUsage();
        results.storageTracking = result;
      } catch (error: any) {
        results.storageTracking = { success: false, message: error.message };
      }

      // 2. Cleanup expired reservations
      try {
        const result = await this.cleanupExpiredReservations();
        results.cleanupReservations = result;
      } catch (error: any) {
        results.cleanupReservations = { success: false, message: error.message };
      }

      // 3. Archive expired projects
      try {
        const result = await this.archiveExpired();
        results.archiveProjects = result;
      } catch (error: any) {
        results.archiveProjects = { success: false, message: error.message };
      }

      // 4. Delete expired projects
      try {
        const result = await this.deleteExpired();
        results.deleteExpired = result;
      } catch (error: any) {
        results.deleteExpired = { success: false, message: error.message };
      }

      this.logger.log('[Cron] ✅ Nightly tasks completed');
      return { success: true, timestamp: new Date().toISOString(), results };
    } catch (error) {
      this.logger.error('[Cron] ❌ Failed nightly tasks', error.stack || error);
      throw error;
    }
  }

  /**
   * Individual Task Methods (for Swagger endpoints)
   */

  /**
   * Send expiration emails for subscriptions expiring soon
   */
  async sendExpirationEmails() {
    this.logger.log('[Cron] Sending subscription expiration emails');
    
    try {
      const result = await this.callBackendEndpoint('/cron/subscription/send-expiration-emails', 'POST');
      this.logger.log('[Cron] ✅ Subscription expiration emails sent');
      return { success: true, message: 'Subscription expiration emails sent', data: result };
    } catch (error: any) {
      this.logger.error('[Cron] ❌ Failed to send expiration emails', error);
      return { success: false, message: error.message };
    }
  }

  /**
   * Send expired emails for subscriptions that expired today
   */
  async sendExpiredEmails() {
    this.logger.log('[Cron] Sending expired subscription emails');
    
    try {
      const result = await this.callBackendEndpoint('/cron/subscription/send-expired-emails', 'POST');
      this.logger.log('[Cron] ✅ Expired subscription emails sent');
      return { success: true, message: 'Expired subscription emails sent', data: result };
    } catch (error: any) {
      this.logger.error('[Cron] ❌ Failed to send expired emails', error);
      return { success: false, message: error.message };
    }
  }

  /**
   * Mark expired subscriptions as ended
   */
  async markExpiredAsEnded() {
    this.logger.log('[Cron] Marking expired subscriptions as ended');
    
    try {
      const result = await this.callBackendEndpoint('/cron/subscription/mark-expired-as-ended', 'POST');
      this.logger.log('[Cron] ✅ Marked expired subscriptions as ended');
      return { success: true, message: 'Marked expired subscriptions as ended', data: result };
    } catch (error: any) {
      this.logger.error('[Cron] ❌ Failed to mark expired subscriptions', error);
      return { success: false, message: error.message };
    }
  }

  /**
   * Expire addon storages
   */
  async expireAddonStorages() {
    this.logger.log('[Cron] Expiring addon storages');
    
    try {
      const result = await this.callBackendEndpoint('/cron/storage/expire-addon-storages', 'POST');
      this.logger.log('[Cron] ✅ Addon storages expired');
      return { success: true, message: 'Addon storages expired', data: result };
    } catch (error: any) {
      this.logger.error('[Cron] ❌ Failed to expire addon storages', error);
      return { success: false, message: error.message };
    }
  }

  /**
   * Send grace period notifications
   */
  async sendGracePeriodNotifications() {
    this.logger.log('[Cron] Sending grace period notifications');
    
    try {
      const result = await this.callBackendEndpoint('/cron/storage/send-grace-period-notifications', 'POST');
      this.logger.log('[Cron] ✅ Grace period notifications sent');
      return { success: true, message: 'Grace period notifications sent', data: result };
    } catch (error: any) {
      this.logger.error('[Cron] ❌ Failed to send grace period notifications', error);
      return { success: false, message: error.message };
    }
  }

  /**
   * Set total storage usage
   */
  async setTotalUsage() {
    this.logger.log('[Cron] Setting total storage usage');
    
    try {
      const result = await this.callBackendEndpoint('/cron/storage/set-total-usage', 'POST');
      this.logger.log('[Cron] ✅ Total storage usage set');
      return { success: true, message: 'Total storage usage set', data: result };
    } catch (error: any) {
      this.logger.error('[Cron] ❌ Failed to set storage usage', error);
      return { success: false, message: error.message };
    }
  }

  /**
   * Cleanup expired reservations
   */
  async cleanupExpiredReservations() {
    this.logger.log('[Cron] Cleaning up expired reservations');
    
    try {
      const result = await this.callBackendEndpoint('/cron/storage/cleanup-expired-reservations', 'POST');
      this.logger.log('[Cron] ✅ Expired reservations cleaned up');
      return { success: true, message: 'Expired reservations cleaned up', data: result };
    } catch (error: any) {
      this.logger.error('[Cron] ❌ Failed to cleanup reservations', error);
      return { success: false, message: error.message };
    }
  }

  /**
   * Archive expired projects
   */
  async archiveExpired() {
    this.logger.log('[Cron] Archiving expired projects');
    
    try {
      const result = await this.callBackendEndpoint('/cron/project/archive-expired', 'POST');
      this.logger.log('[Cron] ✅ Expired projects archived');
      return { success: true, message: 'Expired projects archived', data: result };
    } catch (error: any) {
      this.logger.error('[Cron] ❌ Failed to archive projects', error);
      return { success: false, message: error.message };
    }
  }

  /**
   * Delete expired projects
   */
  async deleteExpired() {
    this.logger.log('[Cron] Deleting expired projects');
    
    try {
      const result = await this.callBackendEndpoint('/cron/project/delete-expired', 'POST');
      this.logger.log('[Cron] ✅ Expired projects deleted');
      return { success: true, message: 'Expired projects deleted', data: result };
    } catch (error: any) {
      this.logger.error('[Cron] ❌ Failed to delete expired projects', error);
      return { success: false, message: error.message };
    }
  }

  /**
   * Delete expired archived projects
   */
  async deleteExpiredArchived() {
    this.logger.log('[Cron] Deleting expired archived projects');
    
    try {
      const result = await this.callBackendEndpoint('/cron/project/delete-expired-archived', 'POST');
      this.logger.log('[Cron] ✅ Expired archived projects deleted');
      return { success: true, message: 'Expired archived projects deleted', data: result };
    } catch (error: any) {
      this.logger.error('[Cron] ❌ Failed to delete expired archived projects', error);
      return { success: false, message: error.message };
    }
  }

  /**
   * Helper method to GET data from main backend API
   */
  private async getBackendData(endpoint: string): Promise<any[]> {
    const url = `${this.mainBackendUrl}${endpoint}`;
    
    try {
      this.logger.log(`[API] Fetching data from GET ${url}`);
      
      const response = await axios({
        method: 'GET',
        url,
        timeout: 30000, // 30 second timeout
      });

      const data = response.data?.data || response.data || [];
      this.logger.log(`[API] ✅ Received ${data.length} items from ${endpoint}`);
      return data;
    } catch (error: any) {
      if (error.code === 'ECONNREFUSED') {
        this.logger.warn(`[API] ⚠️ Main backend not available at ${url}`);
        return []; // Return empty array if backend unavailable
      }
      
      if (error.response) {
        this.logger.error(`[API] ❌ ${endpoint} failed: ${error.response.status} - ${error.response.data?.message || error.message}`);
        return [];
      }

      this.logger.error(`[API] ❌ ${endpoint} failed: ${error.message}`);
      return [];
    }
  }

  /**
   * Helper method to call main backend API endpoints (POST for actions)
   */
  private async callBackendEndpoint(endpoint: string, method: 'GET' | 'POST' = 'POST'): Promise<any> {
    const url = `${this.mainBackendUrl}${endpoint}`;
    
    try {
      this.logger.log(`[API] Calling ${method} ${url}`);
      
      const response = await axios({
        method,
        url,
        timeout: 60000, // 60 second timeout for potentially long-running tasks
      });

      if (response.data?.success !== false) {
        this.logger.log(`[API] ✅ ${endpoint} completed successfully`);
        return response.data;
      } else {
        throw new Error(response.data?.message || 'Backend returned success: false');
      }
    } catch (error: any) {
      if (error.code === 'ECONNREFUSED') {
        this.logger.warn(`[API] ⚠️ Main backend not available at ${url}`);
        throw new Error('Main backend not available');
      }
      
      if (error.response) {
        this.logger.error(`[API] ❌ ${endpoint} failed: ${error.response.status} - ${error.response.data?.message || error.message}`);
        throw new Error(error.response.data?.message || error.message);
      }

      this.logger.error(`[API] ❌ ${endpoint} failed: ${error.message}`);
      throw error;
    }
  }
}
