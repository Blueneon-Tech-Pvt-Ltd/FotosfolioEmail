import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Resend } from 'resend';
import axios from 'axios';

@Injectable()
export class ResendProviderService implements OnModuleInit {
  private readonly logger = new Logger(ResendProviderService.name);
  private resend: Resend;
  private from: string;
  private apiKey1: string;
  private apiKey2: string;
  private dailyLimit: number;
  private emailCountToday: number = 0;
  private lastResetDate: string;
  private lastApiFetchTime: number = 0;
  private apiCacheDuration: number = 5 * 60 * 1000; // Cache for 5 minutes

  constructor(private readonly configService: ConfigService) {}

  async onModuleInit() {
    this.apiKey1 = this.configService.get<string>('resend.apiKey1')!;
    this.apiKey2 = this.configService.get<string>('resend.apiKey2')!;
    this.dailyLimit = this.configService.get<number>('resend.dailyLimit') || 100;
    this.lastResetDate = new Date().toISOString().slice(0, 10);

    // Validate required API keys
    if (!this.apiKey1 || !this.apiKey2) {
      throw new Error('RESEND_API_KEY1 and RESEND_API_KEY2 must be configured');
    }

    // Initialize with first key
    await this.initializeResendClient();
  }

  /**
   * Fetch emails sent today from Resend API with pagination
   */
  private async getTodaysEmailCount(): Promise<number> {
    const now = Date.now();
    
    // Use cache to avoid excessive API calls (refresh every 5 minutes)
    if (now - this.lastApiFetchTime < this.apiCacheDuration) {
      this.logger.debug(`Using cached count: ${this.emailCountToday}`);
      return this.emailCountToday;
    }

    // Get today's date in UTC (Resend uses UTC timestamps)
    const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    let totalCount = 0;
    let pageCount = 0;
    let lastEmailId: string | null = null;
    const maxPages = 5; // Limit to avoid excessive API calls

    try {
      let hasMore = true;
      
      while (hasMore && pageCount < maxPages) {
        // Build URL with limit and starting_after for pagination
        const params: any = { limit: 100 }; // Max allowed by Resend API
        if (lastEmailId) {
          params.starting_after = lastEmailId;
        }

        // Wait 500ms between requests to respect rate limits (2 req/sec)
        if (pageCount > 0) {
          await new Promise(resolve => setTimeout(resolve, 500));
        }

        const response = await axios.get('https://api.resend.com/emails', {
          headers: {
            Authorization: `Bearer ${this.apiKey1}`,
          },
          params,
          timeout: 10000,
        });

        const emails = response.data.data || [];
        hasMore = response.data.has_more === true;
        
        // Debug: Check if we're getting unique emails
        if (pageCount === 0 && emails.length > 0) {
          this.logger.debug(`First email ID: ${emails[0]?.id}, created: ${emails[0]?.created_at}`);
        }
        if (emails.length > 0) {
          this.logger.debug(`Last email ID on page: ${emails[emails.length - 1]?.id}`);
        }
        
        // Filter emails sent today
        const todaysEmails = emails.filter((email: any) => {
          if (!email.created_at) return false;
          const emailDate = email.created_at.slice(0, 10);
          return emailDate === today;
        });

        totalCount += todaysEmails.length;
        pageCount++;
        
        this.logger.log(
          `📄 Page ${pageCount}: ${todaysEmails.length}/${emails.length} from today ` +
          `(total: ${totalCount}, has_more: ${hasMore})`
        );
        
        // Get last email ID for next page
        if (emails.length > 0) {
          lastEmailId = emails[emails.length - 1].id;
        }
        
        // Stop if no more today's emails found on this page (optimization)
        // Since emails are ordered by date (newest first), if we find no emails from today
        // on a page, all subsequent pages will be older
        if (todaysEmails.length === 0) {
          this.logger.log('⏹️ No emails from today on this page, stopping pagination');
          break;
        }
        
        // If all emails on this page are from today AND we have 100 emails, continue
        // Otherwise if we got fewer emails from today, we're at the boundary - stop
        if (emails.length === 100 && todaysEmails.length === emails.length) {
          // All 100 emails are from today, continue to next page
          continue;
        } else if (todaysEmails.length < emails.length) {
          // Mixed page (today + older), stop here
          this.logger.log('⏹️ Reached end of today\'s emails (mixed page), stopping pagination');
          break;
        } else if (emails.length < 100) {
          // This is the last page of results
          this.logger.log('⏹️ Last page of results reached');
          break;
        }
      }
      
      this.logger.log(`📧 Total emails sent today: ${totalCount} (${pageCount} pages fetched)`);
      
      // Update cache
      this.emailCountToday = totalCount;
      this.lastApiFetchTime = now;
      
      return totalCount;
    } catch (error: any) {
      this.logger.error(
        `⚠️ Failed to fetch email count from Resend API: ${error.message}`
      );
      
      if (error.response) {
        this.logger.error(
          `API Error: Status ${error.response.status}, ` +
          `Data: ${JSON.stringify(error.response.data)}`
        );
      }
      
      this.logger.log(`Using last known count: ${this.emailCountToday}`);
      
      // Return last known count (don't reset to 0 on error)
      return this.emailCountToday;
    }
  }

  /**
   * Initialize or reinitialize Resend client with appropriate API key
   */
  private async initializeResendClient(): Promise<void> {
    const count = await this.getTodaysEmailCount();
    this.emailCountToday = count;

    // Check if we need to reset daily counter (new day)
    const today = new Date().toISOString().slice(0, 10);
    if (this.lastResetDate !== today) {
      this.logger.log('📅 New day detected, resetting email counter');
      this.emailCountToday = 0;
      this.lastResetDate = today;
    }

    // Switch between keys based on daily limit
    if (this.emailCountToday >= this.dailyLimit) {
      this.logger.log(`🔄 Switching to API Key 2 (count: ${this.emailCountToday})`);
      this.resend = new Resend(this.apiKey2);
      this.from = 'Fotosfolio <no-reply@mail.fotosfolio.com>';
    } else {
      this.logger.log(`✅ Using API Key 1 (count: ${this.emailCountToday})`);
      this.resend = new Resend(this.apiKey1);
      this.from = 'Fotosfolio <no-reply@fotosfolio.com>';
    }

    this.logger.log(
      `✓ Resend initialized with key ${this.emailCountToday >= this.dailyLimit ? '2' : '1'}, from: ${this.from}`,
    );
  }

  /**
   * Get current Resend client (with automatic key rotation)
   */
  async getResendClient(): Promise<Resend> {
    // Refresh client before each batch of sends
    await this.initializeResendClient();
    return this.resend;
  }

  /**
   * Get current 'from' email address
   */
  getFromAddress(): string {
    return this.from;
  }

  /**
   * Increment email counter (call after successful send)
   * Note: With API fetching enabled, this is a fallback between API refreshes
   */
  incrementCounter(): void {
    this.emailCountToday++;
    this.logger.debug(`📈 Email counter (local): ${this.emailCountToday}/${this.dailyLimit}`);
    
    // Auto-switch if we hit the limit
    if (this.emailCountToday === this.dailyLimit) {
      this.logger.warn('⚠️ Daily limit reached on key 1, switching to key 2');
      this.resend = new Resend(this.apiKey2);
      this.from = 'Fotosfolio <no-reply@mail.fotosfolio.com>';
    }
  }

  /**
   * Get current email count (for monitoring)
   */
  getCurrentCount(): number {
    return this.emailCountToday;
  }

  /**
   * Get daily limit (for monitoring)
   */
  getDailyLimit(): number {
    return this.dailyLimit;
  }
}