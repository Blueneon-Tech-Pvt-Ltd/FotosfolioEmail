import { Injectable, Logger } from '@nestjs/common';
import { ResendProviderService } from './resend-provider.service';
import { EmailData } from '../types/email-types';


@Injectable()
export class EmailSenderService {
  private readonly logger = new Logger(EmailSenderService.name);

  // Global rate limiter: max 2 emails per second (Resend API limit)
  private readonly maxPerSecond = 2;
  private sendTimestamps: number[] = [];
  private sendQueue: Array<{ resolve: () => void }> = [];
  private processing = false;

  constructor(private readonly resendProvider: ResendProviderService) {}

  /**
   * Send an email using Resend API with automatic key rotation
   */
  async sendEmail(emailData: EmailData): Promise<void> {
    const { to, subject, html, text } = emailData;

    // Wait for rate limit slot (queued globally across all workers)
    await this.acquireRateLimitSlot();

    try {
      this.logger.log(`📧 Sending ${emailData.type} email to ${to}`);

      // Get Resend client (with automatic key rotation)
      const resend = await this.resendProvider.getResendClient();
      const from = this.resendProvider.getFromAddress();

      // Ensure we have text fallback
      const textFallback = text || this.stripHtml(html);

      // Send email
      const response = await resend.emails.send({
        from,
        to: to.toLowerCase(),
        subject,
        text: textFallback,
        html,
      });

      // Check if response is valid
      if (!response || !response.data || !response.data.id) {
        this.logger.warn(
          `⚠️ Email sent but no ID returned for ${to}. Response: ${JSON.stringify(response)}`,
        );
        if (response.error) {
          throw new Error(`Resend API error: ${JSON.stringify(response.error)}`);
        }
      }

      // Increment counter after successful send
      this.resendProvider.incrementCounter();

      this.logger.log(
        `✅ Email sent successfully to ${to} (ID: ${response.data?.id || 'N/A'})`,
      );
    } catch (error: any) {
      this.logger.error(
        `❌ Failed to send ${emailData.type} email to ${to}: ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Strip HTML tags for plain text fallback
   */
  private stripHtml(html: string): string {
    return html.replace(/<[^>]+>/g, '').trim();
  }

  /**
   * Acquire a rate limit slot before sending.
   * Uses a queue to serialize access — prevents race conditions where
   * multiple workers slip through the check simultaneously.
   * Guarantees max 2 emails per second globally across all 6 workers.
   */
  private acquireRateLimitSlot(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.sendQueue.push({ resolve });
      this.processQueue();
    });
  }

  /**
   * Process the rate limit queue one at a time.
   * Only one call processes the queue at a time (via `this.processing` flag),
   * ensuring timestamps are recorded before the next caller is released.
   */
  private async processQueue(): Promise<void> {
    if (this.processing) return;
    this.processing = true;

    try {
      while (this.sendQueue.length > 0) {
        const now = Date.now();
        // Remove timestamps older than 1 second
        this.sendTimestamps = this.sendTimestamps.filter(t => now - t < 1000);

        if (this.sendTimestamps.length < this.maxPerSecond) {
          // Slot available — record timestamp NOW (before releasing) to prevent race conditions
          this.sendTimestamps.push(Date.now());
          const next = this.sendQueue.shift();
          next?.resolve();
        } else {
          // Wait until the oldest timestamp expires
          const waitTime = 1000 - (now - this.sendTimestamps[0]);
          this.logger.debug(`⏳ Rate limit: waiting ${waitTime}ms (${this.sendTimestamps.length} emails in last second)`);
          await new Promise(resolve => setTimeout(resolve, Math.max(waitTime, 50)));
        }
      }
    } finally {
      this.processing = false;
      // Safety re-check: if items were pushed while we were setting processing=false,
      // restart the queue processor to avoid stranded items
      if (this.sendQueue.length > 0) {
        this.processQueue();
      }
    }
  }

  /**
   * Send batch emails (useful for bulk operations)
   */
  async sendBatch(emails: EmailData[]): Promise<void> {
    this.logger.log(`📨 Sending batch of ${emails.length} emails`);

    const results = await Promise.allSettled(
      emails.map((email) => this.sendEmail(email)),
    );

    const successful = results.filter((r) => r.status === 'fulfilled').length;
    const failed = results.filter((r) => r.status === 'rejected').length;

    this.logger.log(
      `📊 Batch complete: ${successful} sent, ${failed} failed`,
    );

    if (failed > 0) {
      const errors = results
        .filter((r) => r.status === 'rejected')
        .map((r: any) => r.reason.message);
      this.logger.error(`Batch errors: ${errors.join(', ')}`);
    }
  }

  /**
   * Get current API usage stats
   */
  getUsageStats() {
    return {
      emailsSentToday: this.resendProvider.getCurrentCount(),
      dailyLimit: this.resendProvider.getDailyLimit(),
      currentFrom: this.resendProvider.getFromAddress(),
    };
  }
}