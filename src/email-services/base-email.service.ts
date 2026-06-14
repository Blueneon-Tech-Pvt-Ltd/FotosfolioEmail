// src/email-services/base-email.service.ts
import { Injectable } from '@nestjs/common';
import type { EmailData, EmailCategory, EmailType } from '../types/email-types';

// Re-export for convenience
export type { EmailData };

@Injectable()
export class BaseEmailService {
  // Helper method to add category and type to email data
  protected withMetadata(
    emailData: Omit<EmailData, 'category' | 'type'>,
    category: EmailCategory,
    type: EmailType,
  ): EmailData {
    return {
      ...emailData,
      text: emailData.text || '',
      category,
      type,
    };
  }

  // Shared utility methods if needed in the future
  protected formatDate(date: Date): string {
    return new Date(date).toLocaleDateString();
  }

  protected formatDateTime(date: Date): string {
    return new Date(date).toLocaleString('en-US', {
      timeZone: 'Asia/Kathmandu',
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  }
}
