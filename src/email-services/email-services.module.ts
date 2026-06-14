import { Module } from '@nestjs/common';
import { BaseEmailService } from './base-email.service';
import { AccountEmailsService } from './account-emails.service';
import { SubscriptionEmailsService } from './subscription-emails.service';
import { SecurityEmailsService } from './security-emails.service';
import { ProjectEmailsService } from './project-emails.service';
import { PaymentEmailsService } from './payment-emails.service';
import { StorageEmailsService } from './storage-emails.service';

@Module({
  providers: [
    BaseEmailService,
    AccountEmailsService,
    SubscriptionEmailsService,
    SecurityEmailsService,
    ProjectEmailsService,
    PaymentEmailsService,
    StorageEmailsService,
  ],
  exports: [
    BaseEmailService,
    AccountEmailsService,
    SubscriptionEmailsService,
    SecurityEmailsService,
    ProjectEmailsService,
    PaymentEmailsService,
    StorageEmailsService,
  ],
})
export class EmailServicesModule {}
