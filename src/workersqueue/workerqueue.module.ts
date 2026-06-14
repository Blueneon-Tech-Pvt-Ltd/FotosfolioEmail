import { Module } from '@nestjs/common';
import { WorkerPoolService } from './workerqueue-pool.service';
import { SenderModule } from '../sender/sender.module';
import { BaseEmailService } from '../email-services/base-email.service';
import { AccountEmailsService } from '../email-services/account-emails.service';
import { SubscriptionEmailsService } from '../email-services/subscription-emails.service';
import { SecurityEmailsService } from '../email-services/security-emails.service';
import { ProjectEmailsService } from '../email-services/project-emails.service';
import { StorageEmailsService } from '../email-services/storage-emails.service';
import { PaymentEmailsService } from '../email-services/payment-emails.service';

@Module({
  imports: [SenderModule],
  providers: [
    WorkerPoolService,
    BaseEmailService,
    AccountEmailsService,
    SubscriptionEmailsService,
    SecurityEmailsService,
    ProjectEmailsService,
    PaymentEmailsService,
    StorageEmailsService,
  ],
  exports: [WorkerPoolService],
})
export class WorkersModule {}
