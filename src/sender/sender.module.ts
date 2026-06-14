import { Module } from '@nestjs/common';
import { ResendProviderService } from './resend-provider.service';
import { EmailSenderService } from './email-sender.service';

@Module({
  providers: [ResendProviderService, EmailSenderService],
  exports: [EmailSenderService, ResendProviderService],
})
export class SenderModule {}
