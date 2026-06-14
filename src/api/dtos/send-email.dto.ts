import { IsEnum, IsNotEmpty, IsObject, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';
import { EmailCategory, EmailType } from '../../types/email-types';

/**
 * Base DTO for sending any email
 */
export class SendEmailDto {
  @IsEnum(EmailCategory)
  @IsNotEmpty()
  category: EmailCategory;

  @IsEnum(EmailType)
  @IsNotEmpty()
  type: EmailType;

  @IsObject()
  @IsNotEmpty()
  payload: Record<string, any>;
}

/**
 * DTO for sending account-related emails
 */
export class SendAccountEmailDto {
  @IsEnum(EmailType)
  @IsNotEmpty()
  type: EmailType;

  @IsObject()
  @IsNotEmpty()
  payload: Record<string, any>;
}

/**
 * DTO for batch email sending
 */
export class SendBatchEmailDto {
  @IsEnum(EmailCategory)
  @IsNotEmpty()
  category: EmailCategory;

  @ValidateNested({ each: true })
  @Type(() => SendEmailDto)
  emails: SendEmailDto[];
}

/**
 * DTO for getting queue statistics
 */
export class QueueStatsQueryDto {
  @IsEnum(EmailCategory)
  category?: EmailCategory;
}
