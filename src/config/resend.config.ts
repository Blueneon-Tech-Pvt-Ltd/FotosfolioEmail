import { registerAs } from '@nestjs/config';

export default registerAs('resend', () => ({
  apiKey1: process.env.RESEND_API_KEY1!,
  apiKey2: process.env.RESEND_API_KEY2!,
  dailyLimit: parseInt(process.env.RESEND_DAILY_LIMIT!, 10),
}));
