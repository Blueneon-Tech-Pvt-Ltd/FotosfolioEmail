import { Controller, Get } from '@nestjs/common';

@Controller()
export class ApiIndexController {
  @Get()
  index() {
    return {
      success: true,
      message: 'Fotosfolio Email Microservice API',
      routes: {
        health: '/api/health',
        emails: '/api/emails',
        video: '/api/video',
        cron: '/api/cron',
      },
    };
  }
}
