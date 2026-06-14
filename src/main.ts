import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import type { NextFunction, Request, Response } from 'express';
import { AppModule } from './app.module';
import { AppLoggerService } from './logger/logger.service';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, {
    logger: new AppLoggerService(),
  });

  // Global validation pipe for DTO validation
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true, //
      transform: true, // Transform payloads to DTO instances
      forbidNonWhitelisted: true,
      transformOptions: {
        enableImplicitConversion: true,
      },
    }),
  );

  // CORS configuration for inter-service communication
  app.enableCors({
    origin: process.env.ALLOWED_ORIGINS?.split(',') || '*',
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'],
    allowedHeaders: ['Content-Type', 'Authorization'],
  });

  // Set global prefix for all routes
  app.setGlobalPrefix('api');

  // Port configuration
  const port = process.env.PORT || 3002;

  // Enable graceful shutdown hooks - ensures NestJS lifecycle hooks are called
  app.enableShutdownHooks();

  // Handle termination signals for graceful shutdown
  const logger = app.get(AppLoggerService);

  app.use((req: Request, res: Response, next: NextFunction) => {
    if (req.originalUrl === '/api/health' || req.url === '/api/health') {
      return next();
    }

    const startTime = Date.now();

    logger.log({
      event: 'http_request',
      method: req.method,
      url: req.originalUrl ?? req.url,
    });

    res.on('finish', () => {
      logger.log({
        event: 'http_response',
        method: req.method,
        url: req.originalUrl ?? req.url,
        statusCode: res.statusCode,
        responseTime: `${Date.now() - startTime}ms`,
      });
    });

    next();
  });

  process.on('SIGTERM', async () => {
    logger.log('SIGTERM signal received - initiating graceful shutdown...');
    await app.close();
    logger.log('Application shut down successfully');
    process.exit(0);
  });

  process.on('SIGINT', async () => {
    logger.log('SIGINT signal received - initiating graceful shutdown...');
    await app.close();
    logger.log('Application shut down successfully');
    process.exit(0);
  });

  await app.listen(port);

  console.log(`
  Email Microservice Started           
  Port: ${port}                        
  http://localhost:${port}/api         
  Health: http://localhost:${port}/health 
  `);
}
bootstrap();
