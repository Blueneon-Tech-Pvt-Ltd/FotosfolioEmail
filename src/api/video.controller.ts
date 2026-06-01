import { Body, Controller, HttpCode, HttpStatus, Logger, Post } from '@nestjs/common';
import { VideoProcessQueue } from '../queues/video/video.queue';
import { VideoProcessDto } from './dtos/video-process.dto';

@Controller('video')
export class VideoController {
  private readonly logger = new Logger(VideoController.name);

  constructor(private readonly videoProcessQueue: VideoProcessQueue) {}

  /**
   * POST /video/queue
   * Queue a video for background processing
   */
  @Post('queue')
  @HttpCode(HttpStatus.ACCEPTED)
  async queueVideo(@Body() dto: VideoProcessDto) {
    this.logger.log(
      `📹 Received request to queue video processing job for ${dto.uploadId}`,
    );

    await this.videoProcessQueue.addVideoProcessJob(dto);

    return {
      success: true,
      message: 'Video queued successfully',
      data: {
        uploadId: dto.uploadId,
        key: dto.key,
      },
    };
  }
}