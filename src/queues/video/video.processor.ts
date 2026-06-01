import { Injectable, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { VideoProcessJob } from './video.job.interface';
import { VideoS3Service } from './video.s3.service';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

ffmpeg.setFfmpegPath(ffmpegInstaller.path);

@Injectable()
export class VideoProcessor {
  private readonly logger = new Logger(VideoProcessor.name);
  private webpSupported?: boolean;
  private execFileP = promisify(execFile);

  constructor(private readonly videoS3Service: VideoS3Service) {}

  async process(job: Job<VideoProcessJob>): Promise<void> {
    this.logger.log(
      `Starting video job ${job.id ?? 'unknown'} for ${job.data.key} (uploadId: ${job.data.uploadId})`,
    );

    this.logger.log(`Step 1/4: generating thumbnail for ${job.data.key}`);
    const [thumbnailResult, moovResult] = await Promise.allSettled([
      this.generateThumbnail(job),
      this.extractMoov(job),
    ]);

    this.logger.log(`Step 2/4: updating progress for ${job.data.key}`);
    await job.updateProgress(50);

    this.logger.log(`Step 3/4: collecting task results for ${job.data.key}`);
    const thumbnailFailed = this.logSettledResult('thumbnail', job.data.key, thumbnailResult);
    const moovFailed = this.logSettledResult('moov', job.data.key, moovResult);

    if (thumbnailFailed || moovFailed) {
      throw new Error(`One or more tasks failed for ${job.data.key}`);
    }

    this.logger.log(`Step 4/4: marking ${job.data.key} as complete`);
    await job.updateProgress(100);

    this.logger.log(
      `Finished video job ${job.id ?? 'unknown'} for ${job.data.key}`,
    );
  }

  private async generateThumbnail(job: Job<VideoProcessJob>): Promise<void> {
    this.logger.log(`Thumbnail step: requesting read URL for ${job.data.key}`);
    const sourceUrl = await this.getPresignedReadUrl(job.data.key);

    this.logger.log(`Thumbnail step: capturing frame for ${job.data.key}`);
    const { buffer: thumbnailBuffer, ext, contentType } = await this.captureThumbnail(sourceUrl);

    this.logger.log(
      `Thumbnail step: captured ${thumbnailBuffer.length} bytes for ${job.data.key}`,
    );

    const thumbnailKey = this.deriveThumbnailKey(job.data.key, ext);

    this.logger.log(`Thumbnail step: uploading ${thumbnailKey}`);
    await this.videoS3Service.uploadBuffer(thumbnailKey, thumbnailBuffer, contentType);

    if (thumbnailBuffer.length === 0) {
      this.logger.warn(`Thumbnail generated empty buffer for ${thumbnailKey}`);
    }

    this.logger.log(
      `Thumbnail generated for ${thumbnailKey} (${Math.round(thumbnailBuffer.length / 1024)} KB)`,
    );
  }

  private async extractMoov(job: Job<VideoProcessJob>): Promise<void> {
    this.logger.log(`Moov step: requesting last 10MB bytes for ${job.data.key}`);
    // Check last 10MB for unoptimized video moov atoms
    const buffer = await this.videoS3Service.getObjectRange(job.data.key, 'bytes=-10000000');
    const moovOffset = this.findMoovOffset(buffer);

    if (moovOffset === -1) {
      throw new Error(
        'moov atom not found in last 10MB — file may be optimized (faststart), corrupt, or moov is unusually large',
      );
    }

    // Atom size is a 32-bit big-endian integer at the start of the atom
    const moovSize = buffer.readUInt32BE(moovOffset);
    
    // Ensure we don't slice beyond the buffer if moov is cut off (though we fetched 10MB, it should fit)
    const endOffset = Math.min(moovOffset + moovSize, buffer.length);
    const moovBuffer = buffer.slice(moovOffset, endOffset);
    const moovKey = this.deriveMoovKey(job.data.key);

    this.logger.log(`Moov step: uploading ${moovKey}`);
    await this.videoS3Service.uploadBuffer(moovKey, moovBuffer, 'application/octet-stream');

    this.logger.log(`Moov atom extracted for ${moovKey} (${moovBuffer.length} bytes)`);
  }

  private async getPresignedReadUrl(key: string): Promise<string> {
    return this.videoS3Service.getPresignedReadUrl(key);
  }



  private async captureThumbnail(videoPath: string): Promise<{ buffer: Buffer; ext: string; contentType: string }> {
    this.logger.log('Starting ffmpeg thumbnail capture');

    const tempDir = await mkdtemp(join(tmpdir(), 'fotosfolio-video-thumb-'));
    const useWebp = await this.supportsWebp();
    const ext = useWebp ? '.webp' : '.jpg';
    const contentType = useWebp ? 'image/webp' : 'image/jpeg';
    const outputPath = join(tempDir, `thumbnail${ext}`);

    this.logger.log(`Thumbnail step: writing ffmpeg output to ${outputPath}`);

    try {
      await new Promise<void>((resolve, reject) => {
        const command = ffmpeg(videoPath)
          .frames(1)
          .outputOptions(['-ss 2', '-vf scale=320:-1', '-an', '-sn', '-dn'])
          .videoCodec(useWebp ? 'libwebp' : 'mjpeg')
          .format(useWebp ? 'webp' : 'image2')
          .output(outputPath);

        command.once('end', () => {
          resolve();
        });

        const rejectWithError = (error: unknown) => {
          reject(error instanceof Error ? error : new Error(String(error)));
        };

        command.once('error', rejectWithError);
        command.run();
      });

      const thumbnailBuffer = await readFile(outputPath);

      this.logger.log(
        `Finished ffmpeg thumbnail capture with ${thumbnailBuffer.length} bytes at ${outputPath}`,
      );

      return { buffer: thumbnailBuffer, ext, contentType };
    } finally {
      await rm(tempDir, { recursive: true, force: true });
    }
  }

  private async supportsWebp(): Promise<boolean> {
    if (this.webpSupported !== undefined) return this.webpSupported;

    try {
      const { stdout } = await this.execFileP(ffmpegInstaller.path, ['-encoders']);
      this.webpSupported = /webp/i.test(stdout);
    } catch (err) {
      this.logger.warn(`Failed to check ffmpeg encoders: ${String(err)}`);
      this.webpSupported = false;
    }

    this.logger.log(`ffmpeg webp support: ${this.webpSupported}`);
    return this.webpSupported;
  }

  private findMoovOffset(buffer: Buffer): number {
    const moovTag = Buffer.from('moov');

    for (let index = 0; index <= buffer.length - 8; index++) {
      if (buffer.slice(index + 4, index + 8).equals(moovTag)) {
        return index;
      }
    }

    return -1;
  }

  private deriveThumbnailKey(key: string, ext = '.jpg'): string {
    const previewKey = key.replace('/Original/', '/Preview/');

    return this.replaceExtension(previewKey, ext);
  }

  private deriveMoovKey(key: string): string {
    return this.replaceExtension(key, '.moov');
  }

  private replaceExtension(key: string, extension: string): string {
    if (key.includes('.')) {
      return key.replace(/\.[^./]+$/, extension);
    }

    return `${key}${extension}`;
  }

  private logSettledResult(
    taskName: string,
    key: string,
    result: PromiseSettledResult<void>,
  ): boolean {
    if (result.status === 'fulfilled') {
      this.logger.log(`${taskName} task completed for ${key}`);
      return false;
    }

    const reason =
      result.reason instanceof Error
        ? result.reason.message
        : String(result.reason);
    this.logger.error(`${taskName} task failed for ${key}: ${reason}`);
    return true;
  }
}
