import { Injectable, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegStatic from 'ffmpeg-static';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { VideoProcessJob } from './video.job.interface';
import { VideoS3Service } from './video.s3.service';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

ffmpeg.setFfmpegPath(ffmpegStatic as string);

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

  // 30MB covers ~2 seconds at up to ~200 Mbps — enough for any real-world video format
  private readonly THUMBNAIL_HEAD_BYTES = 30 * 1024 * 1024;
  private readonly MOOV_SCAN_BYTES = 10 * 1024 * 1024;

  private async generateThumbnail(job: Job<VideoProcessJob>): Promise<void> {
    this.logger.log(`Thumbnail step: downloading first ${this.THUMBNAIL_HEAD_BYTES / 1024 / 1024}MB for ${job.data.key}`);

    // We purposely avoid passing an HTTPS URL to ffmpeg. Static ffmpeg binaries crash
    // (SIGSEGV) on many Linux environments due to DNS resolution differences between
    // the static musl/glibc resolver inside ffmpeg and the system resolver used by Node.
    // Instead, we fetch only the first 30MB via Node's AWS SDK and write to a temp file.
    // Extract extension from the S3 key so ffmpeg can detect the container format.
    // e.g. "Original/.../C2632.MP4" → ".MP4"
    const keyExtMatch = job.data.key.match(/(\.[^./]+)$/);
    const keyExt = keyExtMatch ? keyExtMatch[1].toLowerCase() : '.mp4';

    const tempDir = await mkdtemp(join(tmpdir(), 'fotosfolio-video-thumb-'));
    let sourcePath: string;
    try {
      sourcePath = await this.videoS3Service.downloadRangeToTempFile(
        job.data.key,
        this.THUMBNAIL_HEAD_BYTES,
        tempDir,
        keyExt,
      );
    } catch (err: any) {
      await rm(tempDir, { recursive: true, force: true }).catch(() => {});
      throw new Error(`Failed to download video head for thumbnail: ${err.message}`);
    }

    this.logger.log(`Thumbnail step: capturing frame for ${job.data.key}`);
    let thumbnailBuffer: Buffer;
    let ext: string;
    let contentType: string;
    try {
      ({ buffer: thumbnailBuffer, ext, contentType } = await this.captureThumbnail(sourcePath, tempDir));
    } catch (err: any) {
      if (err.message.includes('Invalid data found') || err.message.includes('moov atom not found')) {
        this.logger.warn(`Thumbnail capture failed with partial file. Retrying with full file for ${job.data.key}`);
        await rm(sourcePath, { force: true }).catch(() => {});
        sourcePath = await this.videoS3Service.downloadRangeToTempFile(
          job.data.key,
          undefined, // fetch full file
          tempDir,
          keyExt,
        );
        ({ buffer: thumbnailBuffer, ext, contentType } = await this.captureThumbnail(sourcePath, tempDir));
      } else {
        throw err;
      }
    } finally {
      await rm(tempDir, { recursive: true, force: true }).catch(() => {});
    }

    this.logger.log(
      `Thumbnail step: captured ${thumbnailBuffer!.length} bytes for ${job.data.key}`,
    );

    const thumbnailKey = this.deriveThumbnailKey(job.data.key, ext!);
    this.assertArtifactKey(job.data.key, thumbnailKey, 'thumbnail');

    if (thumbnailBuffer!.length === 0) {
      throw new Error(`Thumbnail generated empty buffer for ${thumbnailKey}`);
    }

    this.logger.log(`Thumbnail step: uploading ${thumbnailKey}`);
    await this.videoS3Service.uploadBuffer(thumbnailKey, thumbnailBuffer!, contentType!);

    this.logger.log(
      `Thumbnail generated for ${thumbnailKey} (${Math.round(thumbnailBuffer!.length / 1024)} KB)`,
    );
  }

  private async extractMoov(job: Job<VideoProcessJob>): Promise<void> {
    this.logger.log(`Moov step: requesting last ${this.MOOV_SCAN_BYTES / 1024 / 1024}MB bytes for ${job.data.key}`);
    const tailBuffer = await this.videoS3Service.getObjectRange(
      job.data.key,
      `bytes=-${this.MOOV_SCAN_BYTES}`,
    );
    const tailMoovBuffer = this.extractMoovBuffer(tailBuffer, job.data.key, 'tail');

    let moovBuffer = tailMoovBuffer;

    if (!moovBuffer) {
      this.logger.log(
        `Moov step: moov atom not found in tail for ${job.data.key}; requesting first ${this.MOOV_SCAN_BYTES / 1024 / 1024}MB bytes`,
      );
      const headBuffer = await this.videoS3Service.getObjectRange(
        job.data.key,
        `bytes=0-${this.MOOV_SCAN_BYTES - 1}`,
      );
      moovBuffer = this.extractMoovBuffer(headBuffer, job.data.key, 'head');
    }

    if (!moovBuffer) {
      this.logger.warn(`Moov atom not found in scanned ranges for ${job.data.key}; skipping moov extraction`);
      return;
    }

    const moovKey = this.deriveMoovKey(job.data.key);
    this.assertArtifactKey(job.data.key, moovKey, 'moov');

    this.logger.log(`Moov step: uploading ${moovKey}`);
    await this.videoS3Service.uploadBuffer(moovKey, moovBuffer, 'application/octet-stream');

    this.logger.log(`Moov atom extracted for ${moovKey} (${moovBuffer.length} bytes)`);
  }

  private extractMoovBuffer(buffer: Buffer, key: string, scanPosition: 'head' | 'tail'): Buffer | undefined {
    const moovOffset = this.findMoovOffset(buffer);

    if (moovOffset === -1) {
      return undefined;
    }

    const moovSize = this.readAtomSize(buffer, moovOffset);

    if (moovSize === undefined) {
      this.logger.warn(
        `Moov atom header is invalid or incomplete in ${scanPosition} range for ${key}; skipping moov extraction`,
      );
      return undefined;
    }

    const endOffset = moovOffset + moovSize;

    if (endOffset > buffer.length) {
      this.logger.warn(
        `Moov atom is larger than downloaded ${scanPosition} range for ${key}; skipping truncated moov extraction`,
      );
      return undefined;
    }

    const moovBuffer = buffer.slice(moovOffset, endOffset);

    if (moovBuffer.length === 0) {
      this.logger.warn(`Moov atom is empty in ${scanPosition} range for ${key}; skipping moov extraction`);
      return undefined;
    }

    return moovBuffer;
  }

  private async captureThumbnail(videoPath: string, tempDir: string): Promise<{ buffer: Buffer; ext: string; contentType: string }> {
    this.logger.log('Starting ffmpeg thumbnail capture');

    const useWebp = await this.supportsWebp();
    const ext = useWebp ? '.webp' : '.jpg';
    const contentType = useWebp ? 'image/webp' : 'image/jpeg';
    const outputPath = join(tempDir, `thumbnail${ext}`);

    this.logger.log(`Thumbnail step: writing ffmpeg output to ${outputPath}`);

    await new Promise<void>((resolve, reject) => {
      const command = ffmpeg(videoPath)
        .seekInput(2)          // input seek: fast, no HTTPS involved — file is local
        .frames(1)
        .outputOptions(['-vf scale=320:-1', '-an', '-sn', '-dn'])
        .videoCodec(useWebp ? 'libwebp' : 'mjpeg')
        .format(useWebp ? 'webp' : 'image2')
        .output(outputPath);

      command.once('end', resolve);
      command.once('error', (err: unknown) => {
        reject(err instanceof Error ? err : new Error(String(err)));
      });
      command.run();
    });

    const thumbnailBuffer = await readFile(outputPath);

    this.logger.log(
      `Finished ffmpeg thumbnail capture with ${thumbnailBuffer.length} bytes at ${outputPath}`,
    );

    return { buffer: thumbnailBuffer, ext, contentType };
  }

  private async supportsWebp(): Promise<boolean> {
    if (this.webpSupported !== undefined) return this.webpSupported;

    try {
      const { stdout } = await this.execFileP(ffmpegStatic as string, ['-encoders']);
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

  private readAtomSize(buffer: Buffer, offset: number): number | undefined {
    if (offset + 8 > buffer.length) {
      return undefined;
    }

    const size = buffer.readUInt32BE(offset);

    if (size === 0) {
      return buffer.length - offset;
    }

    if (size === 1) {
      if (offset + 16 > buffer.length) {
        return undefined;
      }

      const largeSize = buffer.readBigUInt64BE(offset + 8);

      if (largeSize > BigInt(Number.MAX_SAFE_INTEGER)) {
        return undefined;
      }

      const numericSize = Number(largeSize);
      return numericSize >= 16 ? numericSize : undefined;
    }

    return size >= 8 ? size : undefined;
  }

  private deriveThumbnailKey(key: string, ext = '.jpg'): string {
    const previewKey = key
      .replace(/^Original\//, 'Preview/')
      .replace('/Original/', '/Preview/');

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

  private assertArtifactKey(sourceKey: string, artifactKey: string, taskName: string): void {
    if (artifactKey === sourceKey) {
      throw new Error(
        `${taskName} artifact key resolved to source video key ${sourceKey}; refusing to overwrite original video`,
      );
    }
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
