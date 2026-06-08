import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { CompletedPart } from '@aws-sdk/client-s3';
import { Job } from 'bullmq';
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegStatic from 'ffmpeg-static';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { VideoProcessJob } from './video.job.interface';
import { VideoS3Service } from './video.s3.service';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Readable } from 'node:stream';

ffmpeg.setFfmpegPath(ffmpegStatic as string);

@Injectable()
export class VideoProcessor {
  private readonly logger = new Logger(VideoProcessor.name);
  private webpSupported?: boolean;
  private execFileP = promisify(execFile);

  private readonly previewStatusBaseUrl: string;
  private readonly previewStatusPath = '/uploads/bulk/preview-status';

  constructor(
    private readonly configService: ConfigService,
    private readonly videoS3Service: VideoS3Service,
  ) {
    this.previewStatusBaseUrl = this.configService.get(
      'MAIN_BACKEND_URL',
      'https://prod.fotosfolio.com',
    );
  }

  async process(job: Job<VideoProcessJob>): Promise<void> {
    this.logger.log(
      `Starting video job ${job.id ?? 'unknown'} for ${job.data.key} (uploadId: ${job.data.uploadId})`,
    );

    this.logger.log(`Step 1/4: generating thumbnail for ${job.data.key}`);
    await this.generateThumbnail(job);

    this.logger.log(`Step 2/4: thumbnail generated for ${job.data.key}`);

    await job.updateProgress(50);

    this.logger.log(`Step 3/4: optimizing video for browser playback for ${job.data.key}`);
    await this.optimizeVideoForPlayback(job);
    await job.updateProgress(90);

    await this.notifyPreviewStatus([job.data.key]);

    this.logger.log(`Step 4/4: marking ${job.data.key} as complete`);
    await job.updateProgress(100);

    this.logger.log(
      `Finished video job ${job.id ?? 'unknown'} for ${job.data.key}`,
    );
  }

  // 30MB covers ~2 seconds at up to ~200 Mbps — enough for any real-world video format
  private readonly THUMBNAIL_HEAD_BYTES = 30 * 1024 * 1024;
  private readonly MOOV_SCAN_BYTES = 10 * 1024 * 1024;
  private readonly MULTIPART_PART_SIZE = 8 * 1024 * 1024;

  /**
   * Container formats that store the moov/index atom at the END of the file by default.
   * A partial head download of these formats is unreadable by ffmpeg, so we skip
   * straight to a full-file download when generating thumbnails.
   */
  private readonly MOOV_AT_END_FORMATS = new Set(['.mov', '.m4v', '.qt']);

  private async generateThumbnail(job: Job<VideoProcessJob>): Promise<void> {
    // Extract extension from the S3 key so ffmpeg can detect the container format.
    // e.g. "Original/.../C2632.MP4" → ".mp4"
    const keyExtMatch = job.data.key.match(/(\.[^./]+)$/);
    const keyExt = keyExtMatch ? keyExtMatch[1].toLowerCase() : '.mp4';

    // .mov / .m4v / .qt files store their moov atom at the END of the file by default.
    // Feeding ffmpeg a truncated head of these formats always fails with
    // "Invalid data found when processing input" — skip straight to a full-file
    // download so we don't waste 6 ffmpeg invocations before the fallback kicks in.
    const moovAtEnd = this.MOOV_AT_END_FORMATS.has(keyExt);
    const headBytes = moovAtEnd ? undefined : this.THUMBNAIL_HEAD_BYTES;

    if (moovAtEnd) {
      this.logger.log(
        `Thumbnail step: ${keyExt} stores moov at end — downloading full file for ${job.data.key}`,
      );
    } else {
      this.logger.log(
        `Thumbnail step: downloading first ${this.THUMBNAIL_HEAD_BYTES / 1024 / 1024}MB for ${job.data.key}`,
      );
    }

    // We purposely avoid passing an HTTPS URL to ffmpeg. Static ffmpeg binaries crash
    // (SIGSEGV) on many Linux environments due to DNS resolution differences between
    // the static musl/glibc resolver inside ffmpeg and the system resolver used by Node.
    // Instead, we fetch the bytes via Node's AWS SDK and write to a temp file.
    const tempDir = await mkdtemp(join(tmpdir(), 'fotosfolio-video-thumb-'));
    let sourcePath: string | undefined;
    let thumbnailBuffer: Buffer;
    let ext: string;
    let contentType: string;
    try {
      sourcePath = await this.videoS3Service.downloadRangeToTempFile(
        job.data.key,
        headBytes,
        tempDir,
        keyExt,
      );
    } catch (err: any) {
      this.logger.warn(
        `Failed to download video${moovAtEnd ? '' : ' head'} for thumbnail for ${job.data.key}: ${this.formatError(err)}. Uploading fallback thumbnail.`,
      );
      ({ buffer: thumbnailBuffer, ext, contentType } = await this.createFallbackThumbnail(tempDir));
    }

    try {
      if (sourcePath) {
        this.logger.log(`Thumbnail step: capturing frame for ${job.data.key}`);
        try {
          ({ buffer: thumbnailBuffer, ext, contentType } = await this.captureThumbnail(sourcePath, tempDir));
        } catch (err: any) {
          if (moovAtEnd) {
            // We already downloaded the full file — no point retrying; go to fallback.
            this.logger.warn(
              `Thumbnail capture failed for full ${keyExt} file for ${job.data.key}: ${this.formatError(err)}. Uploading fallback thumbnail.`,
            );
            ({ buffer: thumbnailBuffer, ext, contentType } = await this.createFallbackThumbnail(tempDir));
          } else {
            // Partial file was unreadable (e.g. moov not in first ${headBytes} bytes).
            // Retry with the full file before giving up.
            this.logger.warn(
              `Thumbnail capture failed with partial file for ${job.data.key}: ${this.formatError(err)}. Retrying with full file.`,
            );
            await rm(sourcePath, { force: true }).catch(() => {});
            sourcePath = await this.videoS3Service.downloadRangeToTempFile(
              job.data.key,
              undefined,
              tempDir,
              keyExt,
            );

            try {
              ({ buffer: thumbnailBuffer, ext, contentType } = await this.captureThumbnail(sourcePath, tempDir));
            } catch (fullFileErr) {
              this.logger.warn(
                `Thumbnail capture failed with full file for ${job.data.key}: ${this.formatError(fullFileErr)}. Uploading fallback thumbnail.`,
              );
              ({ buffer: thumbnailBuffer, ext, contentType } = await this.createFallbackThumbnail(tempDir));
            }
          }
        }
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

  private async optimizeVideoForPlayback(job: Job<VideoProcessJob>): Promise<void> {
    const key = job.data.key;
    const tempDir = await mkdtemp(join(tmpdir(), 'fotosfolio-video-optimize-'));
    const keyExtMatch = key.match(/(\.[^./]+)$/);
    const keyExt = keyExtMatch ? keyExtMatch[1].toLowerCase() : '.mp4';
    const sourcePath = await this.videoS3Service.downloadRangeToTempFile(key, undefined, tempDir, keyExt);
    const outputPath = join(tempDir, 'video_faststart.mp4');

    try {
      this.logger.log(`Playback optimization step: remuxing ${key} with faststart`);
      await this.execFileP(
        ffmpegStatic as string,
        this.buildFaststartRemuxArgs(sourcePath, outputPath),
      );

      const outputStat = await stat(outputPath);
      if (outputStat.size === 0) {
        throw new Error(`ffmpeg faststart remux produced empty output for ${key}`);
      }

      const uploadId = await this.videoS3Service.createMultipartUpload(key, 'video/mp4');
      try {
        const completedParts = await this.uploadMultipartStream(key, uploadId, createReadStream(outputPath));
        if (completedParts.length === 0) {
          throw new Error(`Faststart upload produced no parts for ${key}`);
        }
        await this.videoS3Service.completeMultipartUpload(key, uploadId, completedParts);
      } catch (err) {
        await this.videoS3Service.abortMultipartUpload(key, uploadId).catch((abortErr) => {
          this.logger.error(`Failed to abort multipart upload for ${key}: ${this.formatError(abortErr)}`);
        });
        throw err;
      }

      this.logger.log(`Faststart video replaced original object for ${key} (${outputStat.size} bytes)`);
    } catch (err) {
      throw new Error(`Failed to optimize video for browser playback for ${key}: ${this.formatError(err)}`);
    } finally {
      await rm(tempDir, { recursive: true, force: true });
    }
  }

  private buildFaststartRemuxArgs(sourcePath: string, outputPath: string): string[] {
    return [
      '-hide_banner',
      '-y',
      '-i',
      sourcePath,
      '-map',
      '0:v:0',
      '-map',
      '0:a?',
      '-map_metadata',
      '0',
      '-map_chapters',
      '-1',
      '-c',
      'copy',
      '-sn',
      '-dn',
      '-movflags',
      '+faststart',
      outputPath,
    ];
  }

  private async uploadMultipartStream(
    key: string,
    uploadId: string,
    output: Readable,
  ): Promise<CompletedPart[]> {
    const completedParts: CompletedPart[] = [];
    let partNumber = 1;
    let pending = Buffer.alloc(0);

    for await (const chunk of output) {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      pending = pending.length === 0 ? buffer : Buffer.concat([pending, buffer]);

      while (pending.length >= this.MULTIPART_PART_SIZE) {
        const partBuffer = pending.subarray(0, this.MULTIPART_PART_SIZE);
        pending = pending.subarray(this.MULTIPART_PART_SIZE);
        completedParts.push(
          await this.videoS3Service.uploadMultipartPart(key, uploadId, partNumber, partBuffer),
        );
        partNumber += 1;
      }
    }

    if (pending.length > 0) {
      completedParts.push(
        await this.videoS3Service.uploadMultipartPart(key, uploadId, partNumber, pending),
      );
    }

    return completedParts;
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

    // Fast-fail: probe the file before running up to 6 ffmpeg thumbnail attempts.
    // If ffmpeg cannot open the container at all (e.g. truncated file, moov atom
    // missing from a partial .mov download), every attempt would fail with
    // "Invalid data found when processing input". Detecting this upfront avoids
    // the wasted work and lets the caller fall back to a full-file download sooner.
    const probeable = await this.canProbeFile(videoPath);
    if (!probeable) {
      throw new Error(
        `ffmpeg cannot probe ${videoPath}: file is likely truncated or has its moov/index atom at the end`,
      );
    }

    const useWebp = await this.supportsWebp();
    const attempts = [
      ...(useWebp ? [{ ext: '.webp', contentType: 'image/webp', codec: 'libwebp', format: 'webp' }] : []),
      { ext: '.jpg', contentType: 'image/jpeg', codec: 'mjpeg', format: 'image2' },
    ];
    const seekTimes = [2, 1, 0];
    let lastErr: unknown;

    for (const attempt of attempts) {
      for (const seekTime of seekTimes) {
        const outputPath = join(tempDir, `thumbnail_${seekTime}s${attempt.ext}`);
        this.logger.log(`Thumbnail step: writing ffmpeg output to ${outputPath} at ${seekTime}s`);

        try {
          await this.captureThumbnailAttempt(
            videoPath,
            outputPath,
            seekTime,
            attempt.codec,
            attempt.format,
          );

          const thumbnailBuffer = await readFile(outputPath);
          if (thumbnailBuffer.length === 0) {
            throw new Error(`ffmpeg wrote empty thumbnail at ${outputPath}`);
          }

          this.logger.log(
            `Finished ffmpeg thumbnail capture with ${thumbnailBuffer.length} bytes at ${outputPath}`,
          );

          return {
            buffer: thumbnailBuffer,
            ext: attempt.ext,
            contentType: attempt.contentType,
          };
        } catch (err) {
          lastErr = err;
          this.logger.warn(
            `Thumbnail attempt failed at ${seekTime}s as ${attempt.ext} for ${videoPath}: ${this.formatError(err)}`,
          );
          await rm(outputPath, { force: true }).catch(() => {});
        }
      }
    }

    throw new Error(`Failed to capture thumbnail after all attempts: ${this.formatError(lastErr)}`);
  }

  /**
   * Returns true if ffmpeg can open and probe the given file.
   * Used as a fast-fail guard before running multiple thumbnail attempts on a
   * file that may be unreadable (e.g. a truncated partial download).
   */
  private async canProbeFile(videoPath: string): Promise<boolean> {
    try {
      // `-t 0` + null muxer: reads just enough to parse the container headers.
      await this.execFileP(ffmpegStatic as string, [
        '-v', 'error',
        '-i', videoPath,
        '-t', '0',
        '-f', 'null',
        '-',
      ]);
      return true;
    } catch {
      return false;
    }
  }

  private async captureThumbnailAttempt(
    videoPath: string,
    outputPath: string,
    seekTime: number,
    codec: string,
    format: string,
  ): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const command = ffmpeg(videoPath)
        .seekInput(seekTime)
        .frames(1)
        .outputOptions(['-vf scale=320:-1', '-an', '-sn', '-dn'])
        .videoCodec(codec)
        .format(format)
        .output(outputPath);

      command.once('end', resolve);
      command.once('error', (err: unknown) => {
        reject(err instanceof Error ? err : new Error(String(err)));
      });
      command.run();
    });
  }

  private async createFallbackThumbnail(tempDir: string): Promise<{ buffer: Buffer; ext: string; contentType: string }> {
    this.logger.warn(`Thumbnail step: using embedded fallback thumbnail in ${tempDir}`);
    const buffer = Buffer.from(
      'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=',
      'base64',
    );

    return {
      buffer,
      ext: '.png',
      contentType: 'image/png',
    };
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

  private formatError(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }

  /**
   * PATCH /uploads/bulk/preview-status on the main backend after a successful
   * video optimisation.  Non-fatal: a failure is logged but never rethrows so
   * the BullMQ job is still marked as completed.
   */
  private async notifyPreviewStatus(keys: string[]): Promise<void> {
    const fileKeys = keys.map((k) => this.stripToFileKey(k));

    try {
      const response = await fetch(
        `${this.previewStatusBaseUrl}${this.previewStatusPath}`,
        {
          method: 'PATCH',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ fileKeys }),
        },
      );

      if (!response.ok) {
        const text = await response.text().catch(() => '');
        this.logger.warn(
          `Preview status update responded with ${response.status} for keys [${fileKeys.join(', ')}]: ${text}`,
        );
        return;
      }

      this.logger.log(
        `✅ Preview status updated for keys: [${fileKeys.join(', ')}]`,
      );
    } catch (err) {
      // Non-fatal — the video is already optimised; don't fail the job.
      this.logger.error(
        `Failed to update preview status for keys [${fileKeys.join(', ')}]: ${this.formatError(err)}`,
      );
    }
  }

  /**
   * Returns a bare S3 object key by stripping the domain, bucket prefix, and
   * query parameters from whatever value arrives in the job.
   *
   * Examples:
   *   'Original/abc/video.mp4'                              → 'Original/abc/video.mp4'
   *   'https://cdn.example.com/my-bucket/Original/a.mp4'   → 'Original/a.mp4'
   *   'https://s3.amazonaws.com/bucket/k.mp4?X-Amz-Sig=…' → 'k.mp4'
   */
  private stripToFileKey(keyOrUrl: string): string {
    if (!keyOrUrl.startsWith('http://') && !keyOrUrl.startsWith('https://')) {
      // Already a bare key — strip any accidental query string.
      return keyOrUrl.split('?')[0];
    }

    try {
      const url = new URL(keyOrUrl);
      // pathname begins with '/' then optionally a bucket segment for path-style URLs.
      const segments = url.pathname.replace(/^\//, '').split('/');

      // If the leading segment is the S3 bucket name (not a known key prefix),
      // remove it so only the object key remains.
      const knownKeyPrefixes = ['Original', 'Preview', 'LowResolution', 'Thumbnail'];
      if (segments.length > 1 && !knownKeyPrefixes.includes(segments[0])) {
        segments.shift();
      }

      return segments.join('/');
    } catch {
      // URL parsing failed — treat as a bare key.
      return keyOrUrl.split('?')[0];
    }
  }
}
