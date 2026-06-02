import { Injectable, Logger } from '@nestjs/common';
import type { CompletedPart } from '@aws-sdk/client-s3';
import { Job } from 'bullmq';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegStatic from 'ffmpeg-static';
import { execFile, spawn } from 'child_process';
import { promisify } from 'util';
import { VideoProcessJob } from './video.job.interface';
import { VideoS3Service } from './video.s3.service';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';

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

    this.logger.log(`Step 1/4: generating thumbnail and extracting moov for ${job.data.key}`);
    const [thumbnailResult, moovResult] = await Promise.allSettled([
      this.generateThumbnail(job),
      this.extractMoov(job),
    ]);

    this.logger.log(`Step 2/4: collecting task results for ${job.data.key}`);
    const thumbnailFailed = this.logSettledResult('thumbnail', job.data.key, thumbnailResult);
    const moovFailed = this.logSettledResult('moov', job.data.key, moovResult);

    if (thumbnailFailed || moovFailed) {
      throw new Error(`One or more tasks failed for ${job.data.key}`);
    }

    await job.updateProgress(50);

    this.logger.log(`Step 3/4: fragmenting video for ${job.data.key}`);
    await this.fragmentVideo(job);
    await job.updateProgress(90);

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

  private async fragmentVideo(job: Job<VideoProcessJob>): Promise<void> {
    const key = job.data.key;
    const sourceStream = await this.videoS3Service.getObjectStream(key);
    const uploadId = await this.videoS3Service.createMultipartUpload(key, 'video/mp4');
    const ffmpegProcess = spawn(ffmpegStatic as string, [
      '-hide_banner',
      '-i',
      'pipe:0',
      '-c',
      'copy',
      '-movflags',
      'frag_keyframe+default_base_moof+empty_moov',
      '-f',
      'mp4',
      'pipe:1',
    ], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    let stderr = '';
    const stderrLimit = 256 * 1024;
    ffmpegProcess.stderr.setEncoding('utf8');
    ffmpegProcess.stderr.on('data', (chunk: string) => {
      stderr = (stderr + chunk).slice(-stderrLimit);
    });

    const exitPromise = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      ffmpegProcess.once('error', reject);
      ffmpegProcess.once('close', (code, signal) => resolve({ code, signal }));
    });

    const killFfmpeg = () => {
      if (!ffmpegProcess.killed) {
        ffmpegProcess.kill('SIGKILL');
      }
    };
    const inputPromise = pipeline(sourceStream, ffmpegProcess.stdin).catch((err) => {
      if ((err as NodeJS.ErrnoException).code !== 'EPIPE') {
        killFfmpeg();
      }
      throw err;
    });
    const uploadPromise = this.uploadFragmentedOutput(key, uploadId, ffmpegProcess.stdout).catch((err) => {
      killFfmpeg();
      throw err;
    });
    const results = await Promise.allSettled([
      exitPromise,
      uploadPromise,
      inputPromise,
    ]);

    const exitResult = results[0];
    const uploadResult = results[1];
    const inputResult = results[2];

    const abortMultipartUpload = async () => {
      await this.videoS3Service.abortMultipartUpload(key, uploadId).catch((err) => {
        this.logger.error(`Failed to abort multipart upload for ${key}: ${this.formatError(err)}`);
      });
    };

    if (exitResult.status === 'rejected') {
      await abortMultipartUpload();
      throw new Error(`Failed to start ffmpeg fragmentation for ${key}: ${this.formatError(exitResult.reason)}`);
    }

    const { code, signal } = exitResult.value;

    if (code !== 0) {
      await abortMultipartUpload();
      this.logger.error(
        `Fragmentation ffmpeg failed for ${key} with code ${code ?? 'null'} signal ${signal ?? 'none'}: ${stderr.trim()}`,
      );
      throw new Error(`ffmpeg fragmentation failed for ${key}`);
    }

    if (uploadResult.status === 'rejected') {
      await abortMultipartUpload();
      throw new Error(`Failed to upload fragmented output for ${key}: ${this.formatError(uploadResult.reason)}`);
    }

    if (inputResult.status === 'rejected') {
      await abortMultipartUpload();
      throw new Error(`Failed to stream source video into ffmpeg for ${key}: ${this.formatError(inputResult.reason)}`);
    }

    const completedParts = uploadResult.value;

    if (completedParts.length === 0) {
      await abortMultipartUpload();
      throw new Error(`ffmpeg fragmentation produced no output for ${key}`);
    }

    await this.videoS3Service.completeMultipartUpload(key, uploadId, completedParts);
    this.logger.log(`Fragmented video replaced original object for ${key}`);
  }

  private async uploadFragmentedOutput(
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

  private formatError(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
