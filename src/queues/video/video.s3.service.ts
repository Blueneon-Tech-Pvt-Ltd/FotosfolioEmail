import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { pipeline } from 'stream/promises';
import { createWriteStream } from 'node:fs';
import { join } from 'node:path';
import { Readable } from 'stream';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

@Injectable()
export class VideoS3Service {
  private readonly logger = new Logger(VideoS3Service.name);
  private readonly client: S3Client;
  private readonly bucket: string;
  private readonly urlExpiresInSeconds: number;

  constructor(private readonly configService: ConfigService) {
    this.bucket =
      this.configService.get<string>('S3_BUCKET') ??
      this.configService.get<string>('AWS_S3_BUCKET') ??
      '';

    if (!this.bucket) {
      throw new Error('S3_BUCKET is not configured');
    }

    const region =
      this.configService.get<string>('AWS_REGION') ?? 'us-east-1';
    const endpoint =
      this.configService.get<string>('S3_ENDPOINT') ??
      this.configService.get<string>('AWS_S3_ENDPOINT');
    const forcePathStyle =
      (this.configService.get<string>('S3_FORCE_PATH_STYLE') ?? 'false') ===
      'true';

    const accessKeyId = this.configService.get<string>('AWS_ACCESS_KEY_ID');
    const secretAccessKey =
      this.configService.get<string>('AWS_SECRET_ACCESS_KEY');
    const sessionToken = this.configService.get<string>('AWS_SESSION_TOKEN');

    this.client = new S3Client({
      region,
      endpoint,
      forcePathStyle,
      credentials:
        accessKeyId && secretAccessKey
          ? {
              accessKeyId,
              secretAccessKey,
              sessionToken,
            }
          : undefined,
    });

    this.urlExpiresInSeconds = Number(
      this.configService.get<string>('S3_SIGNED_URL_EXPIRES_IN_SECONDS') ?? 900,
    );

    this.logger.log(`Video S3 service initialized for bucket ${this.bucket}`);
  }

  async getPresignedReadUrl(key: string): Promise<string> {
    return getSignedUrl(
      this.client,
      new GetObjectCommand({
        Bucket: this.bucket,
        Key: key,
      }),
      {
        expiresIn: this.urlExpiresInSeconds,
      },
    );
  }

  async getPresignedPutUrl(
    key: string,
    contentType: string,
  ): Promise<string> {
    return getSignedUrl(
      this.client,
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        ContentType: contentType,
      }),
      {
        expiresIn: this.urlExpiresInSeconds,
      },
    );
  }

  // Download an object range (e.g. last N bytes). If range is omitted, returns full object.
  async getObjectRange(key: string, range?: string): Promise<Buffer> {
    const cmd = new GetObjectCommand({ Bucket: this.bucket, Key: key, Range: range });
    const res = await this.client.send(cmd);
    const body = res.Body as Readable;

    const chunks: Buffer[] = [];
    for await (const chunk of body) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }

    return Buffer.concat(chunks);
  }

  /**
   * Download the first `bytes` bytes of an S3 object to a temp file using a single
   * S3 range request. This is the preferred way to feed data to ffmpeg — it avoids
   * exposing HTTPS URLs to ffmpeg's static binary, which crashes (SIGSEGV) on some
   * environments when trying to resolve DNS over HTTPS.
   *
   * For a 2-second seek at typical video bitrates (up to ~200Mbps), 30MB covers well
   * over 1 second of data for any real-world file.
   *
   * @param key     S3 object key
   * @param bytes   Number of bytes to fetch from the start of the object (default 30MB)
   * @param tempDir Directory to write the temp file into
   * @returns       Absolute path to the written temp file
   */
  /**
   * @param ext  File extension including the dot, e.g. '.mp4'. Used to name the temp file
   *             so ffmpeg can detect the container format without an explicit -f flag.
   */
  async downloadRangeToTempFile(
    key: string,
    bytes: number | undefined,
    tempDir: string,
    ext: string,
  ): Promise<string> {
    const range = bytes ? `bytes=0-${bytes - 1}` : undefined;
    const cmd = new GetObjectCommand({ Bucket: this.bucket, Key: key, Range: range });
    const res = await this.client.send(cmd);
    const body = res.Body as Readable;

    const outPath = join(tempDir, `video_head${ext}`);
    const ws = createWriteStream(outPath);
    await pipeline(body, ws);

    if (bytes) {
      this.logger.log(`Downloaded first ${(bytes / 1024 / 1024).toFixed(0)}MB of ${key} to ${outPath}`);
    } else {
      this.logger.log(`Downloaded full file ${key} to ${outPath}`);
    }

    return outPath;
  }

  // Upload a buffer to S3 with retries
  async uploadBuffer(key: string, buffer: Buffer, contentType: string, attempts = 3): Promise<void> {
    let lastErr: any;
    for (let i = 0; i < attempts; i++) {
      try {
        this.logger.log(`Uploading ${buffer.length} bytes to ${key} (${contentType})`);

        const cmd = new PutObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Body: buffer,
          ContentType: contentType,
        });

        await this.client.send(cmd);
        return;
      } catch (err) {
        lastErr = err;
        this.logger.warn(`Upload attempt ${i + 1} failed for ${key}: ${err}`);
        await new Promise((r) => setTimeout(r, 200 * (i + 1)));
      }
    }

    throw lastErr;
  }

}
