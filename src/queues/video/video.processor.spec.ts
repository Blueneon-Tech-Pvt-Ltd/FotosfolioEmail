/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return */
import { VideoProcessor } from './video.processor';
import * as fsPromises from 'node:fs/promises';

jest.mock('node:fs/promises', () => {
  return {
    __esModule: true,
    ...jest.requireActual('node:fs/promises'),
    rm: jest.fn(),
    mkdtemp: jest.fn(),
  };
});

describe('VideoProcessor', () => {
  describe('buildFaststartRemuxArgs', () => {
    it('maps only browser-playable streams when remuxing MOV files to MP4', () => {
      const mockConfigService = {
        get: (_key: string, def: string) => def,
      } as any;
      const processor = new VideoProcessor(mockConfigService, {} as any);

      const args = processor['buildFaststartRemuxArgs'](
        '/tmp/input.mov',
        '/tmp/output.mp4',
      );

      expect(args).toEqual([
        '-hide_banner',
        '-y',
        '-i',
        '/tmp/input.mov',
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
        '/tmp/output.mp4',
      ]);
      const mapAllStreamIndex = args.findIndex(
        (arg, index) => arg === '-map' && args[index + 1] === '0',
      );
      expect(mapAllStreamIndex).toBe(-1);
    });
  });

  describe('cleanup on optimizeVideoForPlayback failures', () => {
    beforeEach(() => {
      (fsPromises.rm as jest.Mock).mockResolvedValue(undefined);
      (fsPromises.mkdtemp as jest.Mock).mockResolvedValue('/fake/temp/dir');
    });

    afterEach(() => {
      (fsPromises.rm as jest.Mock).mockReset();
      (fsPromises.mkdtemp as jest.Mock).mockReset();
    });

    it('cleans up tempDir if downloadRangeToTempFile throws an error', async () => {
      const mockConfigService = {
        get: (_key: string, def: string) => def,
      } as any;
      const mockVideoS3Service = {
        downloadRangeToTempFile: jest
          .fn()
          .mockRejectedValue(new Error('S3 download failed')),
      } as any;
      const processor = new VideoProcessor(
        mockConfigService,
        mockVideoS3Service,
      );

      const mockJob = {
        data: {
          key: 'Original/video.mp4',
        },
      } as any;

      await expect(
        processor['optimizeVideoForPlayback'](mockJob),
      ).rejects.toThrow(
        'Failed to optimize video for browser playback for Original/video.mp4: S3 download failed',
      );

      expect(fsPromises.mkdtemp).toHaveBeenCalled();
      expect(fsPromises.rm).toHaveBeenCalledWith('/fake/temp/dir', {
        recursive: true,
        force: true,
      });
    });
  });
});
