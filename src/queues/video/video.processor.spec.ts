import { VideoProcessor } from './video.processor';

describe('VideoProcessor', () => {
  describe('buildFaststartRemuxArgs', () => {
    it('maps only browser-playable streams when remuxing MOV files to MP4', () => {
      const processor = new VideoProcessor({} as any);

      const args = processor['buildFaststartRemuxArgs']('/tmp/input.mov', '/tmp/output.mp4');

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
});
