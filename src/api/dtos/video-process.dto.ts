import { IsNotEmpty, IsString } from 'class-validator';

export class VideoProcessDto {
  @IsString()
  @IsNotEmpty()
  key: string;

  @IsString()
  @IsNotEmpty()
  uploadId: string;
}
