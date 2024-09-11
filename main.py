import os
from video_processor import VideoProcessor


def run(input_dir, out_dir, tmp_dir, rev_dir):
    vid_processor = VideoProcessor()
    for root, dirs, files in os.walk(input_dir):
        for i, file in enumerate(files):
            if file.endswith(('.mp4', '.ts')):
                in_path = os.path.join(root, file)
                print(f"Processing: {in_path}")
                print(f"Processing remaining: {len(files)} : {len(files) - (i + 1)} |  {file}")
                vid_processor.reverse_video(in_path, out_dir, tmp_dir, rev_dir, file)


if __name__ == '__main__':
    input_path = 'video.mp4'
    temp_dir = './output/temp'
    reverse_dir = './output/reversed'
    dir_path = './test_dir'
    output_dir = './output'

    # 단일 파일 처리
    video_processor = VideoProcessor()
    video_processor.reverse_video(input_path, output_dir, temp_dir, reverse_dir, 'reversed_done')

    # 디렉토리 내 모든 파일 처리
    # run(dir_path, output_dir, temp_dir, reverse_dir)
