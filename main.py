import pprint
import itertools
import os
import re
import subprocess
import json
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

from rich.progress import Progress

max_worker = 1
gpu_devices = itertools.cycle([0])


def get_video_info(input_path):
    command = [
        'ffprobe',
        '-v', 'error',
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        input_path
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    result = json.loads(result.stdout)
    return result


def process_bar(process):
    with Progress() as progress:
        task = progress.add_task("[cyan]Processing...", total=100)

        for line in process.stdout:
            if "Duration" in line:
                total_seconds = extract_seconds(line, "Duration: (.*?),")
                if total_seconds is not None:
                    progress.update(task, total=total_seconds)

            if "time=" in line:
                elapsed_seconds = extract_seconds(line, "time=(.*?) ")
                if elapsed_seconds is not None:
                    progress.update(task, completed=elapsed_seconds)


def extract_seconds(line, pattern):
    match = re.search(pattern, line)
    if match:
        time_str = match.group(1)
        try:
            hours, minutes, seconds = map(float, re.split(':', time_str))
            return hours * 3600 + minutes * 60 + seconds
        except ValueError:
            print(f"Invalid time format: {time_str}")
    return None


def reverse_video(input_path, output_dir, temp_dir, reversed_dir, output_name):
    segment_duration = 10

    for directory in [temp_dir, reversed_dir]:
        if os.path.exists(directory):
            shutil.rmtree(directory)
        os.makedirs(directory, exist_ok=True)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    split_video(input_path, segment_duration, temp_dir)
    reverse_segment(temp_dir, reversed_dir)
    concatenate_segments(reversed_dir, output_path=f'{output_dir}/{output_name}.mp4')


def split_video(input_path, segment_duration, temp_dir):
    print("split video")

    command = [
        'ffmpeg',
        '-i', input_path,
        '-map', '0:v',
        '-map', '0:a',
        '-start_at_zero',
        '-segment_time', str(segment_duration),
        '-f', 'segment',
        '-break_non_keyframes', '0',
        '-reset_timestamps', '1',
        '-avoid_negative_ts', 'make_zero',
        '-force_key_frames', f"expr:gte(t,n_forced*{segment_duration})",  # 강제 키프레임 설정
        '-c', 'copy',
        f'{temp_dir}/segment%010d.ts',
        '-y'
    ]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    process_bar(process)


def reverse_segment(temp_dir, reversed_dir):
    """분할된 모든 비디오 세그먼트를 역순으로 만듭니다."""
    if not os.path.exists(reversed_dir):
        os.makedirs(reversed_dir)

    segment_files = sorted(os.listdir(temp_dir))
    total_segments = len(segment_files)

    with ThreadPoolExecutor(max_workers=max_worker) as executor:
        future_to_segment = {
            executor.submit(
                process_segment,
                os.path.join(temp_dir, seg_file),
                os.path.join(reversed_dir, seg_file),
                next(gpu_devices)
            ): seg_file
            for seg_file in segment_files
        }

        count = 0
        for i, future in enumerate(as_completed(future_to_segment), 1):
            segment = future_to_segment[future]
            gpu_device = next(gpu_devices)  # 현재 작업에 할당된 GPU 번호를 가져옴
            try:
                future.result()
                count += 1
                print(
                    f"작업 완료: {segment} | 남은 작업 수: {total_segments - i} | 완료된 작업 수: {count} | 총 작업 수: {total_segments} | 할당 GPU: {gpu_device}")
            except subprocess.CalledProcessError as exc:
                print(f"{segment} 처리 중 ffmpeg 에러 발생 (GPU {gpu_device}): {exc}")
            except Exception as exc:
                print(f"{segment} 처리 중 알 수 없는 에러 발생 (GPU {gpu_device}): {exc}")


def process_segment(input_path, output_path, gpu_deivce):
    """하나의 비디오 세그먼트를 역순으로 만듭니다."""
    command = [
        'ffmpeg',
        '-hwaccel', 'cuda',
        '-hwaccel_device', f'{gpu_deivce}',
        '-i', input_path,
        '-map', '0:v',
        '-map', '0:a',
        '-start_at_zero',
        '-vf', 'reverse,setpts=PTS-STARTPTS',  # 비디오 프레임 역순 및 타임스탬프 조정
        '-af', 'areverse,asetpts=PTS-STARTPTS',  # 오디오 프레임 역순 및 타임스탬프 조정
        '-c:v', 'h264_nvenc',
        # '-gpu', f'{gpu_deivce}',
        '-c:a', 'aac',
        '-avoid_negative_ts', 'make_zero',
        # '-vsync', '0',
        # '-an',
        '-async', '1',
        '-b:v', '8000k',
        output_path,
        '-y'
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    # process_bar(process)
    output, errors = process.communicate()  # 프로세스 완료 대기
    if process.returncode != 0:
        pprint.pprint(f"에러 발생 (첫 시도): {output} | {errors}")
        pprint.pprint(f"gpu 재처리: {input_path} -> {output_path}")

        # 에러 발생 시 재시도
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        output, errors = process.communicate()  # 프로세스 완료 대기
        if process.returncode != 0:
            raise Exception(f"에러 발생 종료 (재시도 후): {output} | {errors}")


def concatenate_segments(reversed_dir, output_path):
    print("concatenate segments")
    with open('segments.txt', 'w') as f:
        for segment_file in sorted(os.listdir(reversed_dir), reverse=True):
            f.write(f"file '{os.path.join(reversed_dir, segment_file)}'\n")

    command = [
        'ffmpeg',
        '-start_at_zero',
        '-f', 'concat',
        '-safe', '0',
        '-i', 'segments.txt',
        '-fflags', '+genpts',  # PTS 재생성
        '-c', 'copy',
        '-threads', '0',
        f'{output_path}',
        '-progress', '-',  # 진행률 표시
        '-y'
    ]

    subprocess.run(command, universal_newlines=True)
    print("reversed Done")
    os.remove('segments.txt')


def run(dir_path, output_dir, temp_dir, reverse_dir):
    for root, dirs, files in os.walk(dir_path):
        for i, file in enumerate(files):
            if file.endswith(('.mp4', '.ts')):
                input_path = os.path.join(root, file)
                print(f"Processing: {input_path}")
                print(f"Processing remaining: {len(files)} : {len(files) - (i + 1)} |  {file}")
                reverse_video(input_path, output_dir, temp_dir, reverse_dir, file)


if __name__ == '__main__':
    input_path = 'input.mp4'
    temp_dir = './output/temp'
    reverse_dir = './output/reversed'
    dir_path = './test_dir'
    output_dir = './output'

    # run(dir_path, output_dir, divide_tmp, merge_tmp, temp_dir, reverse_dir)
    reverse_video(input_path, output_dir, temp_dir, reverse_dir, 'reversed_done')
