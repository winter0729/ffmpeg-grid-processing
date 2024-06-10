import pprint
import itertools
import os
import subprocess
import json
import re
import shutil
import ffmpeg
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

max_worker = 8
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
    pbar = tqdm(total=None)
    for line in process.stdout:
        # 출력에서 "Duration: 00:00:
        # .04"와 같은 라인을 찾아서 전체 동영상 길이를 가져옵니다.
        if "Duration" in line:
            match = re.search("Duration: (.*?),", line)
            if match:
                time_str = match.group(1)
                try:
                    hours, minutes, seconds = map(float, re.split(':', time_str))
                    total_seconds = hours * 3600 + minutes * 60 + seconds
                    pbar.total = total_seconds
                    pbar.refresh()
                except ValueError:
                    # 유효하지 않은 시간 형식을 만났을 때의 처리
                    print(f"유효하지 않은 시간 형식: {time_str}")
                    continue  # 다음 라인으로 넘어갑니다.

            # 출력에서 "time=00:00:10.00"과 같은 라인을 찾아서 현재 진행 시간을 가져옵니다.
        if "time=" in line:
            match = re.search("time=(.*?) ", line)
            if match:
                time_str = match.group(1)
                try:
                    hours, minutes, seconds = map(float, re.split(':', time_str))
                    elapsed_seconds = hours * 3600 + minutes * 60 + seconds
                    pbar.n = elapsed_seconds
                    pbar.refresh()
                except ValueError:
                    # 유효하지 않은 시간 형식을 만났을 때의 처리
                    print(f"유효하지 않은 시간 형식: {time_str}")
                    continue  # 다음 라인으로 넘어갑니다.
    pbar.close()


def reverse_video(input_path, output_dir, temp_dir, reversed_dir, audio_reversed_dir, output_name):
    print("reverse video")
    # input_path = f"{output_dir}/merged.mp4"
    # output_path = f"{output_dir}/reversed.mp4"
    segment_duration = 10

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    if os.path.exists(reversed_dir):
        shutil.rmtree(reversed_dir)
    if os.path.exists(audio_reversed_dir):
        shutil.rmtree(audio_reversed_dir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    if not os.path.exists(reversed_dir):
        os.makedirs(reversed_dir)
    if not os.path.exists(audio_reversed_dir):
        os.makedirs(audio_reversed_dir)

    split_video(input_path, segment_duration, temp_dir)
    reverse_segment(temp_dir, reversed_dir)
    # reverse_audio(input_path, temp_dir, audio_reversed_dir)
    concatenate_segments(reversed_dir=reversed_dir, output_path=f'{output_dir}/{output_name}.mp4')
    # combine_audio_video(video_path=f"{temp_dir}/reversed.mp4", audio_path=f'{temp_dir}/reversed_audio.mp4', output_path=f'{output_dir}/{output_name}.mp4')


def split_video(input_path, segment_duration, temp_dir):
    print("split video")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

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

    # for segment_file in sorted(os.listdir(temp_dir)):
    #     print(f"remaining segment: {segment_file} | {len(os.listdir(temp_dir))})")
    #     input_path = os.path.join(temp_dir, segment_file)
    #     command = [
    #         'ffmpeg',
    #         '-i', input_path,
    #         '-vf', 'reverse',
    #         '-af', 'areverse',
    #         '-c:v', 'h264_nvenc',
    #         '-b:v', '8000k',
    #         f'{reversed_dir}/{segment_file}',
    #         '-y'
    #     ]

    #
    #     process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    #     process_bar(process)


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


def reverse_audio(input_path, temp_dir, audio_reversed_dir):
    print("reverse audio")
    if not os.path.exists(audio_reversed_dir):
        os.makedirs(audio_reversed_dir)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    if not os.path.exists(f"{temp_dir}/audio"):
        os.makedirs(f"{temp_dir}/audio")

    temp_dir_audio = f"{temp_dir}/audio"
    segment_duration = 10
    split_audio(input_path, segment_duration, temp_dir_audio)
    reverse_segment_audio_process(temp_dir_audio, audio_reversed_dir)
    concat_segments_audio(audio_reversed_dir, temp_dir)

    # command = [
    #     'ffmpeg',
    #     '-hwaccel', 'cuda',
    #     '-i', input_path,
    #     '-start_at_zero',
    #     '-threads', '0',
    #     '-map', '0:a',
    #     '-vn',
    #     '-progress', '-',  # 진행률 표시
    #     '-af', 'areverse',
    #     '-c:a', 'aac',
    #     '-channel_layout', 'stereo',
    #     f'{temp_dir}/reversed_audio.mp4',
    #     '-y'
    # ]
    #
    # process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    # process_bar(process)
    # print("reverse audio Done")


def split_audio(input_path, segment_duration, temp_dir):
    print("split audio")
    command = [
        'ffmpeg',
        '-i', input_path,
        '-map', '0:a',
        '-vn',
        '-c', 'copy',
        '-f', 'segment',
        '-segment_time', str(segment_duration),
        '-break_non_keyframes', '0',
        '-reset_timestamps', '1',
        '-channel_layout', 'stereo',
        '-y',
        f'{temp_dir}/audio_segment%010d.ts'
    ]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    process_bar(process)


def reverse_segment_audio_process(temp_dir, reversed_dir):
    """분할된 모든 오디오 세그먼트를 역순으로 만듭니다."""
    if not os.path.exists(reversed_dir):
        os.makedirs(reversed_dir)

    with ThreadPoolExecutor(max_workers=max_worker) as executor:
        count = 0
        segment_files = sorted(os.listdir(temp_dir))
        total_segments = len(segment_files)

        with ThreadPoolExecutor(max_workers=max_worker) as executor:
            future_to_segment = {executor.submit(process_segment_audio, os.path.join(temp_dir, seg_file),
                                                 os.path.join(reversed_dir, seg_file)): seg_file for seg_file in
                                 segment_files}
            for i, future in enumerate(as_completed(future_to_segment), 1):
                segment = future_to_segment[future]
                try:
                    future.result()
                    count += 1
                    print(
                        f"작업 완료: {segment} | 남은 작업 수: {total_segments - i} | 완료된 작업 수: {count} | 총 작업 수: {total_segments}")
                except Exception as exc:
                    print(f"{segment} 처리 중 에러 발생: {exc}")


def process_segment_audio(input_path, output_path):
    """하나의 오디오 세그먼트를 역순으로 만듭니다."""
    command = [
        'ffmpeg',
        '-hwaccel', 'cuda',
        '-i', input_path,
        '-map', '0:a',
        '-start_at_zero',
        '-af', 'areverse',  # 오디오 프레임 역순 및 타임스탬프 조정
        '-c:a', 'aac',
        '-y',
        output_path
    ]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    process_bar(process)


def concat_segments_audio(reversed_dir, output_path):
    print("concatenate segments audio")
    with open('audio_segments.txt', 'w') as f:
        for segment_file in sorted(os.listdir(reversed_dir), reverse=True):
            f.write(f"file '{os.path.join(reversed_dir, segment_file)}'\n")

    command = [
        'ffmpeg',
        '-f', 'concat',
        '-safe', '0',
        '-i', 'audio_segments.txt',
        '-c', 'copy',
        f'{output_path}/reversed_audio.mp4',
        '-progress', '-',  # 진행률 표시
        '-y',
    ]

    process = subprocess.run(command, universal_newlines=True)
    # process_bar(process)
    print("concatenate segments audio Done")
    os.remove('audio_segments.txt')


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


def combine_audio_video(video_path, audio_path, output_path):
    print("combine audio video")
    command = [
        'ffmpeg',
        '-i', video_path,
        '-i', audio_path,
        '-c:v', 'copy',
        '-c:a', 'copy',
        '-threads', '0',
        f"{output_path}",
        '-progress', '-',
        '-y'
    ]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    process_bar(process)
    print("combine audio video Done")


def run(dir_path, output_dir, divide_tmp, merge_tmp, temp_dir, reverse_dir):
    for root, dirs, files in os.walk(dir_path):
        for i, file in enumerate(files):
            if file.endswith(('.mp4', '.ts')):
                input_path = os.path.join(root, file)
                print(f"Processing: {input_path}")
                print(f"Processing remaining: {len(files)} : {len(files) - (i + 1)} |  {file}")
                # divide_2x2_with_progress(input_path, divide_tmp)
                # merge_tile_2x2(divide_tmp, f"{merge_tmp}/merged.mp4")
                reverse_video(input_path, output_dir, temp_dir, reverse_dir, file)


if __name__ == '__main__':
    input_path = '102845970 20230930 044644 001 ts.mp4'
    temp_dir = './output/temp'
    divide_tmp = './output/divide_tmp'
    merge_tmp = './output/merge_tmp'
    reverse_dir = './output/reversed'
    audio_reversed_dir = './output/reversed_audio'
    dir_path = './test_dir'
    output_dir = './output'

    # run(dir_path, output_dir, divide_tmp, merge_tmp, temp_dir, reverse_dir)
    reverse_video(input_path, output_dir, temp_dir, reverse_dir, audio_reversed_dir, 'reversed_done')
