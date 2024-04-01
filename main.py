import pprint
import os
import subprocess
import json
import re
import shutil
import ffmpeg
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

max_worker = 4

def progressbar(label, total):
    pbar = tqdm(total=total, desc=label, unit='B', unit_scale=True, unit_divisor=1024)
    return pbar

def divide_crop(input_path, output_dir, options = None):
    print("Input source: ", input_path)
    if options is None:
        print("No options")
    else:
        print("Options: ", options)

    if options == 1:
        divide_2x2(input_path, output_dir)
    # elif options == 2:
    #     divide_3x3(input_path, output_dir)
    # elif options == 3:
    #     divide_4x4(input_path, output_dir)


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
       # 출력에서 "Duration: 00:00:30.04"와 같은 라인을 찾아서 전체 동영상 길이를 가져옵니다.
       if "Duration" in line:
           time_str = re.search("Duration: (.*?),", line).group(1)
           hours, minutes, seconds = map(float, re.split(':', time_str))
           total_seconds = hours*3600 + minutes*60 + seconds
           pbar.total = total_seconds
           pbar.refresh()
       # 출력에서 "time=00:00:10.00"과 같은 라인을 찾아서 현재 진행 시간을 가져옵니다.
       if "time=" in line:
           match = re.search("time=(.*?) ", line)
           if match is not None:
               time_str = match.group(1)
               hours, minutes, seconds = map(float, re.split(':', time_str))
               elapsed_seconds = hours * 3600 + minutes * 60 + seconds
               pbar.n = elapsed_seconds
               pbar.refresh()
    pbar.close()


def divide_2x2(input_path, output_dir):
    print("divide 2x2")
    result = get_video_info(input_path)
    duration = result['format']['duration']
    width = result['streams'][0]['width']
    height = result['streams'][0]['height']
    print("Duration: ", duration)
    print("Width: ", width)
    print("Height: ", height)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    input_video = ffmpeg.input(input_path)
    crop_width, crop_height = width // 2, height // 2

    # # 2x2 크롭 영역 정의
    # # 각 영역별 크롭 및 출력 설정
    top_left = input_video.crop(x=0, y=0, width=crop_width, height=crop_height)
    top_right = input_video.crop(x=crop_width, y=0, width=crop_width, height=crop_height)
    bottom_left = input_video.crop(x=0, y=crop_height, width=crop_width, height=crop_height)
    bottom_right = input_video.crop(x=crop_width, y=crop_height, width=crop_width, height=crop_height)
    #
    # process = ffmpeg.run([
    #     ffmpeg.output(top_left, f'{output_dir}/top_left.mp4', **{'c:v': 'h264_nvenc'}, **{'c:a': 'copy'}, **{'map': '0:a'}, **{'progress': '-'} ),
    #     ffmpeg.output(top_right, f'{output_dir}/top_right.mp4', **{'c:v': 'h264_nvenc'}, **{'c:a': 'copy'}, **{'map': '0:a'}, **{'progress': '-'} ),
    #     ffmpeg.output(bottom_left, f'{output_dir}/bottom_left.mp4', **{'c:v': 'h264_nvenc'}, **{'c:a': 'copy'}, **{'map': '0:a'}, **{'progress': '-'} ),
    #     ffmpeg.output(bottom_right, f'{output_dir}/bottom_right.mp4', **{'c:v': 'h264_nvenc'}, **{'c:a': 'copy'}, **{'map': '0:a'}, **{'progress': '-'} ),
    # ], capture_stdout=True, capture_stderr=True, overwrite_output=True)

    # #crop 1/2
    # (
    #     ffmpeg
    #     .input(input_path)
    #     .filter('crop', width // 2, height // 2, 0, 0)
    #     .output(output_dir + '/1.mp4', **{'c:v': 'h264_nvenc'}, **{'c:a': 'copy'}, **{'map': '0:a'}, progress='-', y='-y')
    #     .run()
    # )
    # (
    #     ffmpeg
    #     .input(input_path)
    #     .filter('crop', width // 2, height // 2, width // 2, 0)
    #     .output(output_dir + '/2.mp4', **{'c:v': 'h264_nvenc'}, **{'c:a': 'copy'}, **{'map': '0:a'}, progress='pipe:1', y='-y')
    #     .run()
    # )
    # (
    #     ffmpeg
    #     .input(input_path)
    #     .filter('crop', width // 2, height // 2, 0, height // 2)
    #     .output(output_dir + '/3.mp4', **{'c:v': 'h264_nvenc'}, **{'c:a': 'copy'}, **{'map': '0:a'}, progress='pipe:1', y='-y')
    #     .run()
    # )
    # (
    #     ffmpeg
    #     .input(input_path)
    #     .filter('crop', width // 2, height // 2, width // 2, height // 2)
    #     .output(output_dir + '/4.mp4', **{'c:v': 'h264_nvenc'}, **{'c:a': 'copy'}, **{'map': '0:a'}, progress='pipe:1', y='-y')
    #     .run()
    # )


def divide_2x2_with_progress(input_path, output_dir):
    print("divide 2x2")
    result = get_video_info(input_path)
    duration = result['format']['duration']
    width = result['streams'][0]['width']
    height = result['streams'][0]['height']
    print("Duration: ", duration)
    print("Width: ", width)
    print("Height: ", height)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    crop_width, crop_height = width // 2, height // 2

    cmd = [
        'ffmpeg',
        '-i', input_path,
        '-filter_complex',
        f'[0:v]crop={crop_width}:{crop_height}:0:0[1]; '
        f'[0:v]crop={crop_width}:{crop_height}:iw/2:0[2]; '
        f'[0:v]crop={crop_width}:{crop_height}:0:ih/2[3]; '
        f'[0:v]crop={crop_width}:{crop_height}:iw/2:ih/2[4]',

        '-c:v', 'h264_nvenc',
        '-c:a', 'copy',
        "-map", "0:a",
        '-map', '[1]', f'{output_dir}/1.mp4',

        '-c:v', 'h264_nvenc',
        '-c:a', 'copy',
        "-map", "0:a",
        '-map', '[2]', f'{output_dir}/2.mp4',

        '-c:v', 'h264_nvenc',
        '-c:a', 'copy',
        "-map", "0:a",
        '-map', '[3]', f'{output_dir}/3.mp4',

        '-c:v', 'h264_nvenc',
        '-c:a', 'copy',
        "-map", "0:a",
        '-map', '[4]', f'{output_dir}/4.mp4',

        '-progress', '-',
        '-y'
    ]

    # ffmpeg 프로세스 시작
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    process_bar(process)

def merge_tile_2x2(tile_path, output_path):
    split_tiles = ([4,3],
                   [2,1])

    command = [
        'ffmpeg',
    ]

    for row in split_tiles:
        for title in row:
            title_path = f"{tile_path}/{title}.mp4"
            print(f"Title Path: {title_path}")
            command.extend(['-i', title_path])

    # input1(0, 0) | input3(w0, 0)
    # input2(0, h0) | input4(w0, h0)
    # https://ffmpeg.org/ffmpeg-filters.html#xstack-1
    #pprint.pprint(command)
    filter_complex_str = "xstack=inputs=4:layout=0_0|w0_0|0_h0|w0_h0[v];"

    command.extend([
        '-filter_complex', filter_complex_str,
        '-progress', '-',
        '-map', '[v]',
        '-map', '0:a?',
        '-c:v', 'h264_nvenc',
        '-c:a', 'copy',
        '-b:v', '8000k',
        '-strict', 'experimental',  # 일부 ffmpeg 버전에서 aac 오디오 코덱을 사용할 때 필요
        '-y',
        output_path
    ])

    pprint.pprint(command)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    process_bar(process)

def reverse_video(input_path, output_dir, temp_dir, reversed_dir):
    print("reverse video")
    input_path = f"{output_dir}/merged.mp4"
    output_path = f"{output_dir}/reversed.mp4"
    segment_duration = 20

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    if os.path.exists(reversed_dir):
        shutil.rmtree(reversed_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    if not os.path.exists(reversed_dir):
        os.makedirs(reversed_dir)

    split_video(input_path, segment_duration, temp_dir)
    reverse_segment(temp_dir, reversed_dir)
    reverse_audio(input_path, temp_dir)
    concatenate_segments(reversed_dir, output_path)
    combine_audio_video(video_path=f"{output_dir}/reversed.mp4", audio_path=f'{temp_dir}/reversed_audio.mp4', output_path=f'{output_dir}/final_reversed.mp4')


def split_video(input_path, segment_duration, temp_dir):
    print("split video")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    command = [
        'ffmpeg',
        '-i', input_path,
        '-map', '0:v',
        '-map', '0:1',
        '-start_at_zero',
        '-segment_time', str(segment_duration),
        '-f', 'segment',
        '-break_non_keyframes', '0',
        '-reset_timestamps', '1',
        '-c', 'copy',
        f'{temp_dir}/segment%03d.mp4',
        '-y'
    ]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    process_bar(process)

def reverse_segment(temp_dir, reversed_dir):
    """분할된 모든 비디오 세그먼트를 역순으로 만듭니다."""
    if not os.path.exists(reversed_dir):
        os.makedirs(reversed_dir)


    with ThreadPoolExecutor(max_workers=max_worker) as executor:
        count = 0
        segment_files = sorted(os.listdir(temp_dir))
        total_segments = len(segment_files)
        with ThreadPoolExecutor(max_workers=max_worker) as executor:
            future_to_segment = {executor.submit(process_segment, os.path.join(temp_dir, seg_file),
                                                 os.path.join(reversed_dir, seg_file)): seg_file for seg_file in
                                 segment_files}
            for i, future in enumerate(as_completed(future_to_segment), 1):
                segment = future_to_segment[future]
                try:
                    future.result()
                    count += 1
                    print(f"작업 완료: {segment} | 남은 작업 수: {total_segments - i} | 완료된 작업 수: {count} | 총 작업 수: {total_segments}")
                except Exception as exc:
                    print(f"{segment} 처리 중 에러 발생: {exc}")

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

def process_segment(input_path, output_path):
    """하나의 비디오 세그먼트를 역순으로 만듭니다."""
    command = [
        'ffmpeg',
        '-i', input_path,
        '-map', '0:v',
        # '-map', '0:1',
        '-start_at_zero',
        '-vf', 'reverse,setpts=PTS-STARTPTS',  # 비디오 프레임 역순 및 타임스탬프 조정
        # '-af', 'areverse,asetpts=PTS-STARTPTS',  # 오디오 프레임 역순 및 타임스탬프 조정
        '-c:v', 'h264_nvenc',
        '-vsync', '0',
        '-an',
        # '-async', '1',
        # '-c:a', 'aac',
        '-b:v', '8000k',
        output_path,
        '-y'
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    # process_bar(process)
    output, errors = process.communicate()  # 프로세스 완료 대기
    if process.returncode != 0:
        pprint.pprint(f"에러 발생: {output} | {errors}")
        pprint.pprint(f"gpu 재처리: {input_path} -> {output_path}")
        command = [
            'ffmpeg',
            '-i', input_path,
            '-map', '0:v',
            # '-map', '0:1',
            '-start_at_zero',
            '-vf', 'reverse,setpts=PTS-STARTPTS',  # 비디오 프레임 역순 및 타임스탬프 조정
            # '-af', 'areverse,asetpts=PTS-STARTPTS',  # 오디오 프레임 역순 및 타임스탬프 조정
            '-c:v', 'h264_nvenc',
            '-vsync', '0'
            '-an',
            # '-async', '1',
            # '-c:a', 'aac',
            '-b:v', '8000k',
            output_path,
            '-y'
        ]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        # process_bar(process)
        output, errors = process.communicate()  # 프로세스 완료 대기
        if process.returncode != 0:
            raise Exception(f"에러 발생: {output} | {errors}")


def reverse_audio(input_path, temp_dir):
    print("reverse audio")

    command = [
        'ffmpeg',
        '-i', input_path,
        '-start_at_zero',
        '-map', '0:a',
        '-vn',
        '-progress', '-',  # 진행률 표시
        '-af', 'areverse',
        '-c:a', 'aac',
        '-channel_layout', 'stereo',
        f'{temp_dir}/reversed_audio.mp4',
        '-y'
    ]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    process_bar(process)
    print("reverse audio Done")

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
        '-c', 'copy',
        output_path,
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
        output_path,
        '-progress', '-',
        '-y'
    ]

    process = subprocess.run(command, universal_newlines=True)
    print("combine audio video Done")


if __name__ == '__main__':
    input_path = './rev_test.mp4'
    output_dir = './output'
    temp_dir = './output/temp'
    reverse_dir = './output/reversed'
    divide_2x2_with_progress(input_path, output_dir)
    merge_tile_2x2(output_dir, f"{output_dir}/merged.mp4")
    reverse_video(input_path, output_dir, temp_dir, reverse_dir)
    #reverse_audio(input_path, temp_dir)
    #concat_segments(filelist_path= f'{output_dir}/filelist.txt', output_path=f'{output_dir}/final_reversed.mp4')
    #concatenate_segments(reverse_dir, output_path = f"{output_dir}/reversed.mp4")
    #combine_audio_video(video_path=f"{output_dir}/reversed.mp4", audio_path=f'{temp_dir}/reversed_audio.mp4', output_path=f'{output_dir}/final_reversed.mp4')
