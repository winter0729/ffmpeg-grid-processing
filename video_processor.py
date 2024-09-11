import os
import subprocess
import asyncio
import shutil
from rich.progress import Progress, TextColumn, BarColumn, TimeRemainingColumn, TimeElapsedColumn
from rich.live import Live
from rich.console import Console, Group
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.columns import Columns

from utils import get_video_info, extract_seconds, process_bar
from gpu_manager import GPUManager, get_gpu_devices


class VideoProcessor:
    def __init__(self, segment_duration=10):
        self.segment_duration = segment_duration
        self.gpu_manager = GPUManager()
        self.gpu_status = {gpu: {"tasks": 0, "current_segment": ""} for gpu in get_gpu_devices()}
        self.console = Console()
        self.processed_files = []

    def reverse_video(self, input_path, output_dir, temp_dir, reversed_dir, output_name):
        for directory in [temp_dir, reversed_dir]:
            if os.path.exists(directory):
                shutil.rmtree(directory)
            os.makedirs(directory, exist_ok=True)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        self.split_video(input_path, temp_dir)
        asyncio.run(self.reverse_segment(temp_dir, reversed_dir))
        self.concatenate_segments(reversed_dir, output_path=f'{output_dir}/{output_name}.mp4')

    def split_video(self, input_path, temp_dir):
        print("split video")

        video_info = get_video_info(input_path)
        total_duration = float(video_info['format']['duration'])
        total_segments = int(total_duration / self.segment_duration)

        command = [
            'ffmpeg',
            '-i', input_path,
            '-map', '0:v',
            '-map', '0:a',
            '-start_at_zero',
            '-segment_time', str(self.segment_duration),
            '-f', 'segment',
            '-break_non_keyframes', '0',
            '-reset_timestamps', '1',
            '-avoid_negative_ts', 'make_zero',
            '-force_key_frames', f"expr:gte(t,n_forced*{self.segment_duration})",
            '-c', 'copy',
            f'{temp_dir}/segment%010d.ts',
            '-y'
        ]

        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        process_bar(process, total_segments)

    async def reverse_segment(self, temp_dir, reversed_dir):
        os.makedirs(reversed_dir, exist_ok=True)

        segment_files = sorted(os.listdir(temp_dir))
        total_segments = len(segment_files)

        queue = asyncio.Queue(maxsize=1)

        progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}% [{task.completed}/{task.total}]"),
            TimeElapsedColumn(),
            TimeRemainingColumn()
        )

        layout = Layout()
        layout.split_column(
            Layout(Panel(Group(
                progress,
                self.generate_gpu_status()
            )), name="upper"),
            Layout(Panel(self.generate_log(), title="Processed Files"), name="lower")
        )

        with Live(layout, refresh_per_second=4) as live:
            task_id = progress.add_task("Processing", total=total_segments)

            workers = [asyncio.create_task(self.worker(f"Worker-{i + 1}", queue, progress, task_id, live))
                       for i in range(self.gpu_manager.max_worker)]

            for segment_file in segment_files:
                await queue.put((segment_file, temp_dir, reversed_dir))

            await queue.join()

            for worker_task in workers:
                worker_task.cancel()

    def generate_gpu_status(self):
        gpu_status_texts = []
        for gpu, status in self.gpu_status.items():
            gpu_text = Text(f"GPU {gpu}: ", style="cyan")
            gpu_text.append(f"{status['tasks']} tasks", style="magenta")
            if status['current_segment']:
                gpu_text.append(f" | {status['current_segment']}", style="green")
            gpu_status_texts.append(gpu_text)
        return Columns(gpu_status_texts)

    def generate_log(self):
        log = Group(
            *(Text(file, style="bright_blue") for file in self.processed_files[-10:])
        )
        return log

    async def worker(self, name, queue, progress, task_id, live):
        while True:
            segment_file, temp_dir, reversed_dir = await queue.get()
            gpu_device = None
            try:
                gpu_device = self.gpu_manager.get_next_gpu()
                self.gpu_status[gpu_device]['tasks'] += 1
                self.gpu_status[gpu_device]['current_segment'] = segment_file
                live.update(self.generate_updated_layout(progress))

                await self.process_segment(
                    os.path.join(temp_dir, segment_file),
                    os.path.join(reversed_dir, segment_file),
                    gpu_device
                )
                progress.update(task_id, advance=1)
                self.processed_files.append(f"{name} 작업 완료: {segment_file} | 할당 GPU: {gpu_device}")

                self.gpu_status[gpu_device]['tasks'] -= 1
                self.gpu_status[gpu_device]['current_segment'] = ""
                live.update(self.generate_updated_layout(progress))
            except Exception as e:
                if gpu_device is not None:
                    print(f"{segment_file} 처리 중 오류 발생 (GPU {gpu_device}): {e}")
                else:
                    print(f"{segment_file} 처리 중 오류 발생 (GPU 할당 전): {e}")
            finally:
                if gpu_device is not None:
                    self.gpu_status[gpu_device]['tasks'] = max(0, self.gpu_status[gpu_device]['tasks'] - 1)
                    self.gpu_status[gpu_device]['current_segment'] = ""
                queue.task_done()

    def generate_updated_layout(self, progress):
        layout = Layout()
        layout.split_column(
            Layout(Panel(Group(
                progress,
                self.generate_gpu_status()
            )), name="upper"),
            Layout(Panel(self.generate_log(), title="Processed Files"), name="lower")
        )
        return layout

    @staticmethod
    async def process_segment(input_path, output_path, gpu_device):
        command = [
            'ffmpeg',
            '-hwaccel', 'cuda',
            '-hwaccel_device', f'{gpu_device}',
            '-i', input_path,
            '-map', '0:v',
            '-map', '0:a',
            '-start_at_zero',
            '-vf', 'reverse,setpts=PTS-STARTPTS',
            '-af', 'areverse,asetpts=PTS-STARTPTS',
            '-c:v', 'h264_nvenc',
            '-c:a', 'aac',
            '-avoid_negative_ts', 'make_zero',
            '-async', '1',
            '-b:v', '8000k',
            output_path,
            '-y'
        ]
        process = await asyncio.create_subprocess_exec(*command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            raise Exception(f"Error during processing: {stdout} | {stderr}")

    @staticmethod
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
            '-fflags', '+genpts',
            '-c', 'copy',
            '-threads', '0',
            f'{output_path}',
            '-progress', '-',
            '-y'
        ]

        subprocess.run(command, universal_newlines=True)
        print("reversed Done")
        os.remove('segments.txt')
