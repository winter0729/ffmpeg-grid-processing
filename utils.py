import subprocess
import json
import re


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


def process_bar(process, total_segments):
    from rich.progress import Progress

    with Progress() as progress:
        task = progress.add_task("[cyan]Processing...", total=total_segments)

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
