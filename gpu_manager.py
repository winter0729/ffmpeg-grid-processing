import subprocess
import itertools
import asyncio


def get_gpu_devices():
    command = [
        'nvidia-smi',
        '--query-gpu=index',
        '--format=csv,noheader'
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    return [int(gpu_index) for gpu_index in result.stdout.splitlines()]


class GPUManager:
    def __init__(self, max_worker=6):
        self.max_worker = max_worker
        self.gpu_devices = itertools.cycle(get_gpu_devices())

    def get_next_gpu(self):
        return next(self.gpu_devices)

    async def check_gpu_usage(self):
        while True:
            for gpu_device in self.gpu_devices:
                command = [
                    'nvidia-smi',
                    '--query-gpu=utilization.encoder',
                    f'--id={gpu_device}',
                    '--format=csv,noheader,nounits'
                ]
                result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                print(f"GPU {gpu_device} usage: {result.stdout.strip()}%")
            await asyncio.sleep(10)