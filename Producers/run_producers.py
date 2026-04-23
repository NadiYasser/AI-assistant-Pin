import multiprocessing
import subprocess
import sys
from pathlib import Path


PRODUCERS_DIR = Path(__file__).resolve().parent


def run_script(script_name):
    script_path = PRODUCERS_DIR / script_name
    subprocess.run([sys.executable, str(script_path)], check=False)


def run_video():
    run_script("video_stream.py")


def run_audio():
    run_script("audio_stream.py")


def run_location():
    run_script("location_stream.py")


if __name__ == "__main__":
    processes = [
        multiprocessing.Process(target=run_video),
        multiprocessing.Process(target=run_audio),
        multiprocessing.Process(target=run_location),
    ]

    for process in processes:
        process.start()

    for process in processes:
        process.join()
