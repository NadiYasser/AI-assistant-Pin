import multiprocessing
import os

def run_video():
    os.system("python3 Producers/video_stream.py")

def run_audio():
    os.system("python3 Producers/audio_stream.py")

def run_location():
    os.system("python3 Producers/location_stream.py")

if __name__ == "__main__":
    processes = [
        multiprocessing.Process(target=run_video),
        multiprocessing.Process(target=run_audio),
        multiprocessing.Process(target=run_location),
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()