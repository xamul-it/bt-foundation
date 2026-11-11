import argparse
import fcntl
import logging
from pathlib import Path
import time

from .config import Settings
from .lifecycle import SearchLifecycle


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--data', type=Path, required=True)
    parser.add_argument('--idle-seconds', type=float, required=True)
    args = parser.parse_args()
    settings = Settings(runtime=args.runtime, data=args.data, idle_seconds=args.idle_seconds)
    with (settings.runtime / 'idle-worker.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        controller = SearchLifecycle(settings)
        while True:
            try:
                if not controller.stop_if_idle():
                    return
            except Exception:
                logging.exception('Idle shutdown failed; retrying')
            time.sleep(max(0.5, min(5, settings.idle_seconds / 4)))


if __name__ == '__main__':
    main()
