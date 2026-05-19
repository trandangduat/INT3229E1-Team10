"""
utils.py — PySpark Pipeline Utilities
Timing + resource monitoring helpers for all pipeline stages.
"""

import time
import psutil


def start_timer() -> float:
    return time.time()


def log_stage(name: str, start_time: float):
    elapsed = time.time() - start_time
    mem = psutil.virtual_memory()
    print(
        f"[TIMER] [{name}] elapsed={elapsed:.2f}s | "
        f"RAM used={mem.used / 1e9:.2f} GB | "
        f"RAM percent={mem.percent:.1f}%"
    )
    return elapsed
