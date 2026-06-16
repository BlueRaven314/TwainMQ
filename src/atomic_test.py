import os
import time
import random
import string
from multiprocessing import Process, Queue, cpu_count
from pathlib import Path

from twainmq.atomic_append import atomic_append

TEST_FILE = Path("atomic_test.log")
NUM_PROCESSES = max(4, cpu_count())
MESSAGES_PER_PROCESS = 2000
MAX_MESSAGE_SIZE = 2048

def random_message(pid, i):
    body = ''.join(random.choices(string.ascii_letters + string.digits, k=5000))
    return f"{pid}:{i}:{body}\n".encode("utf-8")

def worker(pid, q):
    try:
        for i in range(MESSAGES_PER_PROCESS):
            msg = random_message(pid, i)
            atomic_append(TEST_FILE, msg)
        q.put((pid, "OK"))
    except Exception as e:
        q.put((pid, f"ERROR: {e}"))

def verify():
    seen = set()
    errors = []

    with TEST_FILE.open("rb") as f:
        for lineno, line in enumerate(f, 1):
            if line == b"\n":
                errors.append(f"Blank line at {lineno}")
                continue

            try:
                decoded = line.decode("utf-8").rstrip("\n")
            except Exception:
                errors.append(f"Invalid UTF-8 at line {lineno}: {line!r}")
                continue

            parts = decoded.split(":")
            if len(parts) != 3:
                errors.append(f"Malformed line at {lineno}: {decoded!r}")
                continue

            pid, idx, body = parts
            key = (pid, idx)

            if key in seen:
                errors.append(f"Duplicate message at line {lineno}: {decoded!r}")
            seen.add(key)

    expected = NUM_PROCESSES * MESSAGES_PER_PROCESS
    if len(seen) != expected:
        errors.append(f"Missing messages: expected {expected}, got {len(seen)}")

    return errors


def main():
    if TEST_FILE.exists():
        TEST_FILE.unlink()

    q = Queue()
    procs = []

    print(f"Starting {NUM_PROCESSES} processes, "
          f"{MESSAGES_PER_PROCESS} messages each…")

    for pid in range(NUM_PROCESSES):
        p = Process(target=worker, args=(pid, q))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    # Check worker results
    while not q.empty():
        pid, status = q.get()
        print(f"Process {pid}: {status}")

    print("Verifying output…")
    errors = verify()

    if errors:
        print("\nFAIL — errors detected:")
        for e in errors:
            print("  -", e)
    else:
        print("\nPASS — no corruption detected")


if __name__ == "__main__":
    main()
