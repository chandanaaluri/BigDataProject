# src/simulator_and_writer.py
"""
Simulator that produces fake sensor readings and writes them to a CSV file.

Usage examples:
  # Finite demo (10 rows) from notebook:
  !python -u src/simulator_and_writer.py --iters 10

  # Long running in terminal:
  python src/simulator_and_writer.py

  # Specify output CSV or number of sensors:
  python src/simulator_and_writer.py --csv data/agri_data.csv --sensors 5 --iters 500
"""

import argparse
import csv
import os
import random
import sys
import time
from datetime import datetime, timezone
from queue import Queue, Empty
from threading import Thread, Event

FIELDNAMES = ["sensor_id", "ts", "soil_moisture", "temperature", "humidity", "rainfall", "irrigation_needed"]


def irrigation_rule(soil_moisture, rainfall):
    """Simple rule: irrigation needed if soil_moisture < 30 and rainfall < 0.5"""
    try:
        return (float(soil_moisture) < 30.0) and (float(rainfall) < 0.5)
    except Exception:
        return False


def sensor_loop(q: Queue, sensor_id: str, stop_evt: Event, sleep_min=0.5, sleep_max=1.5):
    """Continuously produce sensor messages and put them on queue until stop_evt is set."""
    while not stop_evt.is_set():
        msg = {
            "sensor_id": sensor_id,
            # timezone-aware UTC timestamp
            "ts": datetime.now(timezone.utc).isoformat(),
            "soil_moisture": float(max(0, min(100, random.gauss(40, 15)))),
            "temperature": float(round(random.gauss(25, 5), 1)),
            "humidity": float(round(random.uniform(30, 90), 1)),
            "rainfall": float(round(max(0, random.gauss(0.2, 0.6)), 2)),
        }
        q.put(msg)
        print(f"[Producer] {sensor_id} -> {msg}", flush=True)
        time.sleep(random.uniform(sleep_min, sleep_max))


def writer_loop(q: Queue, stop_evt: Event, csv_path: str, max_writes: int = 0):
    """
    Pop messages from queue and write to CSV.
    If max_writes > 0, stop after writing that many rows.
    """
    # ensure directory exists
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)

    # create file and header if not exists
    file_exists = os.path.exists(csv_path)
    f = open(csv_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, FIELDNAMES)
    if not file_exists:
        writer.writeheader()
        f.flush()

    writes = 0
    try:
        while not stop_evt.is_set():
            try:
                msg = q.get(timeout=1.0)
            except Empty:
                # check if we should stop due to max_writes
                if max_writes and writes >= max_writes:
                    break
                continue

            row = {
                "sensor_id": msg["sensor_id"],
                "ts": msg["ts"],
                "soil_moisture": msg["soil_moisture"],
                "temperature": msg["temperature"],
                "humidity": msg["humidity"],
                "rainfall": msg["rainfall"],
                "irrigation_needed": irrigation_rule(msg["soil_moisture"], msg["rainfall"]),
            }
            writer.writerow(row)
            f.flush()
            writes += 1
            print(f"[Writer] wrote {row} (total written: {writes})", flush=True)

            if max_writes and writes >= max_writes:
                print(f"[Writer] reached max_writes={max_writes}, stopping writer loop.", flush=True)
                break

    finally:
        try:
            f.flush()
            f.close()
        except Exception:
            pass
        print("[Writer] file closed, writer loop exiting.", flush=True)


def run_simulator(csv_path: str, sensors: int = 3, iters: int = 0):
    """
    Start the producer threads and writer thread.
    - iters: if >0, stop after that many writes (useful for demo).
    """
    q = Queue(maxsize=1000)
    stop = Event()

    # start sensor threads (daemon True is OK; writer is non-daemon to ensure file flush)
    sensor_threads = []
    for i in range(sensors):
        tid = Thread(target=sensor_loop, args=(q, f"sensor_{i+1}", stop), daemon=True)
        tid.start()
        sensor_threads.append(tid)

    writer_thread = Thread(target=writer_loop, args=(q, stop, csv_path, iters), daemon=False)
    writer_thread.start()

    print(f"simulator_and_writer running. sensors={sensors}, csv={csv_path}, iters={iters}", flush=True)
    try:
        # If finite run requested, wait until writer thread completes
        if iters and iters > 0:
            writer_thread.join()  # will return when max writes reached
            print("Writer finished (finite run). Stopping sensors.", flush=True)
            stop.set()
        else:
            # infinite run; block until keyboard interrupt
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt received: stopping simulator...", flush=True)
        stop.set()
        # wait up to a short time for threads to finish
        writer_thread.join(timeout=5)
    finally:
        stop.set()
        print("Simulator stopped.", flush=True)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="IoT sensor simulator that writes readings to CSV.")
    p.add_argument("--iters", type=int, default=0, help="Number of rows to write then exit (0 => run forever)")
    p.add_argument("--sensors", type=int, default=3, help="Number of simulated sensors (default 3)")
    p.add_argument("--csv", type=str, default="data/agri_data.csv", help="CSV file path to write (default: data/agri_data.csv)")
    p.add_argument("--log", type=str, default=None, help="Optional log file to redirect stdout/stderr (for background runs)")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    if args.log:
        # basic redirect of stdout/stderr to a file (append)
        log_dir = os.path.dirname(args.log) or "."
        os.makedirs(log_dir, exist_ok=True)
        log_f = open(args.log, "a", encoding="utf-8")
        print(f"Redirecting logs to {args.log}", flush=True)
        # replace sys.stdout/sys.stderr for the rest of process
        sys.stdout = log_f
        sys.stderr = log_f

    # run simulator (this call blocks until completion or KeyboardInterrupt)
    run_simulator(csv_path=args.csv, sensors=args.sensors, iters=args.iters)

    # If we redirected logs, close file handle nicely
    if args.log:
        try:
            sys.stdout.flush()
            sys.stdout.close()
            sys.stderr = sys.__stderr__
        except Exception:
            pass


if __name__ == "__main__":
    main()
