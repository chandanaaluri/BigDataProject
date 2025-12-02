import time, random, csv, os
from datetime import datetime
from threading import Thread, Event
from queue import Queue, Empty

CSV_FILE = "agri_data.csv"
FIELDNAMES = ["sensor_id", "ts", "soil_moisture", "temperature", "humidity", "rainfall", "irrigation_needed"]

def irrigation_rule(soil_moisture, rainfall):
    return (float(soil_moisture) < 30.0) and (float(rainfall) < 0.5)

def sensor_loop(q, sensor_id, stop_evt):
    while not stop_evt.is_set():
        msg = {
            "sensor_id": sensor_id,
            "ts": datetime.utcnow().isoformat(),
            "soil_moisture": float(max(0, min(100, random.gauss(40, 15)))),
            "temperature": float(round(random.gauss(25,5),1)),
            "humidity": float(round(random.uniform(30,90),1)),
            "rainfall": float(round(max(0, random.gauss(0.2,0.6)),2))
        }
        q.put(msg)
        print(f"[Producer] {sensor_id} -> {msg}")
        time.sleep(random.uniform(0.5, 1.5))

def writer_loop(q, stop_evt):
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="") as f:
            csv.DictWriter(f, FIELDNAMES).writeheader()
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, FIELDNAMES)
        while not stop_evt.is_set():
            try:
                msg = q.get(timeout=1)
            except Empty:
                continue
            row = {
                "sensor_id": msg["sensor_id"],
                "ts": msg["ts"],
                "soil_moisture": msg["soil_moisture"],
                "temperature": msg["temperature"],
                "humidity": msg["humidity"],
                "rainfall": msg["rainfall"],
                "irrigation_needed": irrigation_rule(msg["soil_moisture"], msg["rainfall"])
            }
            writer.writerow(row)
            f.flush()
            print(f"[Writer] wrote {row}")

def main():
    q = Queue()
    stop = Event()
    for i in range(3):
        Thread(target=sensor_loop, args=(q, f"sensor_{i+1}", stop), daemon=True).start()
    Thread(target=writer_loop, args=(q, stop), daemon=True).start()
    print("simulator_and_writer running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
        stop.set()
        time.sleep(1)

if __name__ == "__main__":
    main()
