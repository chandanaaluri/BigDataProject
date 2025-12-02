from kafka import KafkaProducer
import json, time, random, threading
from datetime import datetime

KAFKA_BROKER = "localhost:9092"
TOPIC = "agri-sensors"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

def sensor_loop(sensor_id):
    while True:
        msg = {
            "sensor_id": sensor_id,
            "ts": datetime.utcnow().isoformat(),
            "soil_moisture": max(0, min(100, random.gauss(40, 15))),
            "temperature": round(random.gauss(25,5),1),
            "humidity": round(random.uniform(30,90),1),
            "rainfall": round(max(0, random.gauss(0.2,0.6)),2)
        }
        producer.send(TOPIC, msg)
        print("Produced:", msg)
        time.sleep(random.uniform(0.5,2.0))

for i in range(3):
    threading.Thread(target=sensor_loop, args=(f"sensor_{i+1}",), daemon=True).start()

while True:
    time.sleep(1)
