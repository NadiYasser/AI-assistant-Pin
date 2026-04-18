from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_5s_timestamp():
    now = int(time.time())
    return now - (now % 5)

locations = [
    {"raw": [35.7595, -5.8340], "semantic": "home"},
    {"raw": [35.7600, -5.8350], "semantic": "street"},
]

while True:
    loc = random.choice(locations)

    data = {
        "type": "location",
        "timestamp": get_5s_timestamp(),
        "raw": loc["raw"],
        "semantic": loc["semantic"],
        "confidence": round(random.uniform(0.9, 0.99), 2)
    }

    producer.send("location_stream", data)
    print("Location sent:", data)

    time.sleep(4)