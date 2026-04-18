from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_5s_timestamp():
    now = int(time.time())
    return now - (now % 5)

while True:
    data = {
        "type": "video",
        "timestamp": get_5s_timestamp(),
        "description": "A person walks toward the door carrying a bag",
        "confidence": round(random.uniform(0.8, 0.95), 2)
    }

    producer.send("video_stream", data)
    print("Video sent:", data)

    time.sleep(2)