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
        "type": "audio",
        "timestamp": get_5s_timestamp(),
        "transcript": "I need to leave now",
        "keywords": ["leave"],
        "confidence": round(random.uniform(0.85, 0.98), 2)
    }

    producer.send("audio_stream", data)
    print("Audio sent:", data)

    time.sleep(3)