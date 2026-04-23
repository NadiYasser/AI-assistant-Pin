import json
import time

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def get_15s_timestamp():
    now = int(time.time())
    return now - (now % 15)


locations = [
    {"raw": [35.7595, -5.8340], "semantic": "home", "confidence": 0.99},
    {"raw": [35.7712, -5.8124], "semantic": "kindergarten", "confidence": 0.98},
]


def main():
    location_index = 0

    while True:
        loc = locations[location_index % len(locations)]
        data = {
            "type": "location",
            "timestamp": get_15s_timestamp(),
            "raw": loc["raw"],
            "semantic": loc["semantic"],
            "confidence": loc["confidence"],
        }

        try:
            producer.send("location_stream", data)
            producer.flush()
            print("Location sent:", data)
        except Exception as exc:
            print(f"Error sending location message: {exc}")

        location_index += 1
        time.sleep(15)


if __name__ == "__main__":
    main()
